"""
Vertex ML Logs — PubSub → Kafka Avro Dataflow Flex Template pipeline.

Reads Cloud Logging entries from a Pub/Sub subscription, parses them,
maps to the appropriate Avro schema and publishes to a Google Managed
Kafka topic using the Confluent wire format.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

import apache_beam as beam
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Pipeline options
# ──────────────────────────────────────────────────────────────────────────────

LOG_TYPES = ("online_prediction", "batch_prediction", "monitoring", "training")


class PubSubToKafkaAvroOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--input_subscription",
            required=True,
            help="Full Pub/Sub subscription path, e.g. "
            "projects/PROJECT/subscriptions/SUB",
        )
        parser.add_argument(
            "--kafka_bootstrap_servers",
            required=True,
            help="Kafka bootstrap server(s), comma-separated. "
            "For Google Managed Kafka: "
            "bootstrap.CLUSTER.REGION.managedkafka.PROJECT.cloud.goog:9092",
        )
        parser.add_argument(
            "--kafka_topic",
            required=True,
            help="Destination Kafka topic name.",
        )
        parser.add_argument(
            "--kafka_registry_url",
            required=True,
            help="Confluent-compatible Schema Registry base URL.",
        )
        parser.add_argument(
            "--log_type",
            required=True,
            choices=list(LOG_TYPES),
            help="Log type to process. Determines mapper and Avro subject.",
        )
        parser.add_argument(
            "--dlq_bucket",
            required=True,
            help="GCS bucket name (no gs://) for dead-letter messages.",
        )
        parser.add_argument(
            "--window_size_seconds",
            default=10,
            type=int,
            help="Fixed window size in seconds (default: 10).",
        )
        parser.add_argument(
            "--schema_cache_ttl_seconds",
            default=300,
            type=int,
            help="How long to cache schemas locally on each worker (default: 300).",
        )


# ──────────────────────────────────────────────────────────────────────────────
# DoFns
# ──────────────────────────────────────────────────────────────────────────────


class ParseJsonMessage(beam.DoFn):
    """Decode raw Pub/Sub bytes → dict with injected publish_time_ms."""

    OUTPUT_TAG_DLQ = "dead_letter"

    def process(self, element, timestamp=beam.DoFn.TimestampParam, *args, **kwargs):
        # All module-level names must be local imports: this DoFn is defined in
        # main.py which runs as __main__ on the launcher, so workers won't find
        # globals like json/datetime/traceback in __main__.__dict__.
        import json as _json

        from pipeline.utils.time_utils import pubsub_time_to_millis

        # Initialise raw before the try block so the except handler can always
        # reference it — if element.data were to raise, raw would otherwise be
        # unbound and the except block would produce a confusing NameError.
        raw = b""
        try:
            if hasattr(element, "data"):
                raw = element.data
                publish_time = getattr(element, "publish_time", None)
            else:
                raw = element
                publish_time = None

            text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
            msg = _json.loads(text)
            msg["_publish_time_ms"] = pubsub_time_to_millis(publish_time)
            yield msg
        except Exception as exc:
            logger.warning("ParseJsonMessage failed: %s", exc)
            yield beam.pvalue.TaggedOutput(
                self.OUTPUT_TAG_DLQ,
                {
                    "raw": (
                        raw.decode("utf-8", errors="replace")
                        if isinstance(raw, (bytes, bytearray))
                        else str(raw)
                    ),
                    "error": str(exc),
                    "stage": "parse",
                },
            )


class BuildAvroRecord(beam.DoFn):
    """Map a Cloud Logging dict → serialised Avro bytes (Confluent wire format)."""

    OUTPUT_TAG_DLQ = "dead_letter"

    def __init__(self, log_type: str, registry_url: str, cache_ttl: int = 300):
        self._log_type = log_type
        self._registry_url = registry_url
        self._cache_ttl = cache_ttl
        self._schema_client = None
        self._serializer = None
        self._mapper = None

    def setup(self):
        from pipeline.utils.log_type_mappers import get_mapper
        from pipeline.utils.schema_registry import (
            AvroConfluentSerializer,
            SchemaRegistryClient,
        )

        self._schema_client = SchemaRegistryClient(
            self._registry_url,
            cache_ttl_seconds=self._cache_ttl,
        )
        self._serializer = AvroConfluentSerializer(self._schema_client)
        self._mapper = get_mapper(self._log_type)

    def process(self, element: Dict[str, Any], *args, **kwargs):
        import traceback as _traceback

        from pipeline.utils.avro_mapper import map_to_avro
        from pipeline.utils.log_type_mappers import get_subject

        try:
            subject = get_subject(self._log_type)
            schema_id, parsed_schema, named_types = (
                self._schema_client.get_latest_schema(subject)
            )

            nested = self._mapper.map(element)
            avro_record = map_to_avro(nested, parsed_schema, named_types=named_types)

            # Derive Kafka partition key from the record
            kafka_key = self._get_kafka_key(avro_record)

            serialised = self._serializer.serialize(
                avro_record, schema_id, parsed_schema
            )
            yield (kafka_key, serialised)

        except Exception as exc:
            logger.warning(
                "BuildAvroRecord failed: %s\n%s", exc, _traceback.format_exc()
            )
            # Build a new dict — never mutate the incoming element in-place as
            # Beam may share element references across concurrent operations.
            yield beam.pvalue.TaggedOutput(
                BuildAvroRecord.OUTPUT_TAG_DLQ,
                {
                    **{k: v for k, v in element.items() if k != "_publish_time_ms"},
                    "error": str(exc),
                    "stage": "build_avro",
                    "_publish_time_ms": element.get("_publish_time_ms"),
                },
            )

    def _get_kafka_key(self, avro_record: dict) -> bytes:
        """
        Partition key for Kafka: endpoint_id so all events for the same
        endpoint land on the same partition (order within endpoint preserved).
        Falls back to b"unknown" if the field is absent or None.
        """
        try:
            endpoint_id = avro_record.get("endpoint_id") or "unknown"
            return str(endpoint_id).encode("utf-8")
        except Exception:
            return b"unknown"


class WriteToDlq(beam.DoFn):
    """
    Write dead-letter records to GCS as newline-delimited JSON.

    The GCS client is created once per worker in setup() rather than per
    message — avoids re-initialising credentials on every call.
    """

    def __init__(self, dlq_bucket: str, log_type: str):
        self._bucket_name = dlq_bucket
        self._log_type = log_type
        self._gcs_client = None
        self._bucket = None

    def setup(self):
        from google.cloud import storage

        self._gcs_client = storage.Client()
        self._bucket = self._gcs_client.bucket(self._bucket_name)

    def teardown(self):
        self._gcs_client = None
        self._bucket = None

    def process(self, element, window=beam.DoFn.WindowParam, *args, **kwargs):
        import json as _json
        import logging as _logging
        import uuid as _uuid
        from datetime import datetime, timezone

        # Use a locally-obtained logger to avoid module-scope lookup issues
        # that can occur when Beam re-imports the DoFn in worker subprocesses.
        _log = _logging.getLogger(__name__)

        ts = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H")
        blob_name = f"dlq/{self._log_type}/{ts}/{_uuid.uuid4()}.json"
        try:
            blob = self._bucket.blob(blob_name)
            blob.upload_from_string(
                _json.dumps(element, default=str),
                content_type="application/json",
            )
            _log.info("DLQ record written: gs://%s/%s", self._bucket_name, blob_name)
        except Exception as exc:
            # Log and re-raise so Beam can retry the bundle.
            # Do NOT silently drop — a failure here means a DLQ message is lost.
            _log.error(
                "DLQ write failed for gs://%s/%s: %s",
                self._bucket_name,
                blob_name,
                exc,
            )
            raise


# ──────────────────────────────────────────────────────────────────────────────
# Pipeline construction
# ──────────────────────────────────────────────────────────────────────────────


def build_pipeline(pipeline: beam.Pipeline, opts: PubSubToKafkaAvroOptions):
    from pipeline.transforms.kafka_sink import WriteToKafkaAvro

    log_type = opts.log_type
    window_secs = opts.window_size_seconds

    # ── Read ──────────────────────────────────────────────────────────────────
    raw = pipeline | "ReadPubSub" >> ReadFromPubSub(
        subscription=opts.input_subscription,
        with_attributes=True,
    )

    # ── Parse ─────────────────────────────────────────────────────────────────
    parsed_results = raw | "ParseJson" >> beam.ParDo(ParseJsonMessage()).with_outputs(
        ParseJsonMessage.OUTPUT_TAG_DLQ, main="ok"
    )

    parsed = parsed_results["ok"]
    parse_dlq = parsed_results[ParseJsonMessage.OUTPUT_TAG_DLQ]

    # ── Window ────────────────────────────────────────────────────────────────
    windowed = parsed | "Window" >> beam.WindowInto(FixedWindows(window_secs))

    # ── Build Avro ────────────────────────────────────────────────────────────
    avro_results = windowed | "BuildAvro" >> beam.ParDo(
        BuildAvroRecord(
            log_type=log_type,
            registry_url=opts.kafka_registry_url,
            cache_ttl=opts.schema_cache_ttl_seconds,
        )
    ).with_outputs(BuildAvroRecord.OUTPUT_TAG_DLQ, main="ok")

    avro_records = avro_results["ok"]
    avro_dlq = avro_results[BuildAvroRecord.OUTPUT_TAG_DLQ]

    # ── Write to Kafka ────────────────────────────────────────────────────────
    kafka_results = avro_records | "WriteKafka" >> WriteToKafkaAvro(
        bootstrap_servers=opts.kafka_bootstrap_servers,
        topic=opts.kafka_topic,
    )
    kafka_dlq = kafka_results[WriteToKafkaAvro.OUTPUT_TAG_DLQ]

    # ── Dead letter — all three sources merged into one GCS sink ──────────────
    (
        (parse_dlq, avro_dlq, kafka_dlq)
        | "MergeDlq" >> beam.Flatten()
        | "WriteDlq" >> beam.ParDo(WriteToDlq(opts.dlq_bucket, log_type))
    )


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────


def run():
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True
    opts = pipeline_options.view_as(PubSubToKafkaAvroOptions)

    with beam.Pipeline(options=pipeline_options) as p:
        build_pipeline(p, opts)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    run()
