"""
Kafka Avro write transform for Google Managed Kafka.

Uses confluent-kafka (librdkafka) with SASL_SSL + OAUTHBEARER.
The oauth_cb callback fetches a Google ADC access token on each refresh —
no username/password required when the worker SA has roles/managedkafka.client.

Reference:
  https://cloud.google.com/managed-kafka/docs/authentication
  https://github.com/GoogleCloudPlatform/python-docs-samples/tree/main/managedkafka
"""

from __future__ import annotations

import logging
import traceback
from typing import Iterator, Tuple

import apache_beam as beam

logger = logging.getLogger(__name__)


def _make_oauth_cb():
    """
    Return an OAUTHBEARER token-refresh callback compatible with confluent-kafka
    and Google Managed Kafka.

    Google Managed Kafka requires a specific 3-part base64-encoded token format
    rather than a plain Bearer access token:

        base64url(header) + "." + base64url(claims) + "." + base64url(access_token)

    where:
        header  = {"typ": "JWT", "alg": "GOOG_OAUTH2_TOKEN"}
        claims  = {"exp": <unix_ts>, "iss": "Google", "iat": <unix_ts>,
                   "scope": "kafka", "sub": <service_account_email>}
        access_token = raw Google ADC access token (ya29....)

    Reference implementation:
      https://github.com/GoogleCloudPlatform/professional-services/blob/main/examples/mm2-gmk-migration/producer.py

    All imports are local so the function is safe to use inside a Beam DoFn
    that gets serialised and re-imported on worker subprocesses.
    """
    def _oauth_cb(config_str):  # config_str = sasl.oauthbearer.config value (unused)
        import base64
        import datetime
        import json
        import logging as _logging
        import time

        import google.auth
        import google.auth.transport.requests

        _log = _logging.getLogger(__name__)

        creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        req = google.auth.transport.requests.Request()
        creds.refresh(req)

        # Compute expiry as a UTC-aware datetime, then as a float Unix timestamp.
        expiry = creds.expiry  # datetime.datetime (UTC naive) or None
        if expiry is None:
            expiry = datetime.datetime.utcnow() + datetime.timedelta(hours=1)
        if expiry.tzinfo is None:
            utc_expiry = expiry.replace(tzinfo=datetime.timezone.utc)
        else:
            utc_expiry = expiry
        expiry_ts = utc_expiry.timestamp()

        sa_email = (
            getattr(creds, "service_account_email", None)
            or getattr(creds, "_service_account_email", None)
            or ""
        )

        # ── Build the Google Managed Kafka GOOG_OAUTH2_TOKEN format ──────────
        def _b64url(s: str) -> str:
            """URL-safe base64 without padding."""
            return base64.urlsafe_b64encode(s.encode("utf-8")).decode("utf-8").rstrip("=")

        header_b64 = _b64url(json.dumps({"typ": "JWT", "alg": "GOOG_OAUTH2_TOKEN"}))
        claims_b64 = _b64url(json.dumps({
            "exp": expiry_ts,
            "iss": "Google",
            "iat": time.time(),
            "scope": "kafka",
            "sub": sa_email,
        }))
        token_b64 = _b64url(creds.token or "")

        kafka_token = f"{header_b64}.{claims_b64}.{token_b64}"

        _log.info(
            "Kafka OAUTHBEARER token refreshed — cred_type=%s sa=%s token_prefix=%s",
            type(creds).__name__,
            sa_email or "n/a",
            (creds.token or "")[:8],
        )
        return kafka_token, expiry_ts

    return _oauth_cb


class KafkaAvroWriteFn(beam.DoFn):
    """
    Write ``(key_bytes, avro_bytes)`` tuples to a Kafka topic.

    Parameters
    ----------
    bootstrap_servers:
        Comma-separated Kafka bootstrap servers.
        For Google Managed Kafka:
        ``bootstrap.CLUSTER.REGION.managedkafka.PROJECT.cloud.goog:9092``
    topic:
        Destination Kafka topic name.
    """

    OUTPUT_TAG_SUCCESS = "success"
    OUTPUT_TAG_DLQ = "dead_letter"

    def __init__(self, bootstrap_servers: str, topic: str):
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._producer = None

    def setup(self):
        """Called once per worker instance — initialise the confluent-kafka Producer."""
        from confluent_kafka import Producer

        self._producer = Producer({
            "bootstrap.servers": self._bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "OAUTHBEARER",
            "oauth_cb": _make_oauth_cb(),
            # Durability: wait for all in-sync replicas before ack
            "acks": "all",
            # Batching for throughput
            "linger.ms": 50,
            "batch.size": 65536,
        })
        logger.info(
            "confluent-kafka Producer initialised → %s / topic=%s",
            self._bootstrap_servers,
            self._topic,
        )

    def process(self, element: Tuple[bytes, bytes], *args, **kwargs) -> Iterator:
        import logging as _logging
        import traceback as _traceback

        _log = _logging.getLogger(__name__)
        key, value = element

        # Capture delivery report in a mutable container visible to the callback
        delivery: dict = {}

        def _on_delivery(err, msg):
            delivery["err"] = err
            delivery["msg"] = msg

        try:
            self._producer.produce(
                self._topic,
                key=key,
                value=value,
                on_delivery=_on_delivery,
            )
            # Block until this message is acknowledged (or fails).
            # flush() drains ALL queued messages — since we produce one at a
            # time here, that is exactly our message.
            remaining = self._producer.flush(timeout=60)
            if remaining > 0:
                raise TimeoutError(
                    f"Kafka flush timed out — {remaining} message(s) still queued"
                )

            err = delivery.get("err")
            if err is not None:
                raise Exception(str(err))

            msg_meta = delivery.get("msg")
            _log.debug(
                "Kafka send OK: topic=%s partition=%s offset=%s",
                self._topic,
                msg_meta.partition() if msg_meta else "?",
                msg_meta.offset() if msg_meta else "?",
            )
            yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_SUCCESS, element)

        except Exception as exc:
            _log.warning(
                "Kafka send failed for topic '%s': %s\n%s",
                self._topic,
                exc,
                _traceback.format_exc(),
            )
            yield beam.pvalue.TaggedOutput(
                self.OUTPUT_TAG_DLQ,
                {
                    "key": key.decode("utf-8", errors="replace") if key else None,
                    "value_size": len(value) if value else 0,
                    "error": str(exc),
                    "stage": "kafka_write",
                },
            )

    def teardown(self):
        if self._producer is not None:
            try:
                self._producer.flush(timeout=30)
            except Exception as exc:
                logger.warning("confluent-kafka Producer teardown error: %s", exc)
            finally:
                self._producer = None


class WriteToKafkaAvro(beam.PTransform):
    """
    Composite transform: write Avro records to Kafka, return tagged output.

    Input PCollection: (key: bytes, value: bytes) tuples.
    Tagged outputs: ``"success"`` and ``"dead_letter"``.
    """

    OUTPUT_TAG_DLQ = KafkaAvroWriteFn.OUTPUT_TAG_DLQ

    def __init__(self, bootstrap_servers: str, topic: str):
        super().__init__()
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic

    def expand(self, pcoll):
        return pcoll | "KafkaWrite" >> beam.ParDo(
            KafkaAvroWriteFn(
                bootstrap_servers=self._bootstrap_servers,
                topic=self._topic,
            )
        ).with_outputs(
            KafkaAvroWriteFn.OUTPUT_TAG_DLQ,
            main=KafkaAvroWriteFn.OUTPUT_TAG_SUCCESS,
        )
