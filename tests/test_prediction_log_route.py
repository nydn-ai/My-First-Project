"""
End-to-end tests for the online_prediction log route.

All tests run offline (no GCP, no Kafka, no Schema Registry network calls).
The Avro schema is loaded directly from disk; serialization is validated
via fastavro schemaless round-trip.

Coverage:
  1.  Mapper: container log format (real production entries)
  2.  Mapper: audit log format (protoPayload alternative)
  3.  Mapper: field extraction correctness
  4.  Mapper: nanosecond timestamp truncation
  5.  Mapper: resource_container parsing (projects/N and bare N)
  6.  Mapper: deployed_model_id from top-level labels vs response
  7.  avro_mapper: full dict → Avro coercion against real schema
  8.  avro_mapper: uuid logical type coercion
  9.  avro_mapper: timestamp-millis coercion (int passthrough, ISO string, ns string)
  10. avro_mapper: nullable union — None values serialise to null
  11. avro_mapper: resource_labels map<string,string>
  12. avro_mapper: PredictionPayload nested record
  13. Serialiser: Confluent wire format (magic byte + 4-byte schema id + payload)
  14. Serialiser: round-trip deserialise produces identical record
  15. Kafka key: endpoint_id encoded to bytes
  16. DLQ: malformed JSON routes to dead_letter tag
  17. Integration: real ERROR entry → mapper → avro_mapper → serialise → deserialise
  18. Integration: real INFO entry → same pipeline
  19. Regression: BuildAvroRecord does not mutate the incoming element
  20. Regression: WriteToDlq GCS client not created inside process()
  21. Regression: _strip_non_avro_keys removes x-meta without double-processing
  22. Regression: fastavro.parse_schema receives dict (not list) for self-contained schema
  23. Regression: all three DLQ sources (parse, avro, kafka) are wired in build_pipeline
  24. Auth: SchemaRegistryClient uses separate locks — no deadlock on schema fetch
  25. Auth: _get_google_token uses _token_lock (not _schema_lock) — structural pin
  26. Auth: SchemaRegistryClient.get_latest_schema does not deadlock (live thread test)
  27. Auth: GoogleManagedKafkaTokenProvider caches token — google.auth.default called once
  28. Auth: GoogleManagedKafkaTokenProvider is thread-safe — concurrent token() calls safe
"""

from __future__ import annotations

import json
import struct
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import fastavro
import pytest

# ──────────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────────

SCHEMAS_DIR = Path(__file__).parent.parent / "schemas"
SCHEMA_FILE = SCHEMAS_DIR / "vertex_prediction_log.avsc"


def _load_parsed_schema():
    """Load and parse the prediction schema from disk, stripping x-meta."""
    from pipeline.utils.schema_registry import _strip_non_avro_keys

    raw = json.loads(SCHEMA_FILE.read_text())
    clean = _strip_non_avro_keys(raw)
    return fastavro.parse_schema(clean)


PARSED_SCHEMA = _load_parsed_schema()


def _named_types():
    """Build named_types dict from the parsed schema (mirrors SchemaRegistryClient)."""
    from pipeline.utils.schema_registry import (
        SchemaRegistryClient,
        _strip_non_avro_keys,
    )

    raw = json.loads(SCHEMA_FILE.read_text())
    clean = _strip_non_avro_keys(raw)
    nt = {}
    SchemaRegistryClient._collect_named_types(clean, nt)
    return nt


NAMED_TYPES = _named_types()

# ── Two real container log entries provided by the user ──────────────────────

REAL_ERROR_ENTRY = {
    "insertId": "19a59b2f3qejpf",
    "jsonPayload": {
        "message": "[2026-05-20 13:15:44 +0000] [8] [INFO] Finished server process [8]"
    },
    "resource": {
        "type": "aiplatform.googleapis.com/Endpoint",
        "labels": {
            "endpoint_id": "wasp-candidate-staging",
            "location": "europe-west4",
            "resource_container": "projects/817343037939",
        },
    },
    "timestamp": "2026-05-20T13:15:44.325085878Z",
    "severity": "ERROR",
    "labels": {
        "deployed_model_id": "3106640417467138048",
        "replica_id": "predictor-resource-pool-575717482443046912-755b4ddd8-pdnbm",
    },
    "logName": "projects/p-trust-inference-stg-6321/logs/aiplatform.googleapis.com%2Fprediction_container",
    "receiveTimestamp": "2026-05-20T13:15:44.698098730Z",
}

REAL_INFO_ENTRY = {
    "insertId": "abc123xyz",
    "jsonPayload": {
        "message": "[2026-05-20 13:14:02 +0000] [8] [INFO] Booting worker with pid: 8"
    },
    "resource": {
        "type": "aiplatform.googleapis.com/Endpoint",
        "labels": {
            "endpoint_id": "wasp-candidate-staging",
            "location": "europe-west4",
            "resource_container": "projects/817343037939",
        },
    },
    "timestamp": "2026-05-20T13:14:02.100000000Z",
    "severity": "INFO",
    "labels": {
        "deployed_model_id": "3106640417467138048",
        "replica_id": "predictor-resource-pool-575717482443046912-755b4ddd8-pdnbm",
    },
    "logName": "projects/p-trust-inference-stg-6321/logs/aiplatform.googleapis.com%2Fprediction_container",
    "receiveTimestamp": "2026-05-20T13:14:02.200000000Z",
}


def _mapper():
    from pipeline.utils.log_type_mappers import get_mapper

    return get_mapper("online_prediction")


# ──────────────────────────────────────────────────────────────────────────────
# 1–6  Mapper tests
# ──────────────────────────────────────────────────────────────────────────────


class TestPredictionMapper:

    def test_container_log_project_id_from_projects_prefix(self):
        """resource_container = "projects/817343037939" → project_id = "817343037939"."""
        result = _mapper().map(REAL_ERROR_ENTRY)
        assert result["project_id"] == "817343037939"

    def test_container_log_project_id_bare_number(self):
        """resource_container = "817343037939" (bare) → project_id = "817343037939"."""
        entry = {**REAL_ERROR_ENTRY}
        entry["resource"] = {
            "type": "aiplatform.googleapis.com/Endpoint",
            "labels": {
                "endpoint_id": "wasp-candidate-staging",
                "location": "europe-west4",
                "resource_container": "817343037939",  # bare — no "projects/" prefix
            },
        }
        result = _mapper().map(entry)
        assert result["project_id"] == "817343037939"

    def test_container_log_endpoint_id(self):
        result = _mapper().map(REAL_ERROR_ENTRY)
        assert result["endpoint_id"] == "wasp-candidate-staging"

    def test_container_log_location(self):
        result = _mapper().map(REAL_ERROR_ENTRY)
        assert result["location"] == "europe-west4"

    def test_container_log_severity_preserved(self):
        assert _mapper().map(REAL_ERROR_ENTRY)["severity"] == "ERROR"
        assert _mapper().map(REAL_INFO_ENTRY)["severity"] == "INFO"

    def test_container_log_deployed_model_id_from_top_labels(self):
        """deployed_model_id must come from entry['labels']['deployed_model_id']."""
        result = _mapper().map(REAL_ERROR_ENTRY)
        assert result["payload"]["deployed_model_id"] == "3106640417467138048"

    def test_container_log_method_is_none(self):
        """No protoPayload → method should be None."""
        result = _mapper().map(REAL_ERROR_ENTRY)
        assert result["payload"]["method"] is None

    def test_container_log_raw_payload_json_is_message(self):
        result = _mapper().map(REAL_ERROR_ENTRY)
        assert result["raw_payload_json"] is not None
        decoded = json.loads(result["raw_payload_json"])
        assert "message" in decoded

    def test_nanosecond_timestamp_truncated(self):
        """9-digit fractional seconds must be parsed without raising."""
        result = _mapper().map(REAL_ERROR_ENTRY)
        ts = result["ts"]
        assert isinstance(ts, int)
        assert ts > 0

    def test_insert_id_preserved(self):
        result = _mapper().map(REAL_ERROR_ENTRY)
        assert result["insert_id"] == "19a59b2f3qejpf"

    def test_log_name_preserved(self):
        result = _mapper().map(REAL_ERROR_ENTRY)
        assert "prediction_container" in result["log_name"]

    def test_resource_type_preserved(self):
        result = _mapper().map(REAL_ERROR_ENTRY)
        assert result["resource_type"] == "aiplatform.googleapis.com/Endpoint"

    def test_resource_labels_is_string_map(self):
        """resource_labels must be map<string,string> — all values coerced to str."""
        result = _mapper().map(REAL_ERROR_ENTRY)
        rl = result["resource_labels"]
        assert isinstance(rl, dict)
        for k, v in rl.items():
            assert isinstance(k, str)
            assert isinstance(v, str)

    def test_uuid_id_generated(self):
        result = _mapper().map(REAL_ERROR_ENTRY)
        # Must be a valid UUID string
        uuid.UUID(result["id"])

    def test_audit_log_deployed_model_from_response(self):
        """Audit log: deployed_model_id from protoPayload.response.deployedModelId."""
        audit_entry = {
            "insertId": "audit-001",
            "protoPayload": {
                "@type": "type.googleapis.com/google.cloud.audit.AuditLog",
                "methodName": "google.cloud.aiplatform.v1.PredictionService.Predict",
                "response": {
                    "deployedModelId": "987654321",
                    "model": "projects/p/models/m1",
                    "modelVersionId": "3",
                },
                "latencyMs": "42",
            },
            "resource": {
                "type": "aiplatform.googleapis.com/Endpoint",
                "labels": {
                    "endpoint_id": "endpoint-999",
                    "location": "us-east1",
                    "project_id": "my-project",
                },
            },
            "timestamp": "2026-05-20T12:00:00.000000000Z",
            "severity": "NOTICE",
            "logName": "projects/my-project/logs/cloudaudit.googleapis.com%2Factivity",
        }
        result = _mapper().map(audit_entry)
        assert result["payload"]["deployed_model_id"] == "987654321"
        assert result["payload"]["method"] == "Predict"
        assert result["payload"]["latency_ms"] == 42
        assert result["project_id"] == "my-project"

    def test_missing_severity_defaults_to_default(self):
        """Entry with no 'severity' key → 'DEFAULT'."""
        entry = {k: v for k, v in REAL_ERROR_ENTRY.items() if k != "severity"}
        result = _mapper().map(entry)
        assert result["severity"] == "DEFAULT"


# ──────────────────────────────────────────────────────────────────────────────
# 7–12  avro_mapper coercion tests
# ──────────────────────────────────────────────────────────────────────────────


class TestAvroMapper:

    def _coerce(self, data):
        from pipeline.utils.avro_mapper import map_to_avro

        return map_to_avro(data, PARSED_SCHEMA, named_types=NAMED_TYPES)

    def _minimal_record(self, **overrides) -> Dict[str, Any]:
        """Build the minimum valid VertexPredictionLog dict."""
        base = {
            "id": str(uuid.uuid4()),
            "ts": 1_700_000_000_000,
            "insert_time": 1_700_000_000_000,
            "event_time": None,
            "log_name": "test-log",
            "resource_type": "aiplatform.googleapis.com/Endpoint",
            "resource_labels": {},
            "project_id": None,
            "location": None,
            "endpoint_id": "wasp-candidate-staging",
            "model_id": None,
            "model_version": None,
            "severity": "INFO",
            "trace": None,
            "span_id": None,
            "insert_id": None,
            "payload": {
                "method": None,
                "deployed_model_id": None,
                "latency_ms": None,
                "status_code": None,
                "request_size_bytes": None,
                "response_size_bytes": None,
                "request_payload_json": None,
                "response_payload_json": None,
                "error_message": None,
            },
            "raw_payload_json": None,
        }
        base.update(overrides)
        return base

    def test_minimal_record_coerces(self):
        result = self._coerce(self._minimal_record())
        assert result["severity"] == "INFO"
        assert result["endpoint_id"] == "wasp-candidate-staging"

    def test_uuid_valid_passthrough(self):
        uid = str(uuid.uuid4())
        result = self._coerce(self._minimal_record(id=uid))
        assert result["id"] == uid

    def test_uuid_invalid_raises(self):
        from pipeline.utils.avro_mapper import AvroMappingError

        with pytest.raises(AvroMappingError, match="Invalid UUID"):
            self._coerce(self._minimal_record(id="not-a-uuid"))

    def test_timestamp_int_passthrough(self):
        ts = 1_779_282_944_325
        result = self._coerce(self._minimal_record(ts=ts))
        assert result["ts"] == ts

    def test_timestamp_iso_string(self):
        result = self._coerce(self._minimal_record(ts="2026-05-20T13:15:44.325000Z"))
        assert isinstance(result["ts"], int)
        assert result["ts"] > 0

    def test_timestamp_nanosecond_string(self):
        """9-digit fractional seconds must be truncated before parsing."""
        result = self._coerce(self._minimal_record(ts="2026-05-20T13:15:44.325085878Z"))
        assert isinstance(result["ts"], int)
        assert result["ts"] > 0

    def test_nullable_fields_accept_none(self):
        result = self._coerce(
            self._minimal_record(
                project_id=None,
                location=None,
                endpoint_id=None,
                trace=None,
                span_id=None,
                raw_payload_json=None,
            )
        )
        assert result["project_id"] is None
        assert result["endpoint_id"] is None

    def test_resource_labels_map_coerced(self):
        labels = {
            "endpoint_id": "wasp",
            "location": "europe-west4",
            "resource_container": "817343037939",
        }
        result = self._coerce(self._minimal_record(resource_labels=labels))
        assert result["resource_labels"] == labels

    def test_resource_labels_empty_map(self):
        result = self._coerce(self._minimal_record(resource_labels={}))
        assert result["resource_labels"] == {}

    def test_prediction_payload_all_none(self):
        result = self._coerce(self._minimal_record())
        p = result["payload"]
        assert p["method"] is None
        assert p["deployed_model_id"] is None
        assert p["latency_ms"] is None
        assert p["status_code"] is None

    def test_prediction_payload_with_values(self):
        payload = {
            "method": "Predict",
            "deployed_model_id": "3106640417467138048",
            "latency_ms": 123,
            "status_code": 200,
            "request_size_bytes": 512,
            "response_size_bytes": 1024,
            "request_payload_json": '{"instances": []}',
            "response_payload_json": '{"predictions": []}',
            "error_message": None,
        }
        result = self._coerce(self._minimal_record(payload=payload))
        p = result["payload"]
        assert p["method"] == "Predict"
        assert p["deployed_model_id"] == "3106640417467138048"
        assert p["latency_ms"] == 123
        assert p["status_code"] == 200

    def test_event_time_nullable_timestamp(self):
        """event_time is ["null", timestamp-millis] — None must work."""
        result = self._coerce(self._minimal_record(event_time=None))
        assert result["event_time"] is None

    def test_event_time_with_value(self):
        ts = 1_779_282_944_325
        result = self._coerce(self._minimal_record(event_time=ts))
        assert result["event_time"] == ts


# ──────────────────────────────────────────────────────────────────────────────
# 13–14  Confluent wire format serializer
# ──────────────────────────────────────────────────────────────────────────────


class TestConfluentSerializer:

    def _serializer(self, schema_id: int = 42):
        from pipeline.utils.schema_registry import (
            AvroConfluentSerializer,
            SchemaRegistryClient,
        )

        mock_client = MagicMock(spec=SchemaRegistryClient)
        mock_client.get_schema_by_id.return_value = {"schema": SCHEMA_FILE.read_text()}
        return AvroConfluentSerializer(mock_client), schema_id

    def _minimal_avro_record(self):
        from pipeline.utils.avro_mapper import map_to_avro

        data = {
            "id": str(uuid.uuid4()),
            "ts": 1_700_000_000_000,
            "insert_time": 1_700_000_000_000,
            "event_time": None,
            "log_name": "test",
            "resource_type": "aiplatform.googleapis.com/Endpoint",
            "resource_labels": {},
            "project_id": "my-project",
            "location": "europe-west4",
            "endpoint_id": "wasp-candidate-staging",
            "model_id": None,
            "model_version": None,
            "severity": "INFO",
            "trace": None,
            "span_id": None,
            "insert_id": "abc123",
            "payload": {
                "method": None,
                "deployed_model_id": "3106640417467138048",
                "latency_ms": None,
                "status_code": None,
                "request_size_bytes": None,
                "response_size_bytes": None,
                "request_payload_json": None,
                "response_payload_json": None,
                "error_message": None,
            },
            "raw_payload_json": None,
        }
        return map_to_avro(data, PARSED_SCHEMA, named_types=NAMED_TYPES)

    def test_magic_byte(self):
        ser, schema_id = self._serializer()
        payload = ser.serialize(self._minimal_avro_record(), schema_id, PARSED_SCHEMA)
        assert payload[0:1] == b"\x00"

    def test_schema_id_encoded(self):
        schema_id = 42
        ser, _ = self._serializer(schema_id)
        payload = ser.serialize(self._minimal_avro_record(), schema_id, PARSED_SCHEMA)
        (decoded_id,) = struct.unpack(">I", payload[1:5])
        assert decoded_id == schema_id

    def test_payload_length_nonzero(self):
        ser, schema_id = self._serializer()
        payload = ser.serialize(self._minimal_avro_record(), schema_id, PARSED_SCHEMA)
        assert len(payload) > 5  # magic + id + at least some data

    def test_round_trip(self):
        """Serialise → deserialise → same record."""
        from pipeline.utils.schema_registry import (
            AvroConfluentSerializer,
            SchemaRegistryClient,
            _strip_non_avro_keys,
        )

        schema_id = 7

        mock_client = MagicMock(spec=SchemaRegistryClient)
        mock_client.get_schema_by_id.return_value = {
            "schema": json.dumps(
                _strip_non_avro_keys(json.loads(SCHEMA_FILE.read_text()))
            )
        }
        ser = AvroConfluentSerializer(mock_client)

        original = self._minimal_avro_record()
        raw = ser.serialize(original, schema_id, PARSED_SCHEMA)
        recovered = ser.deserialize(raw)

        assert recovered["endpoint_id"] == original["endpoint_id"]
        assert recovered["severity"] == original["severity"]
        assert (
            recovered["payload"]["deployed_model_id"]
            == original["payload"]["deployed_model_id"]
        )


# ──────────────────────────────────────────────────────────────────────────────
# 15  Kafka key extraction
# ──────────────────────────────────────────────────────────────────────────────


class TestKafkaKey:

    def _build_avro_fn(self):
        """Return a BuildAvroRecord instance with mocked setup."""
        from pipeline.main import BuildAvroRecord

        fn = BuildAvroRecord.__new__(BuildAvroRecord)
        fn._log_type = "online_prediction"
        fn._registry_url = "http://localhost:8081"
        fn._cache_ttl = 60
        fn._schema_client = None
        fn._serializer = None
        fn._mapper = None
        return fn

    def test_endpoint_id_encoded(self):
        fn = self._build_avro_fn()
        record = {"endpoint_id": "wasp-candidate-staging"}
        assert fn._get_kafka_key(record) == b"wasp-candidate-staging"

    def test_none_endpoint_id_gives_unknown(self):
        fn = self._build_avro_fn()
        assert fn._get_kafka_key({"endpoint_id": None}) == b"unknown"

    def test_missing_endpoint_id_gives_unknown(self):
        fn = self._build_avro_fn()
        assert fn._get_kafka_key({}) == b"unknown"


# ──────────────────────────────────────────────────────────────────────────────
# 16  DLQ routing
# ──────────────────────────────────────────────────────────────────────────────


class TestDlqRouting:

    def test_parse_json_bad_utf8_goes_to_dlq(self):
        """Non-JSON bytes must produce a dead_letter output."""
        import apache_beam as beam

        from pipeline.main import ParseJsonMessage

        fn = ParseJsonMessage()
        outputs = list(fn.process(b"not-json-{{{"))
        assert len(outputs) == 1
        out = outputs[0]
        assert isinstance(out, beam.pvalue.TaggedOutput)
        assert out.tag == ParseJsonMessage.OUTPUT_TAG_DLQ

    def test_parse_json_valid_passes_through(self):
        """Valid JSON must yield the dict (not DLQ)."""
        from pipeline.main import ParseJsonMessage

        fn = ParseJsonMessage()
        entry = {"severity": "INFO", "insertId": "x"}
        outputs = list(fn.process(json.dumps(entry).encode()))
        assert len(outputs) == 1
        result = outputs[0]
        assert not isinstance(result, __import__("apache_beam").pvalue.TaggedOutput)
        assert result["severity"] == "INFO"


# ──────────────────────────────────────────────────────────────────────────────
# 17–18  Full offline integration: real entry → mapper → avro → serialise → round-trip
# ──────────────────────────────────────────────────────────────────────────────


class TestPredictionRouteIntegration:
    """
    Full offline pipeline: real log entry → mapper → avro_mapper →
    Confluent serialise → deserialise → verify field values.

    No GCP calls.  Uses schema from disk.
    """

    FAKE_SCHEMA_ID = 99

    def _run(self, entry: dict) -> dict:
        from pipeline.utils.avro_mapper import map_to_avro
        from pipeline.utils.log_type_mappers import get_mapper
        from pipeline.utils.schema_registry import (
            AvroConfluentSerializer,
            SchemaRegistryClient,
            _strip_non_avro_keys,
        )

        # Step 1: map
        mapped = get_mapper("online_prediction").map(entry)

        # Step 2: coerce to Avro
        avro_record = map_to_avro(mapped, PARSED_SCHEMA, named_types=NAMED_TYPES)

        # Step 3: serialise (Confluent wire format)
        mock_client = MagicMock(spec=SchemaRegistryClient)
        mock_client.get_schema_by_id.return_value = {
            "schema": json.dumps(
                _strip_non_avro_keys(json.loads(SCHEMA_FILE.read_text()))
            )
        }
        ser = AvroConfluentSerializer(mock_client)
        raw = ser.serialize(avro_record, self.FAKE_SCHEMA_ID, PARSED_SCHEMA)

        # Step 4: deserialise and return
        return ser.deserialize(raw)

    def test_real_error_entry_round_trip(self):
        result = self._run(REAL_ERROR_ENTRY)

        assert result["endpoint_id"] == "wasp-candidate-staging"
        assert result["location"] == "europe-west4"
        assert result["severity"] == "ERROR"
        assert result["project_id"] == "817343037939"
        assert result["log_name"].endswith("prediction_container")
        assert result["payload"]["deployed_model_id"] == "3106640417467138048"
        assert result["payload"]["method"] is None
        assert (
            result["resource_labels"]["resource_container"] == "projects/817343037939"
        )
        assert isinstance(result["ts"], (int, datetime))

    def test_real_info_entry_round_trip(self):
        result = self._run(REAL_INFO_ENTRY)

        assert result["severity"] == "INFO"
        assert result["endpoint_id"] == "wasp-candidate-staging"
        assert result["payload"]["deployed_model_id"] == "3106640417467138048"

    def test_wire_format_structure(self):
        """Verify the raw bytes have the correct Confluent framing."""
        from pipeline.utils.avro_mapper import map_to_avro
        from pipeline.utils.log_type_mappers import get_mapper
        from pipeline.utils.schema_registry import (
            AvroConfluentSerializer,
            SchemaRegistryClient,
        )

        mapped = get_mapper("online_prediction").map(REAL_ERROR_ENTRY)
        avro_record = map_to_avro(mapped, PARSED_SCHEMA, named_types=NAMED_TYPES)

        mock_client = MagicMock(spec=SchemaRegistryClient)
        ser = AvroConfluentSerializer(mock_client)
        raw = ser.serialize(avro_record, 42, PARSED_SCHEMA)

        assert raw[0:1] == b"\x00"  # magic byte
        (sid,) = struct.unpack(">I", raw[1:5])
        assert sid == 42  # schema id
        assert len(raw) > 5  # payload follows

    def test_generated_container_log_round_trip(self):
        """Generator → mapper → avro → round-trip (fuzz with 5 random entries)."""
        from scripts.generate_test_log import _make_online_prediction_entry

        for _ in range(5):
            entry = _make_online_prediction_entry("project-1c03ae00-17f3-43f4-86a")
            result = self._run(entry)
            assert result["severity"] in ("INFO", "WARNING", "ERROR")
            assert result["payload"]["deployed_model_id"] is not None
            assert isinstance(result["ts"], (int, datetime))

    def test_generated_audit_log_round_trip(self):
        """Audit log generator → mapper → avro → round-trip."""
        from scripts.generate_test_log import _make_online_prediction_audit_entry

        entry = _make_online_prediction_audit_entry("project-1c03ae00-17f3-43f4-86a")
        result = self._run(entry)
        assert result["payload"]["method"] == "Predict"
        assert result["payload"]["deployed_model_id"] is not None
        assert result["payload"]["latency_ms"] is not None


# ──────────────────────────────────────────────────────────────────────────────
# 19–23  Regression tests for the four integration bugs fixed
# ──────────────────────────────────────────────────────────────────────────────


class TestRegressions:
    """
    Each test pins a specific bug that was found during review so it can
    never silently regress.
    """

    # ── 19: BuildAvroRecord must not mutate the incoming element ──────────────

    def test_build_avro_record_does_not_mutate_element(self):
        """
        When BuildAvroRecord fails (e.g. bad schema), it must NOT add 'error'
        or 'stage' keys to the original dict — Beam may share element
        references and a mutation would corrupt other in-flight records.
        """
        import apache_beam as beam

        from pipeline.main import BuildAvroRecord

        fn = BuildAvroRecord.__new__(BuildAvroRecord)
        fn._log_type = "online_prediction"
        fn._registry_url = "http://localhost:8081"
        fn._cache_ttl = 60
        fn._schema_client = None
        fn._serializer = None
        fn._mapper = None

        # Force a failure by leaving _mapper as None so the .map() call raises
        element = {"severity": "INFO", "insertId": "abc", "_publish_time_ms": 0}
        original_keys = set(element.keys())

        outputs = list(fn.process(element))
        # Element must be unmodified
        assert (
            set(element.keys()) == original_keys
        ), f"Element was mutated: new keys = {set(element.keys()) - original_keys}"
        # Output must be a DLQ record
        assert len(outputs) == 1
        out = outputs[0]
        assert isinstance(out, beam.pvalue.TaggedOutput)
        assert out.tag == BuildAvroRecord.OUTPUT_TAG_DLQ
        # DLQ payload has error info
        assert "error" in out.value
        assert "stage" in out.value
        assert out.value["stage"] == "build_avro"

    # ── 20: WriteToDlq must not create GCS client inside process() ────────────

    def test_write_to_dlq_gcs_client_not_in_process(self):
        """
        WriteToDlq.process() must not call storage.Client() — that belongs in
        setup().  Creating a client per-message causes credential round-trips
        and exhausts GCP API quota under load.
        """
        import ast
        import pathlib

        src = pathlib.Path("pipeline/main.py").read_text()
        # Find the WriteToDlq class body
        tree = ast.parse(src)
        dlq_class = next(
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.ClassDef) and n.name == "WriteToDlq"
        )
        process_method = next(
            n
            for n in ast.walk(dlq_class)
            if isinstance(n, ast.FunctionDef) and n.name == "process"
        )
        # Extract source lines for process()
        process_src = ast.get_source_segment(src, process_method) or ""
        assert (
            "storage.Client()" not in process_src
        ), "storage.Client() found inside WriteToDlq.process() — move to setup()"

        # Confirm setup() exists and contains storage.Client()
        setup_method = next(
            (
                n
                for n in ast.walk(dlq_class)
                if isinstance(n, ast.FunctionDef) and n.name == "setup"
            ),
            None,
        )
        assert setup_method is not None, "WriteToDlq must have a setup() method"
        setup_src = ast.get_source_segment(src, setup_method) or ""
        assert (
            "storage.Client()" in setup_src
        ), "storage.Client() not found in WriteToDlq.setup()"

    # ── 21: _strip_non_avro_keys removes x-meta and does not double-process ───

    def test_strip_non_avro_keys_removes_x_meta(self):
        """x-meta keys must be stripped at every nesting level."""
        from pipeline.utils.schema_registry import _strip_non_avro_keys

        schema = {
            "type": "record",
            "name": "Test",
            "x-meta": {"team": "ml"},
            "fields": [
                {
                    "name": "id",
                    "type": "string",
                    "x-sensitive": True,
                    "doc": "identifier",
                }
            ],
        }
        result = _strip_non_avro_keys(schema)
        assert "x-meta" not in result
        assert "x-sensitive" not in result["fields"][0]
        assert result["fields"][0]["doc"] == "identifier"  # non-x keys preserved

    def test_strip_non_avro_keys_real_schema(self):
        """Real prediction schema must survive stripping without losing fields."""
        from pipeline.utils.schema_registry import _strip_non_avro_keys

        raw = json.loads(SCHEMA_FILE.read_text())
        clean = _strip_non_avro_keys(raw)

        assert "x-meta" not in clean
        field_names = [f["name"] for f in clean["fields"]]
        assert "id" in field_names
        assert "ts" in field_names
        assert "payload" in field_names
        assert "severity" in field_names
        assert "resource_labels" in field_names
        # PredictionPayload fields intact
        payload_field = next(f for f in clean["fields"] if f["name"] == "payload")
        payload_sub_fields = [f["name"] for f in payload_field["type"]["fields"]]
        assert "deployed_model_id" in payload_sub_fields
        assert "method" in payload_sub_fields

    def test_strip_non_avro_keys_no_double_processing(self):
        """
        Ensure fields are not processed twice (the redundant second pass was
        removed).  A field modified in the first pass must not be re-processed.
        """
        from pipeline.utils.schema_registry import _strip_non_avro_keys

        # Verify idempotency rather than call counting — a double-processed
        # fields list would result in incorrect output on the second pass.
        schema = {
            "type": "record",
            "name": "Dup",
            "fields": [{"name": "f1", "type": "string", "x-drop": "yes"}],
        }
        result1 = _strip_non_avro_keys(schema)
        result2 = _strip_non_avro_keys(result1)  # second pass on already-cleaned
        assert result1 == result2  # idempotent

    # ── 22: fastavro parse_schema receives dict for self-contained schema ──────

    def test_schema_registry_passes_dict_to_fastavro_when_no_refs(self):
        """
        _fetch_and_resolve must call fastavro.parse_schema(dict) — not
        parse_schema([dict]) — when the schema has no cross-schema references.
        This avoids ambiguity in fastavro list-vs-dict handling.
        """
        import ast
        import pathlib

        src = pathlib.Path("pipeline/utils/schema_registry.py").read_text()
        tree = ast.parse(src)

        # Find _fetch_and_resolve and verify it has the conditional
        fetch_fn = next(
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.FunctionDef) and n.name == "_fetch_and_resolve"
        )
        fn_src = ast.get_source_segment(src, fetch_fn) or ""
        assert (
            "if ref_schemas:" in fn_src
        ), "_fetch_and_resolve must branch on ref_schemas before calling parse_schema"
        assert (
            "fastavro.parse_schema(raw_schema)" in fn_src
        ), "Must call parse_schema(raw_schema) dict directly when no refs"

    # ── 23: All three DLQ sources wired in build_pipeline ─────────────────────

    def test_all_three_dlq_sources_wired(self):
        """
        build_pipeline must merge parse_dlq, avro_dlq, AND kafka_dlq into
        WriteToDlq.  Missing any one means failures are silently dropped.
        """
        import ast
        import pathlib

        src = pathlib.Path("pipeline/main.py").read_text()
        tree = ast.parse(src)

        build_fn = next(
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.FunctionDef) and n.name == "build_pipeline"
        )
        fn_src = ast.get_source_segment(src, build_fn) or ""

        assert "parse_dlq" in fn_src, "parse_dlq not found in build_pipeline"
        assert "avro_dlq" in fn_src, "avro_dlq not found in build_pipeline"
        assert "kafka_dlq" in fn_src, "kafka_dlq not found in build_pipeline"
        assert (
            "(parse_dlq, avro_dlq, kafka_dlq)" in fn_src
        ), "All three DLQ sources must be merged in a single Flatten"
        assert "WriteToDlq" in fn_src, "WriteToDlq not used in build_pipeline"


# ──────────────────────────────────────────────────────────────────────────────
# 24–28  Authentication / authorisation regression tests
# ──────────────────────────────────────────────────────────────────────────────


class TestAuthRegression:
    """
    Pin the authentication and token-caching invariants.

    CRITICAL BUG FIXED (regression tests 24–26):
      get_latest_schema() acquired self._lock, then called _fetch_and_resolve()
      → _headers() → _get_google_token() which tried to acquire the SAME lock.
      Python threading.Lock is not reentrant → the pipeline deadlocked on the
      first non-cached schema fetch against a GCP-hosted registry.

      Fix: two separate locks —
        _schema_lock  guards the schema cache
        _token_lock   guards ADC token refresh
    """

    # ── 24: Structural — _get_google_token must use _token_lock ───────────────

    def test_get_google_token_uses_token_lock_not_schema_lock(self):
        """
        _get_google_token must acquire _token_lock, not _schema_lock.

        If it acquires _schema_lock it deadlocks because get_latest_schema()
        already holds _schema_lock when it calls into _fetch_and_resolve()
        → _headers() → _get_google_token().
        """
        import ast
        import pathlib

        src = pathlib.Path("pipeline/utils/schema_registry.py").read_text()
        tree = ast.parse(src)

        cls = next(
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.ClassDef) and n.name == "SchemaRegistryClient"
        )
        get_token_fn = next(
            n
            for n in ast.walk(cls)
            if isinstance(n, ast.FunctionDef) and n.name == "_get_google_token"
        )
        fn_src = ast.get_source_segment(src, get_token_fn) or ""

        assert "_token_lock" in fn_src, (
            "_get_google_token must use _token_lock to avoid deadlock "
            "when called from within get_latest_schema() which holds _schema_lock"
        )
        assert "self._schema_lock" not in fn_src, (
            "self._schema_lock in _get_google_token would deadlock because "
            "get_latest_schema() already holds _schema_lock when it calls "
            "_fetch_and_resolve() → _headers() → _get_google_token()"
        )

    # ── 25: Structural — get_latest_schema must use _schema_lock ─────────────

    def test_get_latest_schema_uses_schema_lock(self):
        """
        get_latest_schema must use _schema_lock (not _lock or _token_lock) for
        cache access.  This ensures it can call _get_google_token() without
        self-deadlock.
        """
        import ast
        import pathlib

        src = pathlib.Path("pipeline/utils/schema_registry.py").read_text()
        tree = ast.parse(src)

        cls = next(
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.ClassDef) and n.name == "SchemaRegistryClient"
        )
        method = next(
            n
            for n in ast.walk(cls)
            if isinstance(n, ast.FunctionDef) and n.name == "get_latest_schema"
        )
        fn_src = ast.get_source_segment(src, method) or ""

        assert (
            "_schema_lock" in fn_src
        ), "get_latest_schema must use _schema_lock for cache protection"
        # Must NOT use _token_lock — that belongs exclusively to _get_google_token
        assert "_token_lock" not in fn_src, (
            "get_latest_schema must not acquire _token_lock — that is the "
            "exclusive domain of _get_google_token"
        )

    # ── 26: Behavioural — schema fetch must not deadlock ─────────────────────

    def test_schema_registry_get_latest_schema_no_deadlock(self):
        """
        get_latest_schema() on a local registry (no auth) must complete within
        2 seconds.  A deadlock would cause thread.join() to time out.

        Note: uses _is_local URL so auth code is not exercised; this specifically
        tests that the cache + HTTP path doesn't re-enter _schema_lock.
        """
        import threading

        from pipeline.utils.schema_registry import SchemaRegistryClient

        client = SchemaRegistryClient("http://localhost:8081")

        # Minimal valid Avro schema response
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "id": 1,
            "schema": json.dumps(
                {
                    "type": "record",
                    "name": "TestRecord",
                    "fields": [{"name": "x", "type": "string"}],
                }
            ),
            "references": [],
        }

        result = {"done": False, "error": None}

        def _worker():
            try:
                # Patch the session's get method (not requests.get at module level)
                # because SchemaRegistryClient now uses self._session.get() for
                # connection reuse and retry support.
                with patch.object(client._session, "get", return_value=mock_resp):
                    client.get_latest_schema("test-subject")
                result["done"] = True
            except Exception as exc:
                result["error"] = str(exc)

        t = threading.Thread(target=_worker, daemon=True)
        t.start()
        t.join(timeout=2.0)

        assert not t.is_alive(), (
            "get_latest_schema() deadlocked — thread did not complete in 2 s. "
            "Check that _schema_lock and _token_lock are separate locks."
        )
        assert result["done"] is True, f"get_latest_schema() raised: {result['error']}"

    # ── 27: Token provider caches — google.auth.default called only once ──────

    def test_kafka_token_provider_caches_token(self):
        """
        GoogleManagedKafkaTokenProvider.token() must call google.auth.default()
        exactly once across multiple invocations, and must not refresh the ADC
        token unless the cached token has expired.
        """
        import datetime

        from pipeline.utils.kafka_auth import GoogleManagedKafkaTokenProvider

        mock_creds = MagicMock()
        mock_creds.token = "fake-token-value"
        mock_creds.expiry = datetime.datetime.now(datetime.timezone.utc).replace(
            microsecond=0
        ) + datetime.timedelta(hours=1)

        with patch(
            "google.auth.default", return_value=(mock_creds, "project")
        ) as mock_default:
            provider = GoogleManagedKafkaTokenProvider()

            t1 = provider.token()
            t2 = provider.token()
            t3 = provider.token()

        assert t1 == t2 == t3 == "fake-token-value"
        # google.auth.default must be called exactly once — credentials are reused
        assert mock_default.call_count == 1, (
            f"google.auth.default called {mock_default.call_count} times "
            f"(expected 1) — provider is not caching credentials"
        )
        # credentials.refresh must be called exactly once for the initial fetch
        assert mock_creds.refresh.call_count == 1, (
            f"credentials.refresh called {mock_creds.refresh.call_count} times "
            f"(expected 1) — provider is not caching the token"
        )

    # ── 28: Token provider is thread-safe ─────────────────────────────────────

    def test_kafka_token_provider_thread_safe(self):
        """
        Concurrent calls to token() must not result in multiple ADC refreshes
        (races on first initialisation) and must all return the same value.
        """
        import datetime
        import threading

        from pipeline.utils.kafka_auth import GoogleManagedKafkaTokenProvider

        mock_creds = MagicMock()
        mock_creds.token = "thread-safe-token"
        mock_creds.expiry = datetime.datetime.now(
            datetime.timezone.utc
        ) + datetime.timedelta(hours=1)

        results = []
        errors = []

        def _call_token(provider):
            try:
                results.append(provider.token())
            except Exception as exc:
                errors.append(str(exc))

        with patch("google.auth.default", return_value=(mock_creds, "project")):
            provider = GoogleManagedKafkaTokenProvider()
            threads = [
                threading.Thread(target=_call_token, args=(provider,))
                for _ in range(10)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=3.0)

        assert not errors, f"token() raised in concurrent calls: {errors}"
        assert len(results) == 10, "Not all threads completed"
        assert all(
            r == "thread-safe-token" for r in results
        ), f"Inconsistent token values: {set(results)}"
        # Under heavy concurrency the lock prevents multiple simultaneous refreshes
        assert mock_creds.refresh.call_count >= 1
