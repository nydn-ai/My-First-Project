"""
Smoke + integration tests for the batch_prediction, monitoring, and training
log-type routes.

Each route is tested offline (no GCP / Kafka / Schema Registry calls) using
the real Avro schemas from disk, covering:
  - Mapper: real-world-style log entries → dict
  - avro_mapper: dict → Avro coercion against real schema
  - Confluent wire format: serialise → deserialise round-trip
  - DLQ routing: get_mapper / get_subject registry (unknown type → ValueError)
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock

import fastavro
import pytest

SCHEMAS_DIR = Path(__file__).parent.parent / "schemas"


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


def _load_schema(filename: str):
    from pipeline.utils.schema_registry import SchemaRegistryClient, _strip_non_avro_keys

    raw = json.loads((SCHEMAS_DIR / filename).read_text())
    clean = _strip_non_avro_keys(raw)
    parsed = fastavro.parse_schema(clean)
    nt: Dict[str, Any] = {}
    SchemaRegistryClient._collect_named_types(clean, nt)
    return parsed, nt


def _round_trip(log_type: str, schema_file: str, entry: dict) -> dict:
    """Map entry → avro_mapper → serialise → deserialise and return the record."""
    from pipeline.utils.avro_mapper import map_to_avro
    from pipeline.utils.log_type_mappers import get_mapper
    from pipeline.utils.schema_registry import (
        AvroConfluentSerializer,
        SchemaRegistryClient,
        _strip_non_avro_keys,
    )

    parsed, named_types = _load_schema(schema_file)

    mapped = get_mapper(log_type).map(entry)
    avro_record = map_to_avro(mapped, parsed, named_types=named_types)

    mock_client = MagicMock(spec=SchemaRegistryClient)
    mock_client.get_schema_by_id.return_value = {
        "schema": json.dumps(
            _strip_non_avro_keys(json.loads((SCHEMAS_DIR / schema_file).read_text()))
        )
    }
    ser = AvroConfluentSerializer(mock_client)
    raw_bytes = ser.serialize(avro_record, schema_id=1, parsed_schema=parsed)
    return ser.deserialize(raw_bytes)


# ──────────────────────────────────────────────────────────────────────────────
# Shared sample log entries
# ──────────────────────────────────────────────────────────────────────────────

_COMMON_RESOURCE = {
    "type": "aiplatform.googleapis.com/BatchPredictionJob",
    "labels": {
        "project_id": "my-project",
        "location": "us-east1",
    },
}

BATCH_AUDIT_ENTRY = {
    "insertId": "batch-001",
    "protoPayload": {
        "@type": "type.googleapis.com/google.cloud.audit.AuditLog",
        "methodName": "google.cloud.aiplatform.v1.JobService.CreateBatchPredictionJob",
        "resourceName": "projects/my-project/locations/us-east1/batchPredictionJobs/7890",
        "response": {
            "name": "projects/my-project/locations/us-east1/batchPredictionJobs/7890",
            "displayName": "daily-churn-batch-run",
            "state": "JOB_STATE_RUNNING",
            "inputConfig": {
                "instancesFormat": "jsonl",
                "gcsSource": {"uris": ["gs://my-bucket/input/data.jsonl"]},
            },
            "outputConfig": {
                "predictionsFormat": "jsonl",
                "gcsDestination": {"outputUriPrefix": "gs://my-bucket/output/"},
            },
            "createTime": "2026-05-20T10:00:00Z",
            "startTime": "2026-05-20T10:01:00Z",
        },
    },
    "resource": _COMMON_RESOURCE,
    "timestamp": "2026-05-20T10:01:05.123456789Z",
    "severity": "NOTICE",
    "logName": "projects/my-project/logs/cloudaudit.googleapis.com%2Factivity",
}

MONITORING_AUDIT_ENTRY = {
    "insertId": "mon-001",
    "protoPayload": {
        "@type": "type.googleapis.com/google.cloud.audit.AuditLog",
        "methodName": "google.cloud.aiplatform.v1.JobService.CreateModelDeploymentMonitoringJob",
        "resourceName": (
            "projects/my-project/locations/us-east1"
            "/modelDeploymentMonitoringJobs/4321"
        ),
        "response": {
            "name": (
                "projects/my-project/locations/us-east1"
                "/modelDeploymentMonitoringJobs/4321"
            ),
            "displayName": "wasp-staging-monitor",
            "state": "JOB_STATE_RUNNING",
        },
    },
    "resource": {
        "type": "aiplatform.googleapis.com/ModelDeploymentMonitoringJob",
        "labels": {"project_id": "my-project", "location": "us-east1"},
    },
    "timestamp": "2026-05-20T11:00:00.000000000Z",
    "severity": "NOTICE",
    "logName": "projects/my-project/logs/cloudaudit.googleapis.com%2Factivity",
}

MONITORING_VIOLATION_ENTRY = {
    "insertId": "mon-violation-001",
    "resource": {
        "type": "aiplatform.googleapis.com/ModelDeploymentMonitoringJob",
        "labels": {
            "project_id": "my-project",
            "location": "us-east1",
            "resource_container": "projects/817343037939",
        },
    },
    "labels": {
        "policy_id": "policy-abc-123",
        "policy_display_name": "Vertex endpoint wasp-live-staging - Active replicas zero",
        "activity_type_name": "ViolationOpenEventv1",
        "terse_message": (
            "Vertex AI endpoint is below the threshold of 1.000 with a value of 0.000."
        ),
        "started_at": "1747742400",  # Unix epoch seconds
    },
    "timestamp": "2026-05-20T12:00:00.000000000Z",
    "severity": "WARNING",
    "logName": "projects/my-project/logs/monitoring.googleapis.com%2FViolationOpenEventv1",
}

TRAINING_AUDIT_ENTRY = {
    "insertId": "train-001",
    "protoPayload": {
        "@type": "type.googleapis.com/google.cloud.audit.AuditLog",
        "methodName": "google.cloud.aiplatform.v1.JobService.CreateCustomJob",
        "resourceName": "projects/my-project/locations/us-east1/customJobs/5555",
        "response": {
            "name": "projects/my-project/locations/us-east1/customJobs/5555",
            "displayName": "wasp-retrain-v3",
            "state": "JOB_STATE_SUCCEEDED",
            "workerPoolSpecs": [
                {
                    "machineSpec": {
                        "machineType": "n1-highmem-8",
                        "acceleratorType": "NVIDIA_TESLA_T4",
                        "acceleratorCount": 2,
                    },
                    "containerSpec": {
                        "imageUri": "gcr.io/my-project/trainer:v3",
                        "args": ["--epochs=10", "--lr=0.001"],
                    },
                    "replicaCount": 1,
                }
            ],
            "modelToUpload": {"artifactUri": "gs://my-bucket/models/v3/"},
        },
    },
    "resource": {
        "type": "aiplatform.googleapis.com/CustomJob",
        "labels": {"project_id": "my-project", "location": "us-east1"},
    },
    "timestamp": "2026-05-20T14:00:00.000000000Z",
    "severity": "NOTICE",
    "logName": "projects/my-project/logs/cloudaudit.googleapis.com%2Factivity",
}


# ──────────────────────────────────────────────────────────────────────────────
# Batch prediction
# ──────────────────────────────────────────────────────────────────────────────


class TestBatchLogRoute:
    """batch_prediction log type: mapper + avro coercion + wire format."""

    def test_mapper_extracts_job_id(self):
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("batch_prediction").map(BATCH_AUDIT_ENTRY)
        assert result["payload"]["batch_job_id"] == "7890"

    def test_mapper_extracts_job_name(self):
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("batch_prediction").map(BATCH_AUDIT_ENTRY)
        assert result["payload"]["batch_job_name"] == "daily-churn-batch-run"

    def test_mapper_extracts_job_state(self):
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("batch_prediction").map(BATCH_AUDIT_ENTRY)
        assert result["payload"]["job_state"] == "JOB_STATE_RUNNING"

    def test_mapper_extracts_input_uri(self):
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("batch_prediction").map(BATCH_AUDIT_ENTRY)
        assert result["payload"]["input_uri"] == "gs://my-bucket/input/data.jsonl"

    def test_mapper_extracts_output_uri(self):
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("batch_prediction").map(BATCH_AUDIT_ENTRY)
        assert result["payload"]["output_uri"] == "gs://my-bucket/output/"

    def test_mapper_extracts_create_time(self):
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("batch_prediction").map(BATCH_AUDIT_ENTRY)
        assert isinstance(result["payload"]["create_time"], int)
        assert result["payload"]["create_time"] > 0

    def test_mapper_project_id(self):
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("batch_prediction").map(BATCH_AUDIT_ENTRY)
        assert result["project_id"] == "my-project"

    def test_round_trip(self):
        result = _round_trip("batch_prediction", "vertex_batch_log.avsc", BATCH_AUDIT_ENTRY)
        assert result["payload"]["batch_job_id"] == "7890"
        assert result["payload"]["batch_job_name"] == "daily-churn-batch-run"
        assert result["payload"]["job_state"] == "JOB_STATE_RUNNING"
        assert result["payload"]["input_uri"] == "gs://my-bucket/input/data.jsonl"
        assert result["severity"] == "NOTICE"

    def test_missing_fields_produce_none_not_error(self):
        """Sparse entry (only required fields) must map without raising."""
        from pipeline.utils.log_type_mappers import get_mapper

        sparse = {
            "insertId": "sparse-batch",
            "resource": {"type": "aiplatform.googleapis.com/BatchPredictionJob", "labels": {}},
            "timestamp": "2026-05-20T10:00:00Z",
            "severity": "INFO",
            "logName": "projects/p/logs/activity",
        }
        result = get_mapper("batch_prediction").map(sparse)
        assert result["payload"]["batch_job_id"] is None
        assert result["payload"]["job_state"] is None

    def test_get_subject_returns_batch_subject(self):
        from pipeline.utils.log_type_mappers import SUBJECT_BATCH, get_subject

        assert get_subject("batch_prediction") == SUBJECT_BATCH


# ──────────────────────────────────────────────────────────────────────────────
# Monitoring
# ──────────────────────────────────────────────────────────────────────────────


class TestMonitoringLogRoute:
    """monitoring log type: audit entries + violation events + wire format."""

    def test_mapper_extracts_job_id_from_resource_name(self):
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("monitoring").map(MONITORING_AUDIT_ENTRY)
        assert result["payload"]["monitoring_job_id"] == "4321"

    def test_mapper_extracts_job_name(self):
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("monitoring").map(MONITORING_AUDIT_ENTRY)
        assert result["payload"]["monitoring_job_name"] == "wasp-staging-monitor"

    def test_violation_event_policy_id(self):
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("monitoring").map(MONITORING_VIOLATION_ENTRY)
        assert result["payload"]["monitoring_job_id"] == "policy-abc-123"

    def test_violation_event_monitor_type(self):
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("monitoring").map(MONITORING_VIOLATION_ENTRY)
        assert result["payload"]["monitor_type"] == "ViolationOpenEventv1"

    def test_violation_event_alert_triggered(self):
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("monitoring").map(MONITORING_VIOLATION_ENTRY)
        assert result["payload"]["alert_triggered"] is True

    def test_violation_event_threshold_parsed_from_terse_message(self):
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("monitoring").map(MONITORING_VIOLATION_ENTRY)
        assert result["payload"]["threshold_value"] == pytest.approx(1.0)

    def test_violation_event_metric_value_parsed_from_terse_message(self):
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("monitoring").map(MONITORING_VIOLATION_ENTRY)
        assert result["payload"]["metric_value"] == pytest.approx(0.0)

    def test_violation_event_window_start_from_started_at(self):
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("monitoring").map(MONITORING_VIOLATION_ENTRY)
        # started_at = "1747742400" (unix epoch s) → ms
        assert result["payload"]["window_start"] == 1747742400 * 1000

    def test_violation_event_metric_name_from_policy_name(self):
        """Policy name containing 'replica' → metric_name = 'replica_count'."""
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("monitoring").map(MONITORING_VIOLATION_ENTRY)
        assert result["payload"]["metric_name"] == "replica_count"

    def test_round_trip_audit(self):
        result = _round_trip(
            "monitoring", "vertex_monitoring_log.avsc", MONITORING_AUDIT_ENTRY
        )
        assert result["payload"]["monitoring_job_id"] == "4321"
        assert result["severity"] == "NOTICE"

    def test_round_trip_violation(self):
        result = _round_trip(
            "monitoring", "vertex_monitoring_log.avsc", MONITORING_VIOLATION_ENTRY
        )
        assert result["payload"]["alert_triggered"] is True
        assert result["payload"]["threshold_value"] == pytest.approx(1.0)

    def test_get_subject_returns_monitoring_subject(self):
        from pipeline.utils.log_type_mappers import SUBJECT_MONITORING, get_subject

        assert get_subject("monitoring") == SUBJECT_MONITORING


# ──────────────────────────────────────────────────────────────────────────────
# Training
# ──────────────────────────────────────────────────────────────────────────────


class TestTrainingLogRoute:
    """training log type: CustomJob audit entries + wire format."""

    def test_mapper_extracts_job_id(self):
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("training").map(TRAINING_AUDIT_ENTRY)
        assert result["payload"]["training_job_id"] == "5555"

    def test_mapper_extracts_job_name(self):
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("training").map(TRAINING_AUDIT_ENTRY)
        assert result["payload"]["training_job_name"] == "wasp-retrain-v3"

    def test_mapper_extracts_job_state(self):
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("training").map(TRAINING_AUDIT_ENTRY)
        assert result["payload"]["job_state"] == "JOB_STATE_SUCCEEDED"

    def test_mapper_extracts_machine_type(self):
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("training").map(TRAINING_AUDIT_ENTRY)
        assert result["payload"]["machine_type"] == "n1-highmem-8"

    def test_mapper_extracts_accelerator_type(self):
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("training").map(TRAINING_AUDIT_ENTRY)
        assert result["payload"]["accelerator_type"] == "NVIDIA_TESLA_T4"

    def test_mapper_extracts_accelerator_count(self):
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("training").map(TRAINING_AUDIT_ENTRY)
        assert result["payload"]["accelerator_count"] == 2

    def test_mapper_extracts_container_uri(self):
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("training").map(TRAINING_AUDIT_ENTRY)
        assert result["payload"]["container_uri"] == "gcr.io/my-project/trainer:v3"

    def test_mapper_extracts_args(self):
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("training").map(TRAINING_AUDIT_ENTRY)
        assert result["payload"]["args"] == ["--epochs=10", "--lr=0.001"]

    def test_mapper_extracts_artifact_uri(self):
        from pipeline.utils.log_type_mappers import get_mapper

        result = get_mapper("training").map(TRAINING_AUDIT_ENTRY)
        assert result["payload"]["artifact_uris"] == ["gs://my-bucket/models/v3/"]

    def test_mapper_missing_fields_produce_none(self):
        """Sparse entry must map without raising."""
        from pipeline.utils.log_type_mappers import get_mapper

        sparse = {
            "insertId": "sparse-train",
            "resource": {
                "type": "aiplatform.googleapis.com/CustomJob",
                "labels": {"project_id": "p", "location": "us-east1"},
            },
            "timestamp": "2026-05-20T14:00:00Z",
            "severity": "INFO",
            "logName": "projects/p/logs/activity",
        }
        result = get_mapper("training").map(sparse)
        assert result["payload"]["training_job_id"] is None
        assert result["payload"]["machine_type"] is None

    def test_round_trip(self):
        result = _round_trip(
            "training", "vertex_training_log.avsc", TRAINING_AUDIT_ENTRY
        )
        assert result["payload"]["training_job_id"] == "5555"
        assert result["payload"]["machine_type"] == "n1-highmem-8"
        assert result["payload"]["accelerator_count"] == 2
        assert result["payload"]["job_state"] == "JOB_STATE_SUCCEEDED"

    def test_get_subject_returns_training_subject(self):
        from pipeline.utils.log_type_mappers import SUBJECT_TRAINING, get_subject

        assert get_subject("training") == SUBJECT_TRAINING


# ──────────────────────────────────────────────────────────────────────────────
# Registry
# ──────────────────────────────────────────────────────────────────────────────


class TestMapperRegistry:
    """get_mapper and get_subject must raise clearly on unknown log types."""

    def test_unknown_log_type_raises_valueerror_mapper(self):
        from pipeline.utils.log_type_mappers import get_mapper

        with pytest.raises(ValueError, match="Unknown log_type"):
            get_mapper("not_a_type")

    def test_unknown_log_type_raises_valueerror_subject(self):
        from pipeline.utils.log_type_mappers import get_subject

        with pytest.raises(ValueError, match="Unknown log_type"):
            get_subject("not_a_type")

    def test_all_four_types_have_mappers(self):
        from pipeline.utils.log_type_mappers import get_mapper

        for log_type in ("online_prediction", "batch_prediction", "monitoring", "training"):
            assert get_mapper(log_type) is not None

    def test_all_four_types_have_subjects(self):
        from pipeline.utils.log_type_mappers import get_subject

        for log_type in ("online_prediction", "batch_prediction", "monitoring", "training"):
            assert get_subject(log_type) is not None
