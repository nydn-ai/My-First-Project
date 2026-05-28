# tests/

Unit and integration tests for the Vertex ML Logs pipeline.  All tests run
with `pytest` and do **not** require a live GCP project, Kafka cluster, or
Schema Registry — everything is either mocked or tested with pure Python.

---

## Test Files

| File | What it tests |
|------|---------------|
| `conftest.py` | Shared pytest configuration (sys.path setup) |
| `test_prediction_log_route.py` | `VertexPredictionLogMapper` — online prediction log entries |
| `test_other_log_routes.py` | `VertexBatchLogMapper`, `VertexMonitoringLogMapper`, `VertexTrainingLogMapper`, `SchemaRegistryClient`, `_strip_non_avro_keys` |

---

## Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run with short traceback (used in Cloud Build)
pytest tests/ -v --tb=short

# Run a single test file
pytest tests/test_prediction_log_route.py -v

# Run a single test by name
pytest tests/ -k "test_online_prediction_audit_log" -v

# Run with coverage
pytest tests/ --cov=pipeline --cov-report=term-missing
```

Or use the Makefile target:
```bash
make test
```

---

## Test Coverage

### `test_prediction_log_route.py`

Tests for `VertexPredictionLogMapper.map()` covering the main entry formats
and edge cases:

| Test | Scenario |
|------|----------|
| `test_online_prediction_audit_log` | Full audit log with `protoPayload` (method, latency, deployed model) |
| `test_online_prediction_container_log` | Container log with `jsonPayload` (latency_ms, deployed_model_id from top-level labels) |
| `test_missing_optional_fields` | Entry with minimal fields — verifies all nullable fields become `None` |
| `test_project_id_extraction_from_resource_container` | `resource_container = "projects/817343037939"` → `project_id = "817343037939"` |
| `test_endpoint_id_from_resource_labels` | Endpoint ID extracted from `resource.labels.endpoint_id` |
| `test_endpoint_id_from_resource_name` | Endpoint ID extracted from `protoPayload.resourceName` path |
| `test_nanosecond_timestamp_parsing` | `"2026-05-28T08:32:13.442850351Z"` → correct epoch ms |
| `test_severity_preserved` | `"ERROR"` severity passes through unchanged |

### `test_other_log_routes.py`

#### Batch (`VertexBatchLogMapper`)
| Test | Scenario |
|------|----------|
| `test_batch_prediction_audit_log` | Full `protoPayload` with job lifecycle, input/output config |
| `test_batch_job_id_from_resource_name` | Job ID extracted from `protoPayload.resourceName` |
| `test_batch_partial_failures` | `partialFailures` list → `failure_count` |
| `test_batch_minimal_entry` | Minimal entry — all payload fields `None` |

#### Monitoring (`VertexMonitoringLogMapper`)
| Test | Scenario |
|------|----------|
| `test_monitoring_audit_log` | Monitoring job lifecycle `protoPayload` |
| `test_monitoring_violation_event` | Cloud Monitoring alert via top-level `labels` (no `protoPayload`) |
| `test_monitoring_terse_message_parsing` | Metric value and threshold extracted from `terse_message` |
| `test_monitoring_alert_triggered_true` | `ViolationOpenEventv1` sets `alert_triggered = True` |

#### Training (`VertexTrainingLogMapper`)
| Test | Scenario |
|------|----------|
| `test_training_custom_job_audit_log` | `CustomJob` `protoPayload` with worker pool spec |
| `test_training_job_id_from_resource_name` | Job ID from `/customJobs/` path segment |
| `test_training_metrics_list` | `jsonPayload.metrics` as a list → `TrainingMetric` records |
| `test_training_metrics_dict` | `jsonPayload.metrics` as a flat dict → converted list |
| `test_training_hyperparameters` | Hyperparameter dict → `map<string,string>` |

#### Schema Registry (`SchemaRegistryClient`, `_strip_non_avro_keys`)
| Test | Scenario |
|------|----------|
| `test_strip_non_avro_keys_removes_x_meta` | `x-meta` keys removed recursively |
| `test_strip_non_avro_keys_preserves_standard` | Standard Avro keys (`type`, `fields`, etc.) untouched |
| `test_schema_registry_caches_result` | Second `get_latest_schema` call uses cache (no HTTP) |
| `test_schema_registry_local_no_auth` | `localhost` URL → no `Authorization` header sent |

---

## Fixtures and Mocking

`conftest.py` adds the project root to `sys.path` so that `pipeline.*` imports
resolve correctly regardless of where pytest is invoked from.

Tests use `unittest.mock` and `pytest-mock` (if available) to isolate:
- HTTP calls to the Schema Registry (`requests.Session.get` / `.post`)
- ADC token refresh (`google.auth.default`, `creds.refresh`)
- Kafka producer (`confluent_kafka.Producer`)
- GCS writes (`google.cloud.storage.Client`)

No live credentials or network access are required.

---

## Test Data Patterns

Cloud Logging entries used in tests mirror real production log formats:

**Audit log** (from `protoPayload`):
```python
{
    "protoPayload": {
        "methodName": "google.cloud.aiplatform.v1.PredictionService.Predict",
        "resourceName": "projects/P/locations/L/endpoints/E",
        "latencyMs": "42",
        "response": {"deployedModelId": "model-123"},
        "status": {}
    },
    "resource": {"type": "aiplatform.googleapis.com/Endpoint", "labels": {…}},
    "timestamp": "2026-05-28T08:32:13.442850351Z",
    "severity": "INFO",
    ...
}
```

**Container log** (from `jsonPayload`):
```python
{
    "jsonPayload": {
        "message": "prediction served",
        "latency_ms": 38,
        "status_code": 200
    },
    "labels": {
        "deployed_model_id": "1547128309001748480",
        "replica_id": "predictor-…"
    },
    "resource": {"type": "aiplatform.googleapis.com/Endpoint", "labels": {…}},
    ...
}
```

---

## Adding New Tests

1. Add a new test function to the appropriate file, or create a new file for a
   new component.
2. Follow the naming convention `test_<what>_<scenario>`.
3. Keep each test focused on a single behaviour.
4. Mock all external calls — tests must pass offline.
5. Run `make lint && make test` before committing.

---

## CI Integration

Tests run in Cloud Build as the `test` step, after all lint steps pass:

```yaml
- id: test
  name: python:3.11-slim
  entrypoint: python
  args: ["-m", "pytest", "tests/", "-v", "--tb=short"]
  waitFor: [lint-black, lint-isort, lint-flake8]
```

A test failure blocks the Docker build and deploy steps.
