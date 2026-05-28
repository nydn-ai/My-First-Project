# pipeline/

Apache Beam pipeline package that implements the Vertex ML Logs Dataflow Flex
Template.  All code here runs **inside the Dataflow worker containers**.

---

## Package Structure

```
pipeline/
├── main.py                   Pipeline entry point + all DoFns
├── setup.py                  Package metadata (required by Dataflow SDK)
├── Dockerfile                Flex Template container image definition
├── metadata.json             Flex Template spec metadata (parameters)
├── transforms/
│   └── kafka_sink.py         WriteToKafkaAvro composite transform
└── utils/
    ├── avro_mapper.py        Python dict → Avro record type coercion
    ├── kafka_auth.py         OAUTHBEARER token helper (standalone util)
    ├── log_type_mappers.py   Cloud Logging entry → typed Avro dict mappers
    ├── schema_registry.py    Confluent Schema Registry client + serializer
    └── time_utils.py         Timestamp parsing helpers
```

---

## Data Flow

```
ReadFromPubSub
    │  (raw bytes, with_attributes=True)
    ▼
ParseJsonMessage  (DoFn)
    │  Main:       parsed dict + _publish_time_ms
    │  dead_letter: {raw, error, stage="parse"}
    ▼
WindowInto(FixedWindows(window_size_seconds))
    ▼
BuildAvroRecord  (DoFn)
    │  Main:       (kafka_key: bytes, avro_bytes: bytes)
    │  dead_letter: {original fields, error, stage="build_avro"}
    ▼
WriteToKafkaAvro  (PTransform → KafkaAvroWriteFn DoFn)
    │  "success":    (kafka_key, avro_bytes)  — written to Kafka
    │  "dead_letter": {key, value_size, error, stage="kafka_write"}
    ▼
MergeDlq (Flatten all three DLQ streams)
    ▼
WriteToDlq  (DoFn → GCS JSON)
    gs://PROJECT-df-dlq-dev/dlq/LOG_TYPE/YYYY/MM/DD/HH/<uuid>.json
```

---

## Module Reference

### `main.py`

Entry point and core DoFns.

**`ParseJsonMessage`**

Decodes raw PubSub bytes to a Python `dict`.  Injects `_publish_time_ms`
(epoch ms from the PubSub message `publish_time` attribute).  Sends
unparseable messages to the `dead_letter` output.

**`BuildAvroRecord`**

Orchestrates per-record processing:
1. Resolves the Avro schema from the Schema Registry (cached).
2. Calls the log-type mapper to convert the Cloud Logging dict to a typed
   nested dict.
3. Calls `avro_mapper.map_to_avro()` to coerce types against the schema.
4. Serializes to Confluent wire format.
5. Derives the Kafka partition key (`endpoint_id` → bytes).

Any exception routes the message to `dead_letter` with the original
fields preserved for replay.

**`WriteToDlq`**

Writes dead-letter dicts to GCS as newline-delimited JSON.  The GCS client
is initialised once per worker in `setup()`.  Failures re-raise so Beam
can retry the bundle.

### `transforms/kafka_sink.py`

**`KafkaAvroWriteFn`** (DoFn)

Initialises a `confluent_kafka.Producer` in `setup()` (once per worker).
Each `process()` call produces one message, calls `flush(timeout=60)`, and
checks the delivery report callback.  Success → `"success"` tagged output;
any error → `"dead_letter"` tagged output.

**Authentication — GOOG_OAUTH2_TOKEN**

Google Managed Kafka requires a 3-part base64url token for OAUTHBEARER:

```
base64url({"typ":"JWT","alg":"GOOG_OAUTH2_TOKEN"})
  + "."
  + base64url({"exp":…,"iss":"Google","iat":…,"scope":"kafka","sub":<sa>})
  + "."
  + base64url(<raw ADC access token>)
```

This is built in `_make_oauth_cb()` and passed to confluent-kafka as the
`oauth_cb` config key.

**`WriteToKafkaAvro`** (PTransform)

Composite transform wrapping `KafkaAvroWriteFn` with `.with_outputs()` so
callers receive named `"success"` and `"dead_letter"` outputs.

### `utils/log_type_mappers.py`

Contains one mapper class per log type.  All mappers produce a flat dict
conforming to their respective Avro schema, populated from a Cloud Logging
entry dict.

| Mapper | Schema | Subject |
|--------|--------|---------|
| `VertexPredictionLogMapper` | `VertexPredictionLog` | `com.bumble.avro.ml.vertex.VertexPredictionLog` |
| `VertexBatchLogMapper` | `VertexBatchLog` | `com.bumble.avro.ml.vertex.VertexBatchLog` |
| `VertexMonitoringLogMapper` | `VertexMonitoringLog` | `com.bumble.avro.ml.vertex.VertexMonitoringLog` |
| `VertexTrainingLogMapper` | `VertexTrainingLog` | `com.bumble.avro.ml.vertex.VertexTrainingLog` |

`_build_top_level()` extracts common fields shared by all schemas
(`id`, `ts`, `endpoint_id`, `project_id`, `severity`, etc.).  Each mapper's
`_build_*_payload()` helper extracts type-specific nested fields.

**Supported log formats per mapper:**

| Mapper | `protoPayload` (audit log) | `jsonPayload` (container log) |
|--------|---------------------------|-------------------------------|
| Prediction | ✓ (PredictionService.Predict) | ✓ (latency_ms, status_code) |
| Batch | ✓ (BatchPredictionJob lifecycle) | ✓ (job_state, counts) |
| Monitoring | ✓ (MonitoringJob lifecycle) | ✓ (drift/skew alerts, violation events via `labels`) |
| Training | ✓ (CustomJob / TrainingPipeline) | ✓ (metrics list/dict) |

### `utils/schema_registry.py`

**`SchemaRegistryClient`**

Thread-safe client for a Confluent-compatible Schema Registry.

- Auto-detects local vs GCP URLs and skips auth for `localhost` / `127.0.0.1`.
- Caches schemas per subject with a configurable TTL (default 300 s).
- Uses two separate `threading.Lock` instances to avoid deadlock:
  `_schema_lock` for the schema cache and `_token_lock` for ADC token refresh.
- Resolves nested schema references depth-first.

**`AvroConfluentSerializer`**

Produces Confluent wire-format bytes: `\x00` + 4-byte big-endian schema ID +
fastavro binary payload.

### `utils/avro_mapper.py`

`map_to_avro(data, schema, named_types)` recursively coerces a Python dict to
match a fastavro-parsed Avro schema.  Handles:

- Primitives: string, int, long, float, double, boolean, bytes
- Logical types: `uuid` (validates/generates), `timestamp-millis` (ISO strings
  → epoch ms), `timestamp-micros`
- Complex: record, array, map, enum
- Unions: tries each non-null branch in order
- Missing nullable fields → `None`; missing required fields → `AvroMappingError`

Non-standard extension keys (`x-meta`, etc.) are stripped by
`schema_registry._strip_non_avro_keys()` before fastavro sees the schema.

### `utils/time_utils.py`

`pubsub_time_to_millis(publish_time)` — converts a PubSub `Timestamp` protobuf
or ISO 8601 string (including nanosecond precision) to epoch milliseconds.
Kept in a sub-module (not `main.py`) so Beam/dill can serialise references by
stable path.

---

## Pipeline Parameters

Set as Dataflow template parameters (see `metadata.json`):

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `input_subscription` | ✓ | — | Full Pub/Sub subscription path |
| `kafka_bootstrap_servers` | ✓ | — | Kafka bootstrap server(s) |
| `kafka_topic` | ✓ | — | Destination Kafka topic |
| `kafka_registry_url` | ✓ | — | Schema Registry base URL |
| `log_type` | ✓ | — | One of: `online_prediction`, `batch_prediction`, `monitoring`, `training` |
| `dlq_bucket` | ✓ | — | GCS bucket name for dead-letter messages |
| `window_size_seconds` | — | `10` | Fixed window size in seconds |
| `schema_cache_ttl_seconds` | — | `300` | Schema cache TTL per worker |

---

## Local Development

```bash
# Run pipeline locally (DirectRunner) against a test subscription
python pipeline/main.py \
  --runner=DirectRunner \
  --input_subscription=projects/PROJECT/subscriptions/debug-sub \
  --kafka_bootstrap_servers=localhost:9092 \
  --kafka_topic=local-test \
  --kafka_registry_url=http://localhost:8081 \
  --log_type=online_prediction \
  --dlq_bucket=local-dlq
```

---

## Docker Image

The image is built from `pipeline/Dockerfile` using the Dataflow Python
template base image:

```dockerfile
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base
```

The import health-check at the end of the Dockerfile verifies that the package
and its most complex dependency (`WriteToKafkaAvro`) are importable before the
image is pushed:

```dockerfile
RUN python -c "import pipeline.main; from pipeline.transforms.kafka_sink import WriteToKafkaAvro; print('Import health check OK')"
```

> **Note:** Dockerfile heredoc syntax (`RUN <<'HEREDOC'`) requires BuildKit,
> which is not enabled by default in Cloud Build.  Always use `python -c "..."`
> for inline health checks.
