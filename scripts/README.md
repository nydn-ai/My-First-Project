# scripts/

Operational scripts for schema management, end-to-end testing, and
pipeline verification.  All scripts use Google ADC for authentication and
are safe to run locally or inside Cloud Build.

---

## Scripts Overview

| Script | Purpose |
|--------|---------|
| `register_schemas.py` | Register / update Avro schemas in the Schema Registry |
| `test_kafka_consumer.py` | Consume and display messages from a Kafka topic |
| `generate_test_log.py` | Publish synthetic Vertex AI log entries to Pub/Sub |

---

## `register_schemas.py`

Registers all four Avro schemas in a Confluent-compatible Schema Registry.
Called automatically in Cloud Build step `register-schemas`; can also be run
manually.

### Modes

| Mode | Flag | Behaviour |
|------|------|-----------|
| Default | _(none)_ | Register / update all schemas (idempotent) |
| Provision-if-missing | `--missing-only` | Skip subjects that already exist |
| Check-only | `--check` | Verify all subjects exist; exit 1 if any missing |
| Dry-run | `--dry-run` | Print what would be registered, make no changes |

### Usage

```bash
# Register all schemas against GCP (uses ADC automatically)
python scripts/register_schemas.py \
  --registry_url "https://managedkafka.googleapis.com/v1/projects/PROJECT/locations/REGION/schemaRegistries/REGISTRY"

# Local Docker Compose Schema Registry (no auth required)
python scripts/register_schemas.py --registry_url http://localhost:8081

# Verify all subjects exist before starting a Dataflow job
python scripts/register_schemas.py \
  --registry_url "https://..." \
  --check

# Register only new schemas (safe for shared environments)
python scripts/register_schemas.py \
  --registry_url "https://..." \
  --missing-only

# Preview what would change without touching the registry
python scripts/register_schemas.py \
  --registry_url "https://..." \
  --dry-run
```

### Authentication

- **GCP registry:** Automatically uses Application Default Credentials (ADC).
  The caller must have `roles/managedkafka.schemaRegistryEditor` on the project.
- **Local registry (`localhost` / `127.0.0.1`):** No authentication headers are
  sent.

### Schema Plan

Schemas are registered in this order (order matters when schemas have
cross-references; all four are currently self-contained):

1. `com.bumble.avro.ml.vertex.VertexPredictionLog` ← `schemas/vertex_prediction_log.avsc`
2. `com.bumble.avro.ml.vertex.VertexBatchLog` ← `schemas/vertex_batch_log.avsc`
3. `com.bumble.avro.ml.vertex.VertexMonitoringLog` ← `schemas/vertex_monitoring_log.avsc`
4. `com.bumble.avro.ml.vertex.VertexTrainingLog` ← `schemas/vertex_training_log.avsc`

---

## `test_kafka_consumer.py`

Connects to a Kafka topic using the same GOOG_OAUTH2_TOKEN OAUTHBEARER
authentication as the Dataflow pipeline, consumes messages, deserializes
the Confluent Avro wire format, and prints records in a human-readable table.

Use this to **verify end-to-end pipeline output** after publishing test
messages with `make generate-test-log`.

### Usage

```bash
# Consume latest messages (waits 30 s)
python scripts/test_kafka_consumer.py \
  --bootstrap_servers "bootstrap.dev-vertex-log-kafka.us-east1.managedkafka.PROJECT.cloud.goog:9092" \
  --topic dev-enriched-vertex-logs \
  --registry_url "https://managedkafka.googleapis.com/v1/projects/PROJECT/locations/REGION/schemaRegistries/REGISTRY"

# Replay all historical messages from earliest offset
python scripts/test_kafka_consumer.py ... --from_beginning

# Read up to 50 messages with a 60-second timeout
python scripts/test_kafka_consumer.py ... --max_messages 50 --timeout 60
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--bootstrap_servers` | dev cluster | Kafka bootstrap server address |
| `--topic` | `dev-enriched-vertex-logs` | Topic to consume from |
| `--registry_url` | dev registry | Schema Registry base URL |
| `--timeout` | `30` | Seconds to wait for new messages |
| `--max_messages` | `20` | Maximum messages to consume then exit |
| `--from_beginning` | `false` | Read from earliest offset (replay mode) |
| `--group_id` | `vertex-ml-logs-consumer-dev` | Kafka consumer group ID |

### Example Output

```
  ────────────────────────────────────────────────────────────
  Partition: 0  Offset: 42
  Key:       spiderweb-lite-staging

    id                  : f4a7b2c1-3d8e-4f9a-b2c1-d3e4f5a6b7c8
    ts                  : 2026-05-28T08:32:13.442Z (1779957133442)
    endpoint_id         : spiderweb-lite-staging
    severity            : ERROR
    project_id          : 817343037939
    location            : europe-west4
    insert_id           : 5up50ng148ev9a
    payload.deployed_model_id: 1547128309001748480
    raw_payload_json    : [len=118 chars]
```

### Authentication

Requires `roles/managedkafka.client` on the project for the ADC identity.

---

## `generate_test_log.py`

Publishes synthetic Vertex AI log entries to a Pub/Sub topic to trigger the
Dataflow pipeline end-to-end.  Supports all four log types.

### Usage

```bash
# Publish one online prediction log
python scripts/generate_test_log.py \
  --project PROJECT_ID \
  --topic vertex-ml-logs-dev \
  --log_type online_prediction

# Publish 5 batch prediction logs
python scripts/generate_test_log.py \
  --project PROJECT_ID \
  --topic vertex-ml-logs-dev \
  --log_type batch_prediction \
  --count 5

# Publish all log types in one call
python scripts/generate_test_log.py \
  --project PROJECT_ID \
  --topic vertex-ml-logs-dev \
  --all_types
```

Alternatively, use the Makefile shortcut:

```bash
make generate-test-log
```

---

## Makefile Targets

All scripts are also accessible via Makefile targets for convenience:

```bash
make register-schemas     # register_schemas.py against dev registry
make test-consumer        # test_kafka_consumer.py (latest 20 messages)
make generate-test-log    # generate_test_log.py (online_prediction, 1 message)
```

---

## IAM Requirements

| Script | Required role | Scope |
|--------|---------------|-------|
| `register_schemas.py` (GCP) | `roles/managedkafka.schemaRegistryEditor` | project |
| `test_kafka_consumer.py` | `roles/managedkafka.client` | project |
| `generate_test_log.py` | `roles/pubsub.publisher` | topic |
