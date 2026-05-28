# Vertex ML Logs — PubSub → Kafka Avro Pipeline

A **Google Cloud Dataflow Flex Template** that reads Vertex AI log entries from
a Pub/Sub subscription, maps them to typed Avro records, and publishes them to
a Google Managed Kafka topic using the Confluent wire format.

```
Cloud Logging ──► Pub/Sub ──► Dataflow (Flex Template) ──► Managed Kafka
                                   │
                                   └──► GCS Dead-Letter Queue (on error)
```

---

## Repository Layout

```
├── pipeline/           Apache Beam pipeline source (DoFns, transforms, utils)
├── schemas/            Avro schema definitions (.avsc)
├── scripts/            Operational scripts (schema registration, test consumer)
├── tests/              Unit and integration tests (pytest)
├── terraform/
│   └── dev/            Terraform for dev GCP infrastructure
├── docs/               Guides (TROUBLESHOOTING.md)
├── cloudbuild.yaml     Cloud Build — master push / tag deploy
├── cloudbuild-pr.yaml  Cloud Build — PR validation (lint + test only)
├── Makefile            Developer workflow commands
└── requirements.txt    Runtime Python dependencies
```

---

## Quick Start

### Prerequisites

| Tool | Version |
|------|---------|
| Python | ≥ 3.11 |
| Terraform | ≥ 1.7 |
| gcloud CLI | latest |
| Docker | any recent |

```bash
# Authenticate with GCP
gcloud auth application-default login

# Install Python dependencies
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
```

### Provision Infrastructure

```bash
cd terraform/dev
terraform init
terraform plan
terraform apply
```

### Build and Deploy

```bash
# 1. Build the Docker image and push to Artifact Registry
make docker-push

# 2. Register Avro schemas in the Managed Kafka Schema Registry
make register-schemas

# 3. Stage the Flex Template spec to GCS
make stage-template

# 4. Run the Dataflow job (online_prediction by default)
make run-dataflow-job LOG_TYPE=online_prediction
```

### Run Tests

```bash
make lint   # black + isort + flake8
make test   # pytest
```

---

## Log Type Routing

One Dataflow job per log type; each job maps to a different Avro schema and
Kafka topic.

| `log_type` | Avro Schema | Kafka Topic |
|---|---|---|
| `online_prediction` | `VertexPredictionLog` | `dev-enriched-vertex-logs` |
| `batch_prediction` | `VertexBatchLog` | `dev-enriched-vertex-batch-logs` |
| `monitoring` | `VertexMonitoringLog` | `dev-enriched-vertex-monitoring-logs` |
| `training` | `VertexTrainingLog` | `dev-enriched-vertex-training-logs` |

---

## Cloud Build Triggers

| Trigger | Event | YAML |
|---|---|---|
| `pr-validation` | Pull request opened/updated | `cloudbuild-pr.yaml` |
| `push-to-master` | Push to `master` branch | `cloudbuild.yaml` |
| `release-tag` | Push of `v*.*.*` tag | `cloudbuild.yaml` |

Build steps: install deps → lint → test → docker build → push → register
schemas → stage Flex Template → write latest-tag reference.

---

## Key Design Decisions

- **Confluent wire format**: Every Kafka message starts with `\x00` + 4-byte
  big-endian schema ID so consumers can look up the schema dynamically.
- **GOOG_OAUTH2_TOKEN**: Google Managed Kafka requires a specific 3-part
  base64url token for OAUTHBEARER SASL — not a plain Bearer token. See
  `pipeline/transforms/kafka_sink.py`.
- **Partition key = `endpoint_id`**: All events for the same Vertex AI endpoint
  land on the same Kafka partition, preserving order within an endpoint.
- **Dead-letter queue**: Failures at parse, Avro mapping, or Kafka write are
  written as JSON to `gs://PROJECT-df-dlq-dev/dlq/LOG_TYPE/YYYY/MM/DD/HH/`.
- **Schema caching**: `SchemaRegistryClient` caches schemas per subject for
  `schema_cache_ttl_seconds` (default 300 s) to avoid a registry round-trip on
  every message.

---

## Further Reading

- [`pipeline/README.md`](pipeline/README.md) — Beam pipeline internals
- [`schemas/README.md`](schemas/README.md) — Avro schema catalogue
- [`scripts/README.md`](scripts/README.md) — Operational scripts
- [`terraform/README.md`](terraform/README.md) — Infrastructure management
- [`tests/README.md`](tests/README.md) — Test suite guide
- [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md) — Known issues and fixes
