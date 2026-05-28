# ──────────────────────────────────────────────────────────────────────────────
# Dev environment variable overrides
# These match the real dev resources — edit as needed.
# ──────────────────────────────────────────────────────────────────────────────

project_id   = "project-1c03ae00-17f3-43f4-86a"
region       = "us-east1"
ar_repo_name = "vertex-ml-logs"
image_name   = "pubsub-to-kafka-avro"

template_bucket = "project-1c03ae00-17f3-43f4-86a-df-templates-dev"

kafka_bootstrap_servers = "bootstrap.dev-vertex-log-kafka.us-east1.managedkafka.project-1c03ae00-17f3-43f4-86a.cloud.goog:9092"
kafka_registry_url      = "https://managedkafka.googleapis.com/v1/projects/project-1c03ae00-17f3-43f4-86a/locations/us-east1/schemaRegistries/dev_schema_registry"

# ── Schema / topic routing ────────────────────────────────────────────────────
#
#   log_type             schema                subject                                             kafka_topic
#   online_prediction →  VertexPredictionLog → com.bumble.avro.ml.vertex.VertexPredictionLog  →  dev-enriched-vertex-logs
#   batch_prediction  →  VertexBatchLog      → com.bumble.avro.ml.vertex.VertexBatchLog       →  dev-enriched-vertex-batch-logs
#   monitoring        →  VertexMonitoringLog → com.bumble.avro.ml.vertex.VertexMonitoringLog  →  dev-enriched-vertex-monitoring-logs
#   training          →  VertexTrainingLog   → com.bumble.avro.ml.vertex.VertexTrainingLog    →  dev-enriched-vertex-training-logs
#
# Deploy one Dataflow job per log_type.  Change both variables and re-apply.
# ─────────────────────────────────────────────────────────────────────────────

# Active job — default: online prediction
log_type    = "online_prediction"
kafka_topic = "dev-enriched-vertex-logs"

# Batch job:      log_type = "batch_prediction"   kafka_topic = "dev-enriched-vertex-batch-logs"
# Monitoring job: log_type = "monitoring"          kafka_topic = "dev-enriched-vertex-monitoring-logs"
# Training job:   log_type = "training"            kafka_topic = "dev-enriched-vertex-training-logs"

pubsub_subscription   = "projects/project-1c03ae00-17f3-43f4-86a/subscriptions/vertex-ml-logs-df-sub"
dataflow_max_workers  = 2
dataflow_machine_type = "n1-standard-2"

# Set to true AFTER running: make build-and-deploy
enable_dataflow_job = false
