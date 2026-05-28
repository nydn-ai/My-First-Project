locals {
  name_prefix    = "vertex-ml-logs"
  worker_sa_name = "sa-df-vertex-logs-dev"

  # Kafka cluster project — same as pipeline project for dev
  kafka_project_id = var.project_id

  common_labels = {
    team        = "trust"
    environment = "dev"
    managed_by  = "terraform"
    component   = "vertex-ml-logs"
  }
}

# ──────────────────────────────────────────────────────────────────────────────
# Artifact Registry
# ──────────────────────────────────────────────────────────────────────────────
resource "google_artifact_registry_repository" "vertex_ml_logs" {
  project       = var.project_id
  location      = var.region
  repository_id = var.ar_repo_name
  format        = "DOCKER"
  description   = "Vertex ML Logs Dataflow Flex Template images"

  labels = local.common_labels
}

# ──────────────────────────────────────────────────────────────────────────────
# GCS Buckets
# ──────────────────────────────────────────────────────────────────────────────

# Flex Template spec storage
resource "google_storage_bucket" "df_templates" {
  project                     = var.project_id
  name                        = var.template_bucket
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      num_newer_versions = 10
    }
    action {
      type = "Delete"
    }
  }

  labels = local.common_labels
}

# Dataflow staging (SDK components, worker files)
resource "google_storage_bucket" "df_staging" {
  project                     = var.project_id
  name                        = "${var.project_id}-df-staging-dev"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false

  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }

  labels = local.common_labels
}

# Dataflow temp (shuffle data, spill)
resource "google_storage_bucket" "df_temp" {
  project                     = var.project_id
  name                        = "${var.project_id}-df-temp-dev"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false

  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }
  }

  labels = local.common_labels
}

# Dead-letter queue
resource "google_storage_bucket" "df_dlq" {
  project                     = var.project_id
  name                        = "${var.project_id}-df-dlq-dev"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }

  labels = local.common_labels
}

# ──────────────────────────────────────────────────────────────────────────────
# Pub/Sub — ingest topic, Dataflow subscription, debug pull subscription
# ──────────────────────────────────────────────────────────────────────────────

resource "google_pubsub_topic" "vertex_ml_logs" {
  project = var.project_id
  name    = "vertex-ml-logs-dev"
  labels  = local.common_labels

  # Retain undelivered messages for 7 days (Dataflow can replay on restart)
  message_retention_duration = "604800s"
}

# Primary subscription — Dataflow streaming job reads from here
resource "google_pubsub_subscription" "df_ingest" {
  project = var.project_id
  name    = "vertex-ml-logs-df-sub"
  topic   = google_pubsub_topic.vertex_ml_logs.id
  labels  = local.common_labels

  # Keep unacked messages for 7 days so a restarted job can catch up
  message_retention_duration = "604800s"

  # Ack deadline large enough for Dataflow to process a full window + write
  ack_deadline_seconds = 120

  # No expiration — streaming job subscription must survive job restarts
  expiration_policy {
    ttl = ""
  }
}

# Debug pull subscription — for manual inspection and test verification
resource "google_pubsub_subscription" "debug_sub" {
  project = var.project_id
  name    = "debug-sub"
  topic   = google_pubsub_topic.vertex_ml_logs.id
  labels  = local.common_labels

  message_retention_duration = "600s"  # 10 min — debug only
  ack_deadline_seconds       = 30

  expiration_policy {
    ttl = "86400s"  # auto-expire after 1 day of inactivity
  }
}

# ──────────────────────────────────────────────────────────────────────────────
# Cloud Logging sink — routes Vertex AI logs → Pub/Sub → Dataflow → Kafka
# ──────────────────────────────────────────────────────────────────────────────
#
# Filter covers all six Vertex AI log types:
#   - online_prediction / online_prediction_audit  → resource.type = aiplatform.googleapis.com/Endpoint
#   - batch_prediction                             → resource.type = aiplatform.googleapis.com/BatchPredictionJob
#   - monitoring (violation alerts)                → resource.type = aiplatform.googleapis.com/Endpoint
#   - monitoring_ml (drift/skew)                   → resource.type = aiplatform.googleapis.com/ModelDeploymentMonitoringJob
#   - training (CustomJob / TrainingPipeline)      → resource.type = aiplatform.googleapis.com/CustomJob
#   - legacy resource types (ml_job, cloudml_model_version) kept for any pre-GA entries
#
resource "google_logging_project_sink" "vertex_ml_logs" {
  project = var.project_id
  name    = "vertex-ml-logs-dev-sink"

  destination = "pubsub.googleapis.com/${google_pubsub_topic.vertex_ml_logs.id}"

  filter = join(" OR ", [
    "resource.type=\"aiplatform.googleapis.com/Endpoint\"",
    "resource.type=\"aiplatform.googleapis.com/BatchPredictionJob\"",
    "resource.type=\"aiplatform.googleapis.com/ModelDeploymentMonitoringJob\"",
    "resource.type=\"aiplatform.googleapis.com/CustomJob\"",
    "resource.type=\"ml_job\"",
    "resource.type=\"cloudml_model_version\"",
    "log_id(\"aiplatform.googleapis.com/prediction_logs\")",
  ])

  # Each project sink gets its own writer identity so IAM is scoped precisely
  unique_writer_identity = true

  depends_on = [google_pubsub_topic.vertex_ml_logs]
}

# Grant the log sink's writer SA permission to publish to the ingest topic
resource "google_pubsub_topic_iam_member" "logging_sink_publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.vertex_ml_logs.name
  role    = "roles/pubsub.publisher"
  member  = google_logging_project_sink.vertex_ml_logs.writer_identity
}

# ──────────────────────────────────────────────────────────────────────────────
# Worker Service Account
# ──────────────────────────────────────────────────────────────────────────────
resource "google_service_account" "df_worker" {
  project      = var.project_id
  account_id   = local.worker_sa_name
  display_name = "Dataflow Worker — Vertex ML Logs (dev)"
  description  = "Used by Dataflow workers for Vertex ML Logs pipeline in dev."
}

# ──────────────────────────────────────────────────────────────────────────────
# Dataflow Job (optional — set enable_dataflow_job = true)
# ──────────────────────────────────────────────────────────────────────────────
resource "google_dataflow_flex_template_job" "vertex_ml_logs" {
  count = var.enable_dataflow_job ? 1 : 0

  provider = google-beta

  project                 = var.project_id
  name                    = "${local.name_prefix}-${var.log_type}-dev"
  region                  = var.region
  container_spec_gcs_path = "gs://${google_storage_bucket.df_templates.name}/templates/pubsub-to-kafka-avro.json"

  parameters = {
    log_type                = var.log_type
    input_subscription      = var.pubsub_subscription
    kafka_bootstrap_servers = var.kafka_bootstrap_servers
    kafka_topic             = var.kafka_topic
    kafka_registry_url      = var.kafka_registry_url
    dlq_bucket              = google_storage_bucket.df_dlq.name
    temp_location           = "gs://${google_storage_bucket.df_temp.name}/temp"
    staging_location        = "gs://${google_storage_bucket.df_staging.name}/staging"
  }

  service_account_email = google_service_account.df_worker.email
  machine_type          = var.dataflow_machine_type
  max_workers           = var.dataflow_max_workers
  num_workers           = 1

  # Use default VPC for dev (no Shared VPC)
  # For prod add: subnetwork = "regions/REGION/subnetworks/SUBNET"

  on_delete = "drain"

  labels = local.common_labels

  depends_on = [
    google_project_iam_member.df_worker_dataflow_worker,
    google_project_iam_member.df_worker_log_writer,
    google_project_iam_member.df_worker_monitoring,
  ]
}
