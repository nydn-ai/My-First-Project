# ──────────────────────────────────────────────────────────────────────────────
# IAM — Dataflow Worker Service Account
# ──────────────────────────────────────────────────────────────────────────────

# ── Project-level roles ───────────────────────────────────────────────────────

# Required to run as a Dataflow worker
resource "google_project_iam_member" "df_worker_dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.df_worker.email}"
}

# Write pipeline logs to Cloud Logging
resource "google_project_iam_member" "df_worker_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.df_worker.email}"
}

# Emit Dataflow metrics to Cloud Monitoring
resource "google_project_iam_member" "df_worker_monitoring" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.df_worker.email}"
}

# ── Pub/Sub subscriptions ─────────────────────────────────────────────────────

# Subscriber access on the production ingest subscription
resource "google_pubsub_subscription_iam_member" "df_worker_pubsub_subscriber" {
  project      = var.project_id
  subscription = google_pubsub_subscription.df_ingest.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.df_worker.email}"
}

# Subscriber access on the debug subscription (kept for ad-hoc testing)
resource "google_pubsub_subscription_iam_member" "df_worker_debug_sub_subscriber" {
  project      = var.project_id
  subscription = google_pubsub_subscription.debug_sub.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.df_worker.email}"
}

# ── GCS Buckets ───────────────────────────────────────────────────────────────

# Full access to staging and temp buckets
resource "google_storage_bucket_iam_member" "df_worker_staging_admin" {
  bucket = google_storage_bucket.df_staging.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.df_worker.email}"
}

resource "google_storage_bucket_iam_member" "df_worker_temp_admin" {
  bucket = google_storage_bucket.df_temp.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.df_worker.email}"
}

# Read access to the template bucket (to load the Flex Template)
resource "google_storage_bucket_iam_member" "df_worker_template_viewer" {
  bucket = google_storage_bucket.df_templates.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.df_worker.email}"
}

# Write-only access to DLQ bucket (dead-letter messages)
resource "google_storage_bucket_iam_member" "df_worker_dlq_creator" {
  bucket = google_storage_bucket.df_dlq.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.df_worker.email}"
}

# ── Artifact Registry ─────────────────────────────────────────────────────────

# Pull the container image from AR — worker SA
resource "google_artifact_registry_repository_iam_member" "df_worker_ar_reader" {
  project    = var.project_id
  location   = var.region
  repository = google_artifact_registry_repository.vertex_ml_logs.repository_id
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.df_worker.email}"
}

# Pull the launcher image from AR — Dataflow Service Agent
#
# The Dataflow service agent pulls the Flex Template launcher Docker image
# BEFORE workers start.  Without this binding a fresh deploy fails immediately
# with "403 permission denied" during the QUEUED → RUNNING transition.
# This was encountered as a production bug; pinned here so tf apply never loses it.
resource "google_artifact_registry_repository_iam_member" "dataflow_agent_ar_reader" {
  project    = var.project_id
  location   = var.region
  repository = google_artifact_registry_repository.vertex_ml_logs.repository_id
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:service-${data.google_project.dev.number}@dataflow-service-producer-prod.iam.gserviceaccount.com"
}

# ── Google Managed Kafka ──────────────────────────────────────────────────────
# roles/managedkafka.client covers:
#   - Kafka produce/consume
#   - Schema Registry read/write
# No username/password needed — ADC OAUTHBEARER is used instead.

resource "google_project_iam_member" "df_worker_kafka_client" {
  project = var.project_id # Kafka cluster project (same as pipeline project for dev)
  role    = "roles/managedkafka.client"
  member  = "serviceAccount:${google_service_account.df_worker.email}"
}

# ──────────────────────────────────────────────────────────────────────────────
# IAM — Dataflow Service Agent
#
# The Dataflow service agent (dataflow-service-producer-prod@dataflow-service-accounts.iam.gserviceaccount.com)
# needs to impersonate the worker SA and pull from AR.
# ──────────────────────────────────────────────────────────────────────────────

data "google_project" "dev" {
  project_id = var.project_id
}

# Dataflow service agent needs to act as the worker SA
resource "google_service_account_iam_member" "dataflow_agent_token_creator" {
  service_account_id = google_service_account.df_worker.name
  role               = "roles/iam.serviceAccountUser"
  member = "serviceAccount:service-${data.google_project.dev.number}@dataflow-service-producer-prod.iam.gserviceaccount.com"
}
