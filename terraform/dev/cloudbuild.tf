# ──────────────────────────────────────────────────────────────────────────────
# Cloud Build Pipeline
#
# Service account used by Cloud Build triggers to:
#   - Build and push Docker images to Artifact Registry
#   - Register Avro schemas to Managed Kafka Schema Registry
#   - Stage Flex Template specs to GCS
#
# The Cloud Build P4SA (gcp-sa-cloudbuild) needs:
#   - serviceAccountTokenCreator on this SA  →  to impersonate it during builds
#   - secretmanager.secretAccessor on the PAT secret  →  to read GitHub creds
# ──────────────────────────────────────────────────────────────────────────────

# ── Required APIs ─────────────────────────────────────────────────────────────

resource "google_project_service" "secretmanager" {
  project            = var.project_id
  service            = "secretmanager.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "cloudbuild" {
  project            = var.project_id
  service            = "cloudbuild.googleapis.com"
  disable_on_destroy = false
}

# ── Cloud Build Service Account ───────────────────────────────────────────────

resource "google_service_account" "cloudbuild" {
  project      = var.project_id
  account_id   = "sa-cloudbuild"
  display_name = "Cloud Build Pipeline SA"
  description  = "Used by Cloud Build to build images, register schemas, and stage Flex Templates."
}

# ── Cloud Build SA — Project-level roles ──────────────────────────────────────

# Allows running Cloud Build steps (required base role for custom SA builds)
resource "google_project_iam_member" "cloudbuild_builder" {
  project = var.project_id
  role    = "roles/cloudbuild.builds.builder"
  member  = "serviceAccount:${google_service_account.cloudbuild.email}"
}

# Write build logs to Cloud Logging
resource "google_project_iam_member" "cloudbuild_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.cloudbuild.email}"
}

# Produce/consume Kafka — needed to authenticate against Managed Kafka
# during schema registration (scripts/register_schemas.py uses ADC)
resource "google_project_iam_member" "cloudbuild_kafka_client" {
  project = var.project_id
  role    = "roles/managedkafka.client"
  member  = "serviceAccount:${google_service_account.cloudbuild.email}"
}

# Register Avro schemas to the Managed Kafka Schema Registry
resource "google_project_iam_member" "cloudbuild_schema_registry_editor" {
  project = var.project_id
  role    = "roles/managedkafka.schemaRegistryEditor"
  member  = "serviceAccount:${google_service_account.cloudbuild.email}"
}

# ── Cloud Build SA — Resource-scoped roles ────────────────────────────────────

# Push Docker images to Artifact Registry
resource "google_artifact_registry_repository_iam_member" "cloudbuild_ar_writer" {
  project    = var.project_id
  location   = var.region
  repository = google_artifact_registry_repository.vertex_ml_logs.repository_id
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${google_service_account.cloudbuild.email}"
}

# Write Flex Template JSON + latest-image-tag to the template bucket
resource "google_storage_bucket_iam_member" "cloudbuild_template_admin" {
  bucket = google_storage_bucket.df_templates.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.cloudbuild.email}"
}

# ── Cloud Build P4SA — impersonate the Cloud Build SA ─────────────────────────
#
# Cloud Build's platform SA (gcp-sa-cloudbuild) must be able to create tokens
# for the custom SA so that build steps run under it.

resource "google_service_account_iam_member" "cloudbuild_p4sa_token_creator" {
  service_account_id = google_service_account.cloudbuild.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:service-${data.google_project.dev.number}@gcp-sa-cloudbuild.iam.gserviceaccount.com"
}

# ── GitHub PAT Secret ─────────────────────────────────────────────────────────
#
# The secret VALUE is NOT managed here — it is set once via:
#   gcloud secrets versions add github-cloudbuild-pat --data-file=-
# Rotating the PAT: add a new secret version and update the Cloud Build
# connection's --authorizer-token-secret-version to point to the new version.

resource "google_secret_manager_secret" "github_cloudbuild_pat" {
  project   = var.project_id
  secret_id = "github-cloudbuild-pat"

  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }

  labels = local.common_labels

  depends_on = [google_project_service.secretmanager]
}

# Cloud Build P4SA reads the PAT to clone from GitHub
resource "google_secret_manager_secret_iam_member" "cloudbuild_p4sa_secret_accessor" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.github_cloudbuild_pat.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:service-${data.google_project.dev.number}@gcp-sa-cloudbuild.iam.gserviceaccount.com"
}
