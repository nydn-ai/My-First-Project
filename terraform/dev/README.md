# terraform/dev/

Terraform configuration for the **dev** environment of the Vertex ML Logs
pipeline.  Manages all GCP infrastructure needed to run the pipeline end-to-end:
Artifact Registry, GCS buckets, Pub/Sub, Cloud Logging sink, service accounts,
IAM, Secret Manager, and an optional Dataflow job.

---

## Files

| File | Purpose |
|------|---------|
| `providers.tf` | Terraform ≥ 1.7, `google` and `google-beta` provider pins (~5.0) |
| `variables.tf` | All input variable declarations with types, defaults, and validation |
| `terraform.tfvars` | Dev-specific values (project ID, region, Kafka URLs, etc.) |
| `main.tf` | Core infra: Artifact Registry, GCS buckets, Pub/Sub topic + subscriptions, Cloud Logging sink, worker SA, optional Dataflow Flex Template job |
| `iam.tf` | IAM bindings for the Dataflow worker SA and the Dataflow Service Agent |
| `cloudbuild.tf` | Cloud Build SA, all required IAM for CI builds, Secret Manager secret for GitHub PAT |
| `outputs.tf` | Named outputs: bucket names, SA emails, subscription IDs, next-step commands |

---

## Quick Start

```bash
cd terraform/dev

# Initialise providers (only needed once or after provider changes)
terraform init

# Preview what will be created
terraform plan

# Create all infrastructure
terraform apply
```

After `apply`, run the commands printed in the `next_steps` output to build
and deploy the pipeline:

```bash
make docker-push         # Build and push the container image
make register-schemas    # Register Avro schemas in the Schema Registry
make stage-template      # Stage the Flex Template spec to GCS
make run-dataflow-job LOG_TYPE=online_prediction   # Launch the Dataflow job
```

---

## Variables

Edit `terraform.tfvars` to change any of these for the dev environment.
Override individual values at plan/apply time with `-var="KEY=VALUE"`.

| Variable | Default | Description |
|----------|---------|-------------|
| `project_id` | `project-1c03ae00-17f3-43f4-86a` | GCP project ID |
| `region` | `us-east1` | Region for all resources |
| `ar_repo_name` | `vertex-ml-logs` | Artifact Registry repository name |
| `image_name` | `pubsub-to-kafka-avro` | Docker image name |
| `template_bucket` | `PROJECT-df-templates-dev` | GCS bucket for Flex Template specs |
| `kafka_bootstrap_servers` | dev cluster | Google Managed Kafka bootstrap server(s) |
| `kafka_topic` | `dev-enriched-vertex-logs` | Destination Kafka topic |
| `kafka_registry_url` | dev registry | Schema Registry base URL |
| `pubsub_subscription` | `…/vertex-ml-logs-df-sub` | Full Pub/Sub subscription path for Dataflow |
| `log_type` | `online_prediction` | Active log type — see routing table below |
| `dataflow_max_workers` | `2` | Maximum Dataflow worker count |
| `dataflow_machine_type` | `n1-standard-2` | Dataflow worker machine type |
| `enable_dataflow_job` | `false` | Set `true` to create the Dataflow job via Terraform |

### Log Type → Topic Routing

Change both `log_type` and `kafka_topic` together to deploy a different job:

| `log_type` | `kafka_topic` |
|---|---|
| `online_prediction` | `dev-enriched-vertex-logs` |
| `batch_prediction` | `dev-enriched-vertex-batch-logs` |
| `monitoring` | `dev-enriched-vertex-monitoring-logs` |
| `training` | `dev-enriched-vertex-training-logs` |

```bash
# Example: deploy a batch prediction job
terraform apply \
  -var="enable_dataflow_job=true" \
  -var="log_type=batch_prediction" \
  -var="kafka_topic=dev-enriched-vertex-batch-logs"
```

---

## Resources Created

### `main.tf`

| Resource | GCP Name | Notes |
|----------|----------|-------|
| Artifact Registry repository | `vertex-ml-logs` | DOCKER format |
| GCS bucket — templates | `PROJECT-df-templates-dev` | Versioned; 10-version lifecycle |
| GCS bucket — staging | `PROJECT-df-staging-dev` | 7-day object TTL |
| GCS bucket — temp | `PROJECT-df-temp-dev` | 3-day object TTL |
| GCS bucket — DLQ | `PROJECT-df-dlq-dev` | 30-day object TTL |
| Pub/Sub topic | `vertex-ml-logs-dev` | 7-day message retention |
| Pub/Sub subscription | `vertex-ml-logs-df-sub` | Primary Dataflow subscription; no TTL expiry |
| Pub/Sub subscription | `debug-sub` | Debug pull; 10-min retention; expires after 1 day idle |
| Cloud Logging sink | `vertex-ml-logs-dev-sink` | Routes 6 Vertex AI resource types → Pub/Sub |
| Service account | `sa-df-vertex-logs-dev` | Dataflow worker identity |
| Dataflow Flex Template job | `vertex-ml-logs-*-dev` | Created only if `enable_dataflow_job=true` |

**Log sink filter** (routes all of):
- `resource.type="aiplatform.googleapis.com/Endpoint"`
- `resource.type="aiplatform.googleapis.com/BatchPredictionJob"`
- `resource.type="aiplatform.googleapis.com/ModelDeploymentMonitoringJob"`
- `resource.type="aiplatform.googleapis.com/CustomJob"`
- `resource.type="ml_job"` (legacy)
- `resource.type="cloudml_model_version"` (legacy)
- `log_id("aiplatform.googleapis.com/prediction_logs")`

### `iam.tf` — Dataflow Worker SA (`sa-df-vertex-logs-dev`)

| Terraform resource | Role | Scope |
|--------------------|------|-------|
| `df_worker_dataflow_worker` | `roles/dataflow.worker` | project |
| `df_worker_log_writer` | `roles/logging.logWriter` | project |
| `df_worker_monitoring` | `roles/monitoring.metricWriter` | project |
| `df_worker_kafka_client` | `roles/managedkafka.client` | project |
| `df_worker_pubsub_subscriber` | `roles/pubsub.subscriber` | `vertex-ml-logs-df-sub` subscription |
| `df_worker_debug_sub_subscriber` | `roles/pubsub.subscriber` | `debug-sub` subscription |
| `df_worker_staging_admin` | `roles/storage.objectAdmin` | `df-staging-dev` bucket |
| `df_worker_temp_admin` | `roles/storage.objectAdmin` | `df-temp-dev` bucket |
| `df_worker_template_viewer` | `roles/storage.objectViewer` | `df-templates-dev` bucket |
| `df_worker_dlq_creator` | `roles/storage.objectCreator` | `df-dlq-dev` bucket |
| `df_worker_ar_reader` | `roles/artifactregistry.reader` | `vertex-ml-logs` AR repo |
| `dataflow_agent_ar_reader` | `roles/artifactregistry.reader` | `vertex-ml-logs` AR repo (Dataflow Service Agent) |
| `dataflow_agent_token_creator` | `roles/iam.serviceAccountUser` | `sa-df-vertex-logs-dev` SA (Dataflow Service Agent) |

> **Critical:** `dataflow_agent_ar_reader` grants `roles/artifactregistry.reader`
> to the Dataflow Service Agent
> (`service-NUMBER@dataflow-service-producer-prod.iam.gserviceaccount.com`).
> Without this, the Dataflow job fails immediately in `QUEUED → RUNNING` with a
> 403 on the Docker image pull — before any worker starts.  This is a common
> source of confusing failures.

### `cloudbuild.tf` — Cloud Build SA (`sa-cloudbuild`) + Infra

| Terraform resource | Role / Resource | Scope / Notes |
|--------------------|----------------|---------------|
| `google_project_service.secretmanager` | API enablement | `secretmanager.googleapis.com` |
| `google_project_service.cloudbuild` | API enablement | `cloudbuild.googleapis.com` |
| `google_service_account.cloudbuild` | SA: `sa-cloudbuild` | Identity for all CI builds |
| `cloudbuild_builder` | `roles/cloudbuild.builds.builder` | project |
| `cloudbuild_log_writer` | `roles/logging.logWriter` | project |
| `cloudbuild_kafka_client` | `roles/managedkafka.client` | project |
| `cloudbuild_schema_registry_editor` | `roles/managedkafka.schemaRegistryEditor` | project |
| `cloudbuild_ar_writer` | `roles/artifactregistry.writer` | `vertex-ml-logs` AR repo (scoped) |
| `cloudbuild_template_admin` | `roles/storage.objectAdmin` | `df-templates-dev` bucket (scoped) |
| `cloudbuild_p4sa_token_creator` | `roles/iam.serviceAccountTokenCreator` | `sa-cloudbuild` SA only (P4SA binding) |
| `google_secret_manager_secret.github_cloudbuild_pat` | Secret resource | PAT value set manually — Terraform manages only the resource |
| `cloudbuild_p4sa_secret_accessor` | `roles/secretmanager.secretAccessor` | `github-cloudbuild-pat` secret only (P4SA) |

**Setting the GitHub PAT value** (not managed by Terraform):
```bash
echo "ghp_YOUR_TOKEN" | gcloud secrets versions add github-cloudbuild-pat --data-file=-
```

**Rotating the PAT:**
```bash
# 1. Add a new version
echo "ghp_NEW_TOKEN" | gcloud secrets versions add github-cloudbuild-pat --data-file=-

# 2. Update the Cloud Build connection to point to the new version
gcloud builds connections update nydn-ai-connection \
  --region=us-east1 \
  --authorizer-token-secret-version=projects/PROJECT/secrets/github-cloudbuild-pat/versions/LATEST
```

---

## Outputs

| Output | Description |
|--------|-------------|
| `worker_sa_email` | Dataflow worker SA email (`sa-df-vertex-logs-dev@…`) |
| `worker_sa_name` | Worker SA full resource name |
| `ar_repository_url` | Docker push/pull URL for the AR repo |
| `template_bucket_name` | Flex Template GCS bucket name |
| `template_gcs_path` | Full `gs://` path to the staged template spec |
| `staging_bucket_name` | Dataflow staging bucket name |
| `temp_bucket_name` | Dataflow temp bucket name |
| `dlq_bucket_name` | Dead-letter queue bucket name |
| `dataflow_job_name` | Active Dataflow job name (or `"not deployed"`) |
| `pubsub_topic` | Pub/Sub ingest topic resource ID |
| `pubsub_df_subscription` | Production Dataflow subscription ID |
| `pubsub_debug_subscription` | Debug pull subscription ID |
| `log_sink_name` | Cloud Logging sink name |
| `log_sink_writer_identity` | Sink writer SA (needs `pubsub.publisher` on the topic) |
| `next_steps` | Shell commands to complete the deploy after `terraform apply` |

---

## State

State is currently stored locally in `terraform.tfstate`.  The `.gitignore`
excludes all `*.tfstate` and `*.tfstate.backup` files — never commit state.

To migrate to a remote GCS backend (recommended for team use):

```bash
# 1. Create the state bucket
gsutil mb -l us-east1 gs://PROJECT-tf-state-dev
gsutil versioning set on gs://PROJECT-tf-state-dev

# 2. Uncomment the backend block in providers.tf:
#    backend "gcs" {
#      bucket = "PROJECT-tf-state-dev"
#      prefix = "vertex-ml-logs/dev"
#    }

# 3. Re-initialise and migrate
terraform init -migrate-state
```

---

## Importing Manually-Created Resources

If a GCP resource was created outside Terraform (e.g., to unblock a build
during an incident), import it before the next `apply` to prevent a 409
conflict:

```bash
# Service account
terraform import google_service_account.cloudbuild \
  projects/PROJECT/serviceAccounts/sa-cloudbuild@PROJECT.iam.gserviceaccount.com

# Project IAM member
terraform import 'google_project_iam_member.cloudbuild_schema_registry_editor' \
  'PROJECT roles/managedkafka.schemaRegistryEditor serviceAccount:sa-cloudbuild@PROJECT.iam.gserviceaccount.com'

# Secret Manager secret
terraform import google_secret_manager_secret.github_cloudbuild_pat \
  projects/PROJECT/secrets/github-cloudbuild-pat

# PubSub topic
terraform import google_pubsub_topic.vertex_ml_logs \
  projects/PROJECT/topics/vertex-ml-logs-dev
```

See [`../../docs/TROUBLESHOOTING.md`](../../docs/TROUBLESHOOTING.md#10-terraform--409-conflict-on-existing-resources)
for the complete import ID format reference for every resource type used here.

After all imports, run `terraform plan` and confirm **No changes** before
applying.
