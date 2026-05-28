# terraform/

Terraform configuration for the Vertex ML Logs pipeline GCP infrastructure.
Currently contains one environment: `dev/`.

```
terraform/
└── dev/
    ├── providers.tf      Terraform and provider version constraints
    ├── variables.tf      Input variable declarations with defaults
    ├── terraform.tfvars  Dev-environment variable values (NOT sensitive)
    ├── main.tf           Core resources (AR, GCS buckets, PubSub, log sink,
    │                     worker SA, optional Dataflow job)
    ├── iam.tf            IAM bindings for the Dataflow worker SA
    ├── cloudbuild.tf     IAM + secrets for the Cloud Build pipeline SA
    └── outputs.tf        Terraform outputs (resource names, next-steps)
```

---

## Prerequisites

- Terraform ≥ 1.7
- `gcloud auth application-default login` with an account that has
  `roles/owner` or equivalent on the GCP project
- Required APIs enabled (Terraform enables them automatically via
  `google_project_service` resources in `cloudbuild.tf`)

---

## First-Time Setup

```bash
cd terraform/dev

# Initialise providers
terraform init

# Review the plan
terraform plan

# Apply — creates all infrastructure (SA, PubSub, GCS, log sink, IAM, etc.)
terraform apply
```

After `apply`, the `next_steps` output shows the commands to run to deploy
the pipeline.

---

## Environment Variables

All configurable values live in `terraform.tfvars`.  Override any value on
the command line with `-var`:

```bash
terraform apply -var="log_type=batch_prediction" -var="kafka_topic=dev-enriched-vertex-batch-logs"
```

| Variable | Default | Description |
|----------|---------|-------------|
| `project_id` | `project-1c03ae00-17f3-43f4-86a` | GCP project ID |
| `region` | `us-east1` | GCP region for all resources |
| `ar_repo_name` | `vertex-ml-logs` | Artifact Registry repository name |
| `image_name` | `pubsub-to-kafka-avro` | Docker image name |
| `template_bucket` | `PROJECT-df-templates-dev` | GCS bucket for Flex Template specs |
| `kafka_bootstrap_servers` | dev cluster URL | Managed Kafka bootstrap server(s) |
| `kafka_topic` | `dev-enriched-vertex-logs` | Destination Kafka topic |
| `kafka_registry_url` | dev registry URL | Schema Registry base URL |
| `pubsub_subscription` | `…/vertex-ml-logs-df-sub` | Full Pub/Sub subscription path |
| `log_type` | `online_prediction` | Active log type (`online_prediction` \| `batch_prediction` \| `monitoring` \| `training`) |
| `dataflow_max_workers` | `2` | Max Dataflow worker count |
| `dataflow_machine_type` | `n1-standard-2` | Dataflow worker machine type |
| `enable_dataflow_job` | `false` | Set `true` to create the Dataflow job via Terraform |

---

## Resource Inventory

### `main.tf`

| Resource | Name / ID | Purpose |
|----------|-----------|---------|
| `google_artifact_registry_repository` | `vertex-ml-logs` | Docker image repository |
| `google_storage_bucket` | `PROJECT-df-templates-dev` | Flex Template spec storage |
| `google_storage_bucket` | `PROJECT-df-staging-dev` | Dataflow SDK staging (7-day TTL) |
| `google_storage_bucket` | `PROJECT-df-temp-dev` | Dataflow shuffle temp (3-day TTL) |
| `google_storage_bucket` | `PROJECT-df-dlq-dev` | Dead-letter queue (30-day TTL) |
| `google_pubsub_topic` | `vertex-ml-logs-dev` | Ingest topic (7-day retention) |
| `google_pubsub_subscription` | `vertex-ml-logs-df-sub` | Dataflow streaming subscription |
| `google_pubsub_subscription` | `debug-sub` | Debug pull subscription (10-min retention) |
| `google_logging_project_sink` | `vertex-ml-logs-dev-sink` | Routes Vertex AI logs → Pub/Sub |
| `google_service_account` | `sa-df-vertex-logs-dev` | Dataflow worker identity |
| `google_dataflow_flex_template_job` | `vertex-ml-logs-*-dev` | Dataflow job (if `enable_dataflow_job=true`) |

**Log sink filter** routes six Vertex AI resource types:
- `aiplatform.googleapis.com/Endpoint`
- `aiplatform.googleapis.com/BatchPredictionJob`
- `aiplatform.googleapis.com/ModelDeploymentMonitoringJob`
- `aiplatform.googleapis.com/CustomJob`
- `ml_job` (legacy)
- `cloudml_model_version` (legacy)

### `iam.tf` — Dataflow Worker SA (`sa-df-vertex-logs-dev`)

| Resource | Role | Scope |
|----------|------|-------|
| `df_worker_dataflow_worker` | `roles/dataflow.worker` | project |
| `df_worker_log_writer` | `roles/logging.logWriter` | project |
| `df_worker_monitoring` | `roles/monitoring.metricWriter` | project |
| `df_worker_kafka_client` | `roles/managedkafka.client` | project |
| `df_worker_pubsub_subscriber` | `roles/pubsub.subscriber` | `vertex-ml-logs-df-sub` |
| `df_worker_debug_sub_subscriber` | `roles/pubsub.subscriber` | `debug-sub` |
| `df_worker_staging_admin` | `roles/storage.objectAdmin` | `df-staging-dev` bucket |
| `df_worker_temp_admin` | `roles/storage.objectAdmin` | `df-temp-dev` bucket |
| `df_worker_template_viewer` | `roles/storage.objectViewer` | `df-templates-dev` bucket |
| `df_worker_dlq_creator` | `roles/storage.objectCreator` | `df-dlq-dev` bucket |
| `df_worker_ar_reader` | `roles/artifactregistry.reader` | `vertex-ml-logs` AR repo |
| `dataflow_agent_ar_reader` | `roles/artifactregistry.reader` | `vertex-ml-logs` AR repo (Dataflow Service Agent) |
| `dataflow_agent_token_creator` | `roles/iam.serviceAccountUser` | `sa-df-vertex-logs-dev` SA (Dataflow Service Agent) |

> **Important:** The Dataflow Service Agent
> (`service-NUMBER@dataflow-service-producer-prod.iam.gserviceaccount.com`)
> must have `artifactregistry.reader` on the repo.  Without it, the job fails
> immediately in the `QUEUED → RUNNING` transition before any worker starts.

### `cloudbuild.tf` — Cloud Build SA (`sa-cloudbuild`)

| Resource | Role | Scope |
|----------|------|-------|
| `cloudbuild_builder` | `roles/cloudbuild.builds.builder` | project |
| `cloudbuild_log_writer` | `roles/logging.logWriter` | project |
| `cloudbuild_kafka_client` | `roles/managedkafka.client` | project |
| `cloudbuild_schema_registry_editor` | `roles/managedkafka.schemaRegistryEditor` | project |
| `cloudbuild_ar_writer` | `roles/artifactregistry.writer` | `vertex-ml-logs` AR repo |
| `cloudbuild_template_admin` | `roles/storage.objectAdmin` | `df-templates-dev` bucket |
| `cloudbuild_p4sa_token_creator` | `roles/iam.serviceAccountTokenCreator` | `sa-cloudbuild` SA (P4SA only) |
| `cloudbuild_p4sa_secret_accessor` | `roles/secretmanager.secretAccessor` | `github-cloudbuild-pat` secret (P4SA only) |

The GitHub PAT is stored in Secret Manager as `github-cloudbuild-pat`.
**Terraform manages the secret resource but not its value.**  Set the value
once manually:
```bash
echo "ghp_YOUR_PAT" | gcloud secrets versions add github-cloudbuild-pat --data-file=-
```

---

## Deploying the Dataflow Job

By default `enable_dataflow_job = false` — Terraform manages infrastructure
only.  To launch the job via Terraform:

```bash
terraform apply \
  -var="enable_dataflow_job=true" \
  -var="log_type=online_prediction" \
  -var="kafka_topic=dev-enriched-vertex-logs"
```

Or launch it manually via `make run-dataflow-job` (recommended — gives
more control over the exact template version):

```bash
make run-dataflow-job LOG_TYPE=online_prediction
```

To deploy a **different log type**, change both `log_type` and `kafka_topic`
together (the Makefile target does this automatically from `LOG_TYPE`).

---

## Importing Manually-Created Resources

If a resource was created outside of Terraform (e.g., during incident
response), import it before applying:

```bash
# Example: import a project IAM member
terraform import 'google_project_iam_member.cloudbuild_builder' \
  'PROJECT_ID roles/cloudbuild.builds.builder serviceAccount:sa-cloudbuild@PROJECT_ID.iam.gserviceaccount.com'
```

See [`docs/TROUBLESHOOTING.md § 10`](../docs/TROUBLESHOOTING.md#10-terraform--409-conflict-on-existing-resources)
for the full import ID format reference and common import patterns.

---

## State Management

State is currently stored **locally** (`terraform.tfstate`).  For team use,
migrate to a GCS backend:

```hcl
# In providers.tf — uncomment and create the bucket first
backend "gcs" {
  bucket = "PROJECT-tf-state-dev"
  prefix = "vertex-ml-logs/dev"
}
```

```bash
# Create the state bucket
gsutil mb -l us-east1 gs://PROJECT-tf-state-dev
gsutil versioning set on gs://PROJECT-tf-state-dev

# Migrate local state to GCS
terraform init -migrate-state
```

---

## Outputs

After `terraform apply`, these values are printed:

| Output | Description |
|--------|-------------|
| `worker_sa_email` | Dataflow worker SA email |
| `ar_repository_url` | Artifact Registry URL for Docker images |
| `template_bucket_name` | Flex Template GCS bucket |
| `template_gcs_path` | Full GCS path to the staged template spec |
| `pubsub_topic` | Pub/Sub ingest topic ID |
| `pubsub_df_subscription` | Production Dataflow subscription ID |
| `pubsub_debug_subscription` | Debug pull subscription ID |
| `log_sink_name` | Cloud Logging sink name |
| `log_sink_writer_identity` | Sink's writer SA (used for `pubsub.publisher` grant) |
| `next_steps` | Commands to run next (docker-push, register-schemas, etc.) |
