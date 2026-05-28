# Troubleshooting Guide — Vertex ML Logs Pipeline

This document captures every real failure encountered during development and
operation of the PubSub → Kafka Avro Dataflow pipeline, together with root
causes, diagnostics, and the exact fixes applied.  Each entry is written so
that the next person hitting the same wall can resolve it in minutes rather
than hours.

---

## Table of Contents

1. [Cloud Build — Trigger Creation](#1-cloud-build--trigger-creation)
2. [Cloud Build — Service Account / IAM](#2-cloud-build--service-account--iam)
3. [Cloud Build — Lint Failures](#3-cloud-build--lint-failures)
4. [Cloud Build — Dockerfile Parse Error](#4-cloud-build--dockerfile-parse-error)
5. [Cloud Build — Schema Registry 403](#5-cloud-build--schema-registry-403)
6. [Cloud Build — GitHub App / Connection Setup](#6-cloud-build--github-app--connection-setup)
7. [Dataflow — Kafka OAUTHBEARER Authentication](#7-dataflow--kafka-oauthbearer-authentication)
8. [Dataflow — Artifact Registry 403 (Dataflow Service Agent)](#8-dataflow--artifact-registry-403-dataflow-service-agent)
9. [Dataflow — Worker SA Missing PubSub Subscriber Access](#9-dataflow--worker-sa-missing-pubsub-subscriber-access)
10. [Terraform — 409 Conflict on Existing Resources](#10-terraform--409-conflict-on-existing-resources)
11. [Terraform — IAM Drift After Manual Fixes](#11-terraform--iam-drift-after-manual-fixes)
12. [Pipeline — Container Logs Producing Sparse Avro Records](#12-pipeline--container-logs-producing-sparse-avro-records)
13. [Secret Manager — Cloud Build P4SA Too Broad](#13-secret-manager--cloud-build-p4sa-too-broad)

---

## 1. Cloud Build — Trigger Creation

### Symptom
```
ERROR: (gcloud.builds.triggers.create) INVALID_ARGUMENT: Request contains an
invalid argument.
```
Occurs when creating Cloud Build triggers against a **2nd-generation GitHub
connection** using the `gcloud builds triggers create github` subcommand.

### Root Cause
The `gcloud builds triggers create github` CLI does not support the
`serviceAccount` field required by 2nd-gen connections.  Without it the API
rejects the request.

### Fix
Create the trigger via the **Cloud Build REST API** directly instead of gcloud:

```bash
ACCESS_TOKEN=$(gcloud auth print-access-token)
PROJECT_ID="your-project-id"
SA="sa-cloudbuild@${PROJECT_ID}.iam.gserviceaccount.com"
REPO_RESOURCE="projects/${PROJECT_ID}/locations/us-east1/connections/nydn-ai-connection/repositories/nydn-ai-My-First-Project"

curl -s -X POST \
  "https://cloudbuild.googleapis.com/v1/projects/${PROJECT_ID}/locations/us-east1/triggers" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "push-to-master",
    "description": "Push to master — build and deploy",
    "serviceAccount": "projects/'"${PROJECT_ID}"'/serviceAccounts/'"${SA}"'",
    "repositoryEventConfig": {
      "repository": "'"${REPO_RESOURCE}"'",
      "push": { "branch": "^master$" }
    },
    "filename": "cloudbuild.yaml"
  }'
```

Key point: the `serviceAccount` field must be a full resource name:
`projects/PROJECT/serviceAccounts/SA_EMAIL`.

---

## 2. Cloud Build — Service Account / IAM

### Symptom A — `invalid value for build.service_account`
```
INVALID_ARGUMENT: invalid value for build.service_account:
  357220095406@cloudbuild.gserviceaccount.com
```
Triggered when the `serviceAccount` field in the trigger references the Cloud
Build **P4SA** (`NUMBER@cloudbuild.gserviceaccount.com`).

### Root Cause
The Cloud Build P4SA cannot be used as a user-specified service account in a
trigger.  The `serviceAccount` field must reference a **user-managed** SA.

### Fix
Create a dedicated SA and reference it in the trigger:

```bash
gcloud iam service-accounts create sa-cloudbuild \
  --display-name="Cloud Build Pipeline SA" \
  --project=PROJECT_ID
```

Assign minimum required roles (all now in `terraform/dev/cloudbuild.tf`):

| Role | Scope | Reason |
|------|-------|--------|
| `roles/cloudbuild.builds.builder` | project | Run Cloud Build steps |
| `roles/logging.logWriter` | project | Write build logs |
| `roles/managedkafka.client` | project | Kafka produce/consume |
| `roles/managedkafka.schemaRegistryEditor` | project | Register Avro schemas |
| `roles/artifactregistry.writer` | AR repo (scoped) | Push Docker images |
| `roles/storage.objectAdmin` | template bucket (scoped) | Stage Flex Template |

The Cloud Build **P4SA** needs one additional role to impersonate the custom SA:

```bash
gcloud iam service-accounts add-iam-policy-binding \
  sa-cloudbuild@PROJECT_ID.iam.gserviceaccount.com \
  --member="serviceAccount:service-NUMBER@gcp-sa-cloudbuild.iam.gserviceaccount.com" \
  --role="roles/iam.serviceAccountTokenCreator"
```

This is a **service-account-level** binding (not project-level).  All of the
above is codified in `terraform/dev/cloudbuild.tf`.

### Symptom B — Build step silently uses wrong identity
If the build runs successfully but pushes as the P4SA (no explicit SA in
trigger), images and templates will be owned by the wrong identity and may
fail IAM checks in downstream steps.  Always confirm the trigger JSON has
`serviceAccount` set.

---

## 3. Cloud Build — Lint Failures

### 3a. `lint-isort` exits 1 — multi-import on one line

**Error log:**
```
ERROR: ... Imports are incorrectly sorted and/or formatted.
```

**Root Cause:** `isort` requires multi-symbol imports from the same module to
use the multi-line parenthesised form:

```python
# BAD — isort will reject this
from pipeline.utils.schema_registry import SchemaRegistryClient, _strip_non_avro_keys

# GOOD
from pipeline.utils.schema_registry import (
    SchemaRegistryClient,
    _strip_non_avro_keys,
)
```

**Fix:** Run isort locally to auto-fix before pushing:
```bash
python -m isort pipeline/ scripts/ tests/
```

### 3b. `lint-flake8` exits 1 — unused imports (F401)

**Error log:**
```
F401 'traceback' imported but unused
F401 'uuid' imported but unused
F401 'urllib.parse' imported but unused
```

**Fix:** Remove the unused imports.  Common culprits when refactoring:
- `import traceback` left behind after moving exception handling
- `import uuid` left from an earlier implementation
- `import urllib.parse` inside a function body that no longer uses it

Run flake8 locally before pushing:
```bash
python -m flake8 pipeline/ scripts/ tests/ --max-line-length=110
```

### 3c. `lint-black` exits 1 — formatting

**Fix:** Run black locally:
```bash
python -m black pipeline/ scripts/ tests/
```

Always run tools in this order before committing:
```bash
python -m black pipeline/ scripts/ tests/
python -m isort pipeline/ scripts/ tests/
python -m flake8 pipeline/ scripts/ tests/ --max-line-length=110
```

---

## 4. Cloud Build — Dockerfile Parse Error

### Symptom
```
dockerfile parse error line N: unknown instruction: IMPORT
```
or
```
failed to solve: failed to read dockerfile: ...
```

### Root Cause
The Dockerfile used a **heredoc** `RUN` syntax:
```dockerfile
RUN python - <<'PY'
import pipeline.main
from pipeline.transforms.kafka_sink import WriteToKafkaAvro
print("Import health check OK")
PY
```
Cloud Build uses Kaniko/BuildKit by default only for certain configurations.
Without explicit BuildKit enablement, the classic Docker builder is used, and
it does not support heredoc syntax introduced in Dockerfile 1.4+.

### Fix
Replace heredoc with a single-line `-c` argument:
```dockerfile
RUN python -c "import pipeline.main; from pipeline.transforms.kafka_sink import WriteToKafkaAvro; print('Import health check OK')"
```

If you genuinely need multi-line shell in a `RUN` step, use `bash -c` with
escaped newlines or a `&&`-chained command. Never rely on heredoc `<<` inside
a Dockerfile unless you have confirmed BuildKit is enabled.

---

## 5. Cloud Build — Schema Registry 403

### Symptom
```
Step #6 - "register-schemas": POST https://managedkafka.googleapis.com/...
  403 PERMISSION_DENIED: Permission 'managedkafka.versions.create' denied
  on resource '...schemaRegistries/dev_schema_registry'.
```

### Root Cause
`roles/managedkafka.client` covers Kafka **produce/consume** operations but
does **not** include schema registry write operations.  The `versions.create`
permission requires a separate role.

### Fix
Grant `roles/managedkafka.schemaRegistryEditor` to the Cloud Build SA:

```bash
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:sa-cloudbuild@PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/managedkafka.schemaRegistryEditor"
```

This is now in `terraform/dev/cloudbuild.tf` as
`google_project_iam_member.cloudbuild_schema_registry_editor`.

**Role summary for Managed Kafka:**

| Role | Covers |
|------|--------|
| `roles/managedkafka.client` | Produce, consume, list topics |
| `roles/managedkafka.schemaRegistryEditor` | Register/update schemas (`versions.create`) |
| `roles/managedkafka.viewer` | Read schema metadata only |

---

## 6. Cloud Build — GitHub App / Connection Setup

### Symptom A — `PENDING_INSTALL_APP` after OAuth
```
Error processing oauth callback: connection installation state is
PENDING_INSTALL_APP, not PENDING_USER_OAUTH as expected
```

**Root Cause:** The Cloud Build GitHub App must be installed on the **GitHub
account that owns the repository** (`nydn-ai`), not on a collaborator account
(`atewodros`).  If you start the OAuth flow while logged in as a collaborator,
the app cannot be installed and the connection stays in `PENDING_INSTALL_APP`.

**Fix:**
1. Delete the stale connection:
   ```bash
   gcloud builds connections delete CONNECTION_NAME --region=REGION
   ```
2. Re-create the connection using a PAT from the **repo-owner account**:
   ```bash
   gcloud secrets versions add github-cloudbuild-pat --data-file=- <<< "ghp_..."
   gcloud builds connections create github CONNECTION_NAME \
     --region=REGION \
     --authorizer-token-secret-version=projects/PROJECT/secrets/github-cloudbuild-pat/versions/latest
   ```
3. Click the `install_v2` URL printed by step 2 while **logged in as the repo
   owner** in your browser.
4. Verify the connection reaches `COMPLETE`:
   ```bash
   gcloud builds connections describe CONNECTION_NAME --region=REGION
   ```

### Symptom B — "user does not have access to installation"
```
user does not have access to installation 136226521
```
Same root cause as above.  The GitHub App installation ID referenced in the
connection belongs to a different account.  Delete and recreate using the
owner's PAT.

### Rotating the GitHub PAT
The PAT value is **never** stored in Terraform (the secret resource is managed
but not its value).  To rotate:
```bash
# Add a new version
echo "ghp_NEW_TOKEN" | gcloud secrets versions add github-cloudbuild-pat --data-file=-

# Update the connection to use the new version
gcloud builds connections update CONNECTION_NAME \
  --region=REGION \
  --authorizer-token-secret-version=projects/PROJECT/secrets/github-cloudbuild-pat/versions/LATEST_VERSION
```

---

## 7. Dataflow — Kafka OAUTHBEARER Authentication

### Symptom
```
[rdkafka] SASL handshake failed: OAUTHBEARER token validation error
```
or the producer silently fails to connect and all messages time out.

### Root Cause
Google Managed Kafka requires a specific **3-part base64url-encoded token**
for OAUTHBEARER SASL, not a plain Bearer access token.  The format is:

```
base64url(header) + "." + base64url(claims) + "." + base64url(access_token)
```

Where:
- `header = {"typ": "JWT", "alg": "GOOG_OAUTH2_TOKEN"}`
- `claims = {"exp": <unix_ts>, "iss": "Google", "iat": <unix_ts>, "scope": "kafka", "sub": <sa_email>}`
- `access_token` = the raw ADC access token (`ya29.…`)

Passing a plain access token (or a standard JWT) will be rejected.

### Fix
The `_make_oauth_cb()` function in `pipeline/transforms/kafka_sink.py` already
implements this format correctly.  If you see this error after upgrading
`google-auth` or `confluent-kafka`, verify:

1. The token is being built in the correct 3-part format.
2. The `expiry_ts` is a **UTC-aware** float Unix timestamp (not a naive
   datetime — naive datetimes produce wrong `expiry` values in some timezones).
3. The `creds.token` is non-empty after `creds.refresh(req)`.

**Quick diagnostic:**
```python
import google.auth, google.auth.transport.requests, base64, json, time

creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
creds.refresh(google.auth.transport.requests.Request())

def b64url(s):
    return base64.urlsafe_b64encode(s.encode()).decode().rstrip("=")

header  = b64url(json.dumps({"typ": "JWT", "alg": "GOOG_OAUTH2_TOKEN"}))
claims  = b64url(json.dumps({"exp": time.time()+3600, "iss": "Google",
                              "iat": time.time(), "scope": "kafka",
                              "sub": creds.service_account_email}))
token   = b64url(creds.token)
print(f"{header}.{claims}.{token}")
```

### Required IAM
The Dataflow worker SA must have `roles/managedkafka.client` on the project
(or the Kafka cluster's project if different):
```bash
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:sa-df-vertex-logs-dev@PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/managedkafka.client"
```

---

## 8. Dataflow — Artifact Registry 403 (Dataflow Service Agent)

### Symptom
Dataflow job transitions from `QUEUED` → error immediately:
```
Workflow failed. Causes: There was a problem refreshing your credentials ...
  Error: Failed to pull Docker image ... 403 Forbidden
```

### Root Cause
The **Dataflow Service Agent**
(`service-NUMBER@dataflow-service-producer-prod.iam.gserviceaccount.com`)
pulls the Flex Template launcher Docker image **before** any worker starts.
If the service agent does not have `roles/artifactregistry.reader` on the
repository, the job fails in the `QUEUED → RUNNING` transition.

This identity is different from the worker SA — it is the platform-level agent
and it is easy to overlook.

### Fix
Grant `roles/artifactregistry.reader` to the Dataflow Service Agent on the
Artifact Registry repository:

```bash
gcloud artifacts repositories add-iam-policy-binding vertex-ml-logs \
  --location=REGION \
  --member="serviceAccount:service-NUMBER@dataflow-service-producer-prod.iam.gserviceaccount.com" \
  --role="roles/artifactregistry.reader"
```

This is codified in `terraform/dev/iam.tf` as
`google_artifact_registry_repository_iam_member.dataflow_agent_ar_reader`.

The worker SA also needs reader access on the same repo for the actual
container pull:
```
google_artifact_registry_repository_iam_member.df_worker_ar_reader
```

---

## 9. Dataflow — Worker SA Missing PubSub Subscriber Access

### Symptom
Dataflow job starts but immediately stalls with no data processed:
```
Workflow failed. Causes: Permission denied on resource ...
  Error: PERMISSION_DENIED: User not authorized to perform this action.
```
Or more subtly: the job runs but processes 0 messages.

### Root Cause
The worker SA (`sa-df-vertex-logs-dev`) needs `roles/pubsub.subscriber` on
the **specific subscription** it reads from.  A common mistake is to grant
this on the `debug-sub` subscription (used during development) and forget to
grant it on the production `vertex-ml-logs-df-sub` subscription.

### Fix
Grant subscriber access on the production subscription:
```bash
gcloud pubsub subscriptions add-iam-policy-binding vertex-ml-logs-df-sub \
  --member="serviceAccount:sa-df-vertex-logs-dev@PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/pubsub.subscriber"
```

In Terraform (`terraform/dev/iam.tf`) this is managed as:
```hcl
resource "google_pubsub_subscription_iam_member" "df_worker_pubsub_subscriber" {
  subscription = google_pubsub_subscription.df_ingest.name   # NOT hardcoded "debug-sub"
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.df_worker.email}"
}
```

**Warning:** If the Terraform resource had `subscription = "debug-sub"` hardcoded,
the production subscription will lack the binding and the job will fail silently.
Always reference the Terraform resource rather than hardcoding subscription names.

---

## 10. Terraform — 409 Conflict on Existing Resources

### Symptom
```
Error: Error creating Topic: googleapi: Error 409: Resource already exists in
  the project: projects/PROJECT/topics/vertex-ml-logs-dev., alreadyExists
```

### Root Cause
The GCP resource was created manually (or by a previous Terraform run from a
different state) and is not in the current Terraform state.  Terraform tries to
`CREATE` it and gets a 409.

### Fix
Import the existing resource into Terraform state:

```bash
# PubSub topic
terraform import google_pubsub_topic.vertex_ml_logs \
  projects/PROJECT_ID/topics/vertex-ml-logs-dev

# PubSub subscription
terraform import google_pubsub_subscription.debug_sub \
  projects/PROJECT_ID/subscriptions/debug-sub

# Service account
terraform import google_service_account.cloudbuild \
  projects/PROJECT_ID/serviceAccounts/sa-cloudbuild@PROJECT_ID.iam.gserviceaccount.com

# Secret Manager secret
terraform import google_secret_manager_secret.github_cloudbuild_pat \
  projects/PROJECT_ID/secrets/github-cloudbuild-pat

# Project IAM member
terraform import 'google_project_iam_member.cloudbuild_builder' \
  'PROJECT_ID roles/cloudbuild.builds.builder serviceAccount:sa-cloudbuild@PROJECT_ID.iam.gserviceaccount.com'

# AR repository IAM member
terraform import 'google_artifact_registry_repository_iam_member.cloudbuild_ar_writer' \
  'projects/PROJECT_ID/locations/REGION/repositories/REPO_NAME roles/artifactregistry.writer serviceAccount:SA_EMAIL'

# GCS bucket IAM member
terraform import 'google_storage_bucket_iam_member.cloudbuild_template_admin' \
  'BUCKET_NAME roles/storage.objectAdmin serviceAccount:SA_EMAIL'

# Service account IAM member (resource-scoped)
terraform import 'google_service_account_iam_member.cloudbuild_p4sa_token_creator' \
  'projects/PROJECT_ID/serviceAccounts/SA_EMAIL roles/iam.serviceAccountTokenCreator serviceAccount:MEMBER_EMAIL'

# Secret Manager IAM member
terraform import 'google_secret_manager_secret_iam_member.cloudbuild_p4sa_secret_accessor' \
  'projects/PROJECT_ID/secrets/SECRET_ID roles/secretmanager.secretAccessor serviceAccount:SA_EMAIL'
```

After all imports: run `terraform plan` and confirm **no changes** before
applying anything.

**Import ID format reference:**

| Resource type | Import ID format |
|---|---|
| `google_project_service` | `PROJECT_ID/SERVICE_NAME` |
| `google_service_account` | `projects/PROJECT/serviceAccounts/EMAIL` |
| `google_project_iam_member` | `PROJECT ROLE MEMBER` (space-separated) |
| `google_pubsub_subscription_iam_member` | `projects/PROJECT/subscriptions/NAME ROLE MEMBER` |
| `google_artifact_registry_repository_iam_member` | `projects/PROJECT/locations/LOC/repositories/REPO ROLE MEMBER` |
| `google_storage_bucket_iam_member` | `BUCKET_NAME ROLE MEMBER` |
| `google_service_account_iam_member` | `projects/PROJECT/serviceAccounts/EMAIL ROLE MEMBER` |
| `google_secret_manager_secret_iam_member` | `projects/PROJECT/secrets/SECRET_ID ROLE MEMBER` |

---

## 11. Terraform — IAM Drift After Manual Fixes

### Symptom
`terraform plan` shows resources to **destroy** that you know are needed, or
shows bindings that differ from what is in GCP.

### Root Cause
Manually applied IAM bindings (via `gcloud` during incident response) are not
in Terraform state.  Conversely, overly broad bindings applied manually to
unblock a build are now captured in state and need scoping down.

### Common over-grants to audit and remove

| What was granted | Why it's wrong | Replacement |
|---|---|---|
| `roles/storage.objectAdmin` on **project** for `sa-cloudbuild` | Can write to every bucket | Scoped to `df-templates-dev` bucket only (in `cloudbuild.tf`) |
| `roles/artifactregistry.writer` on **project** for `sa-cloudbuild` | Can push to every repo | Scoped to `vertex-ml-logs` repo (in `cloudbuild.tf`) |
| `roles/iam.serviceAccountTokenCreator` on **project** for `sa-cloudbuild` | Can impersonate any SA | SA-level binding: P4SA on `sa-cloudbuild` only (in `cloudbuild.tf`) |
| `roles/secretmanager.admin` on **project** for Cloud Build P4SA | Can read/write every secret | `secretAccessor` on `github-cloudbuild-pat` only (in `cloudbuild.tf`) |

**Cleanup commands:**
```bash
PROJECT=your-project-id
SA=sa-cloudbuild@${PROJECT}.iam.gserviceaccount.com
P4SA=service-NUMBER@gcp-sa-cloudbuild.iam.gserviceaccount.com

gcloud projects remove-iam-policy-binding $PROJECT \
  --member="serviceAccount:${SA}" --role="roles/storage.objectAdmin"

gcloud projects remove-iam-policy-binding $PROJECT \
  --member="serviceAccount:${SA}" --role="roles/artifactregistry.writer"

gcloud projects remove-iam-policy-binding $PROJECT \
  --member="serviceAccount:${SA}" --role="roles/iam.serviceAccountTokenCreator"

gcloud projects remove-iam-policy-binding $PROJECT \
  --member="serviceAccount:${P4SA}" --role="roles/secretmanager.admin"
```

After cleanup run `terraform plan` — it should show **No changes**.

### Best practice
Always apply IAM through Terraform.  When a manual fix is needed urgently
(e.g., to unblock a build), do it manually first, then immediately add it to
the appropriate `.tf` file and `terraform import` it.  Never leave manual
bindings untracked.

---

## 12. Pipeline — Container Logs Producing Sparse Avro Records

### Symptom
Downstream consumers see `VertexPredictionLog` records where nearly every
prediction field is `null` (`method`, `latency_ms`, `status_code`,
`request_payload_json`, `response_payload_json`).

### Root Cause
The Cloud Logging sink filter routes **all** logs from
`resource.type="aiplatform.googleapis.com/Endpoint"`, which includes:
- Prediction request/response logs (the intended events)
- Container lifecycle logs (startup, health checks, model loading)

A container startup log looks like this in Cloud Logging:
```json
{
  "jsonPayload": {
    "message": "I0528 08:32:13.441753 1 vertex_ai_server.cc:350] \"Started Vertex AI HTTPService at 0.0.0.0:8089\""
  },
  "logName": "projects/.../logs/aiplatform.googleapis.com%2Fprediction_container",
  "severity": "ERROR"
}
```

Note: the `I` prefix in the message is **glog INFO** — the application-level
severity is INFO, but Cloud Logging may tag it as `ERROR` if the log stream
is stderr.  There is no prediction data in this entry.

**What the pipeline produces for this entry:**
```json
{
  "endpoint_id": "spiderweb-lite-staging",
  "severity": "ERROR",
  "payload": {
    "deployed_model_id": "1547128309001748480",
    "method": null,
    "latency_ms": null,
    "status_code": null,
    "request_payload_json": null,
    "response_payload_json": null,
    "error_message": null
  },
  "raw_payload_json": "{\"message\": \"I0528 ... Started Vertex AI HTTPService ...\"}"
}
```

The record is valid Avro and will be written to Kafka (not DLQ).

### Options

**Option A — Tighten the log sink filter** (recommended):
Add `log_id("aiplatform.googleapis.com/predict")` or
`protoPayload.methodName:Predict` to the sink filter so only actual prediction
audit logs flow through:
```hcl
filter = join(" OR ", [
  "resource.type=\"aiplatform.googleapis.com/Endpoint\" AND log_id(\"aiplatform.googleapis.com/predict\")",
  ...
])
```

**Option B — Add a pipeline pre-filter step:**
Drop entries that have `jsonPayload.message` but no prediction-relevant fields:
```python
# In build_pipeline(), before BuildAvro:
def _is_prediction_entry(entry):
    return (entry.get("protoPayload") or
            (entry.get("jsonPayload") or {}).get("latency_ms") or
            (entry.get("jsonPayload") or {}).get("deployed_model_id"))

filtered = windowed | "FilterNoise" >> beam.Filter(_is_prediction_entry)
```

**Option B** keeps the sink broad (useful for audit/debugging) while preventing
noisy records from reaching Kafka.

---

## 13. Secret Manager — Cloud Build P4SA Too Broad

### Symptom
The Cloud Build P4SA (`service-NUMBER@gcp-sa-cloudbuild.iam.gserviceaccount.com`)
has `roles/secretmanager.admin` at the **project level**.  This grants read/write
access to **every secret** in the project.

### Root Cause
During initial Cloud Build connection setup, `roles/secretmanager.admin` was
granted at the project level to quickly unblock the PAT secret access.  This
is too broad.

### Fix
Grant `roles/secretmanager.secretAccessor` scoped to only the PAT secret, then
remove the project-level admin grant:

```bash
# Scoped grant (already applied via terraform/dev/cloudbuild.tf)
gcloud secrets add-iam-policy-binding github-cloudbuild-pat \
  --member="serviceAccount:service-NUMBER@gcp-sa-cloudbuild.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

# Remove over-grant
gcloud projects remove-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:service-NUMBER@gcp-sa-cloudbuild.iam.gserviceaccount.com" \
  --role="roles/secretmanager.admin"
```

The scoped binding is captured in Terraform as
`google_secret_manager_secret_iam_member.cloudbuild_p4sa_secret_accessor`.

---

## General Diagnostic Commands

```bash
# Show all project-level roles for a service account
gcloud projects get-iam-policy PROJECT_ID --format=json | python3 -c "
import json, sys
p = json.load(sys.stdin)
sa = 'serviceAccount:SA_EMAIL'
print([b['role'] for b in p['bindings'] if sa in b.get('members',[])])
"

# Check Cloud Build build logs (last 5 builds)
gcloud builds list --limit=5 --project=PROJECT_ID

# Stream a specific build's logs
gcloud builds log BUILD_ID --project=PROJECT_ID

# Check Dataflow job status
gcloud dataflow jobs list --region=REGION --project=PROJECT_ID

# Check Cloud Build connection status
gcloud builds connections describe CONNECTION_NAME --region=REGION --project=PROJECT_ID

# Verify Secret Manager secret exists
gcloud secrets versions list github-cloudbuild-pat --project=PROJECT_ID

# Check IAM on a PubSub subscription
gcloud pubsub subscriptions get-iam-policy SUBSCRIPTION_NAME --project=PROJECT_ID

# Check IAM on an Artifact Registry repo
gcloud artifacts repositories get-iam-policy REPO_NAME \
  --location=REGION --project=PROJECT_ID
```

---

## Checklist — Before a New Deploy

- [ ] `terraform plan` shows **No changes** (or only expected changes)
- [ ] `make lint` passes locally (black + isort + flake8)
- [ ] `make test` passes locally
- [ ] Cloud Build SA has scoped IAM only (no project-level storage/AR admin)
- [ ] Cloud Build P4SA has only `secretAccessor` on the PAT secret (not `secretmanager.admin`)
- [ ] Both subscription IAM bindings exist: `vertex-ml-logs-df-sub` and `debug-sub`
- [ ] Dataflow Service Agent has `artifactregistry.reader` on the repo
- [ ] GitHub connection is `COMPLETE` (not `PENDING_*`)
