# ──────────────────────────────────────────────────────────────────────────────
# Vertex ML Logs Pipeline — Makefile
#
# Dev project defaults (override via env vars or CLI args)
# ──────────────────────────────────────────────────────────────────────────────

# Use the venv Python if present, otherwise fall back to system python
ifeq ($(OS),Windows_NT)
    PYTHON ?= .venv/Scripts/python.exe
    # Windows: use PowerShell for UTC timestamp (Unix 'date -u' is not available)
    BUILD_DATE := $(shell powershell -NoProfile -Command "[DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ssZ')" 2>NUL)
else
    PYTHON ?= .venv/bin/python
    BUILD_DATE := $(shell date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || echo unknown)
endif

PROJECT_ID      ?= project-1c03ae00-17f3-43f4-86a
REGION          ?= us-east1
AR_REPO         ?= vertex-ml-logs
IMAGE_NAME      ?= pubsub-to-kafka-avro

TEMPLATE_BUCKET ?= $(PROJECT_ID)-df-templates-dev

REGISTRY_URL    ?= https://managedkafka.googleapis.com/v1/projects/$(PROJECT_ID)/locations/$(REGION)/schemaRegistries/dev_schema_registry

KAFKA_BOOTSTRAP ?= bootstrap.dev-vertex-log-kafka.$(REGION).managedkafka.$(PROJECT_ID).cloud.goog:9092

# online prediction topic   (VertexPredictionLog)
KAFKA_TOPIC                 ?= dev-enriched-vertex-logs

# batch prediction topic    (VertexBatchLog)
KAFKA_TOPIC_BATCH           ?= dev-enriched-vertex-batch-logs

# monitoring topic          (VertexMonitoringLog)
KAFKA_TOPIC_MONITORING      ?= dev-enriched-vertex-monitoring-logs

# training topic            (VertexTrainingLog)
KAFKA_TOPIC_TRAINING        ?= dev-enriched-vertex-training-logs

PUBSUB_SUB      ?= projects/$(PROJECT_ID)/subscriptions/vertex-ml-logs-df-sub
LOG_TYPE        ?= online_prediction
COUNT           ?= 3

IMAGE           := $(REGION)-docker.pkg.dev/$(PROJECT_ID)/$(AR_REPO)/$(IMAGE_NAME)
IMAGE_TAG       := $(shell git rev-parse --short HEAD 2>/dev/null || echo local)

.PHONY: help auth install lint test \
        docker-build docker-push \
        register-schemas ensure-schemas check-schemas stage-template \
        build-and-deploy \
        tf-init tf-plan tf-apply tf-destroy \
        run-dataflow-job cancel-dataflow-job \
        generate-test-log \
        generate-test-log-online generate-test-log-online-audit \
        generate-test-log-batch \
        generate-test-log-monitoring generate-test-log-monitoring-ml \
        generate-test-log-training \
        generate-test-log-all \
        print-test-log \
        write-cloud-log \
        write-cloud-log-online write-cloud-log-online-audit \
        write-cloud-log-batch \
        write-cloud-log-monitoring write-cloud-log-monitoring-ml \
        write-cloud-log-training \
        write-cloud-log-all \
        consume-kafka consume-kafka-batch \
        consume-kafka-monitoring consume-kafka-training \
        clean

# ── Help ──────────────────────────────────────────────────────────────────────

help:
	@echo ""
	@echo "Vertex ML Logs Pipeline"
	@echo "======================="
	@echo ""
	@echo "Auth:"
	@echo "  make auth                        gcloud + docker auth"
	@echo ""
	@echo "Dev:"
	@echo "  make install                     pip install all dependencies"
	@echo "  make lint                        black + isort + flake8"
	@echo "  make test                        pytest (93 tests, all offline)"
	@echo ""
	@echo "Build:"
	@echo "  make docker-build                Build container image"
	@echo "  make docker-push                 Push image to Artifact Registry"
	@echo "  make register-schemas            Register / update all Avro schemas"
	@echo "  make ensure-schemas              Provision only missing schemas"
	@echo "  make check-schemas               Verify all schemas exist"
	@echo "  make stage-template              Stage Flex Template spec to GCS"
	@echo "  make build-and-deploy            Full build + deploy"
	@echo ""
	@echo "Terraform:"
	@echo "  make tf-init"
	@echo "  make tf-plan"
	@echo "  make tf-apply"
	@echo "  make tf-destroy"
	@echo ""
	@echo "Run:"
	@echo "  make run-dataflow-job            Launch Dataflow Flex Template job"
	@echo "  make cancel-dataflow-job         Cancel all running vertex-ml-logs jobs"
	@echo ""
	@echo "Test log generation (publishes to Pub/Sub → Dataflow → Kafka):"
	@echo "  make generate-test-log           online_prediction container log (default)"
	@echo "  make generate-test-log-online    online_prediction container log"
	@echo "  make generate-test-log-online-audit  online_prediction audit log (protoPayload)"
	@echo "  make generate-test-log-batch     batch_prediction audit log"
	@echo "  make generate-test-log-monitoring    monitoring infra violation alert"
	@echo "  make generate-test-log-monitoring-ml monitoring ML drift/skew log"
	@echo "  make generate-test-log-training  training CustomJob audit log"
	@echo "  make generate-test-log-all       one of each log type (6 messages)"
	@echo "  make print-test-log              print entry without publishing (LOG_TYPE=...)"
	@echo ""
	@echo "Cloud Logging sink test (writes to Cloud Logging → log sink → Pub/Sub → Dataflow):"
	@echo "  make write-cloud-log             online_prediction via Cloud Logging (default)"
	@echo "  make write-cloud-log-online      online_prediction container log"
	@echo "  make write-cloud-log-online-audit  online_prediction audit log"
	@echo "  make write-cloud-log-batch       batch_prediction audit log"
	@echo "  make write-cloud-log-monitoring  monitoring infra violation alert"
	@echo "  make write-cloud-log-monitoring-ml monitoring ML drift/skew log"
	@echo "  make write-cloud-log-training    training CustomJob audit log"
	@echo "  make write-cloud-log-all         online_prediction + monitoring via Cloud Logging (2 non-audit entries)"
	@echo ""
	@echo "Kafka consumers (VPC-only -- run from a GCE VM or Cloud Shell):"
	@echo "  make consume-kafka               online_prediction topic"
	@echo "  make consume-kafka-batch         batch_prediction topic"
	@echo "  make consume-kafka-monitoring    monitoring topic"
	@echo "  make consume-kafka-training      training topic"
	@echo ""
	@echo "Overrides:"
	@echo "  PROJECT_ID REGION LOG_TYPE KAFKA_TOPIC PUBSUB_SUB COUNT"
	@echo ""

# ── Auth ──────────────────────────────────────────────────────────────────────

auth:
	gcloud auth application-default login
	gcloud auth configure-docker $(REGION)-docker.pkg.dev --quiet

# ── Install ───────────────────────────────────────────────────────────────────

install:
	$(PYTHON) -m pip install -r requirements-dev.txt
	$(PYTHON) -m pip install --no-deps -e pipeline/

# ── Lint ──────────────────────────────────────────────────────────────────────

lint:
	$(PYTHON) -m black --check pipeline/ scripts/ tests/
	$(PYTHON) -m isort --check pipeline/ scripts/ tests/
	$(PYTHON) -m flake8 pipeline/ scripts/ tests/

lint-fix:
	$(PYTHON) -m black pipeline/ scripts/ tests/
	$(PYTHON) -m isort pipeline/ scripts/ tests/

# ── Test ──────────────────────────────────────────────────────────────────────

test:
	$(PYTHON) -m pytest tests/ -v

# ── Docker ────────────────────────────────────────────────────────────────────

docker-build:
	docker build \
	  --build-arg GIT_SHA=$(IMAGE_TAG) \
	  --build-arg BUILD_DATE=$(BUILD_DATE) \
	  -t $(IMAGE):$(IMAGE_TAG) \
	  -t $(IMAGE):latest \
	  -f pipeline/Dockerfile \
	  .

docker-push: docker-build
	docker push $(IMAGE):$(IMAGE_TAG)
	docker push $(IMAGE):latest
	@echo "$(IMAGE_TAG)" > .last-image-tag
	@echo "Pushed $(IMAGE):$(IMAGE_TAG)"

# ── Schema Registry ───────────────────────────────────────────────────────────

register-schemas:
	python scripts/register_schemas.py \
	  --registry_url "$(REGISTRY_URL)"

ensure-schemas:
	@echo "Provisioning missing schemas..."
	python scripts/register_schemas.py \
	  --registry_url "$(REGISTRY_URL)" \
	  --missing-only

check-schemas:
	@echo "Verifying all required schemas are registered..."
	python scripts/register_schemas.py \
	  --registry_url "$(REGISTRY_URL)" \
	  --check

# ── Flex Template ─────────────────────────────────────────────────────────────

stage-template:
	$(eval TAG := $(shell cat .last-image-tag 2>/dev/null || echo latest))

	gcloud dataflow flex-template build \
	  "gs://$(TEMPLATE_BUCKET)/templates/pubsub-to-kafka-avro.json" \
	  --image "$(IMAGE):$(TAG)" \
	  --sdk-language PYTHON \
	  --metadata-file pipeline/metadata.json \
	  --project $(PROJECT_ID)

	@echo "Template staged to gs://$(TEMPLATE_BUCKET)/templates/pubsub-to-kafka-avro.json"

# ── Full build + deploy ───────────────────────────────────────────────────────

build-and-deploy: docker-push register-schemas stage-template
	@echo ""
	@echo "✓ Build complete"
	@echo "  Image    : $(IMAGE):$(IMAGE_TAG)"
	@echo "  Registry : $(REGISTRY_URL)"
	@echo "  Template : gs://$(TEMPLATE_BUCKET)/templates/pubsub-to-kafka-avro.json"

# ── Terraform ─────────────────────────────────────────────────────────────────

tf-init:
	cd terraform/dev && terraform init

tf-plan:
	cd terraform/dev && terraform plan \
	  -var="project_id=$(PROJECT_ID)" \
	  -var="region=$(REGION)" \
	  -var="ar_repo_name=$(AR_REPO)" \
	  -var="image_name=$(IMAGE_NAME)" \
	  -var="template_bucket=$(TEMPLATE_BUCKET)" \
	  -var="kafka_bootstrap_servers=$(KAFKA_BOOTSTRAP)" \
	  -var="kafka_topic=$(KAFKA_TOPIC)" \
	  -var="kafka_registry_url=$(REGISTRY_URL)" \
	  -var="pubsub_subscription=$(PUBSUB_SUB)"

tf-apply:
	cd terraform/dev && terraform apply \
	  -var="project_id=$(PROJECT_ID)" \
	  -var="region=$(REGION)" \
	  -var="ar_repo_name=$(AR_REPO)" \
	  -var="image_name=$(IMAGE_NAME)" \
	  -var="template_bucket=$(TEMPLATE_BUCKET)" \
	  -var="kafka_bootstrap_servers=$(KAFKA_BOOTSTRAP)" \
	  -var="kafka_topic=$(KAFKA_TOPIC)" \
	  -var="kafka_registry_url=$(REGISTRY_URL)" \
	  -var="pubsub_subscription=$(PUBSUB_SUB)"

tf-destroy:
	cd terraform/dev && terraform destroy \
	  -var="project_id=$(PROJECT_ID)" \
	  -var="region=$(REGION)" \
	  -var="ar_repo_name=$(AR_REPO)" \
	  -var="image_name=$(IMAGE_NAME)" \
	  -var="template_bucket=$(TEMPLATE_BUCKET)" \
	  -var="kafka_bootstrap_servers=$(KAFKA_BOOTSTRAP)" \
	  -var="kafka_topic=$(KAFKA_TOPIC)" \
	  -var="kafka_registry_url=$(REGISTRY_URL)" \
	  -var="pubsub_subscription=$(PUBSUB_SUB)"

# ── Run Dataflow job ──────────────────────────────────────────────────────────

run-dataflow-job:
	$(eval WORKER_SA := $(shell cd terraform/dev && terraform output -raw worker_sa_email 2>/dev/null || echo ""))
	$(eval DLQ_BUCKET := $(shell cd terraform/dev && terraform output -raw dlq_bucket_name 2>/dev/null || echo "$(PROJECT_ID)-df-dlq-dev"))
	$(eval STAGING_BUCKET := $(shell cd terraform/dev && terraform output -raw staging_bucket_name 2>/dev/null || echo "$(PROJECT_ID)-df-staging-dev"))
	$(eval TEMP_BUCKET := $(shell cd terraform/dev && terraform output -raw temp_bucket_name 2>/dev/null || echo "$(PROJECT_ID)-df-temp-dev"))

	# Dataflow job names cannot contain underscores.
	$(eval JOB_LOG_TYPE := $(shell echo $(LOG_TYPE) | tr '_' '-'))

	gcloud dataflow flex-template run "vertex-ml-logs-$(JOB_LOG_TYPE)-dev-$(shell date +%Y%m%d%H%M)" \
	  --template-file-gcs-location "gs://$(TEMPLATE_BUCKET)/templates/pubsub-to-kafka-avro.json" \
	  --project $(PROJECT_ID) \
	  --region $(REGION) \
	  --service-account-email "$(WORKER_SA)" \
	  --staging-location "gs://$(STAGING_BUCKET)/staging" \
	  --parameters log_type=$(LOG_TYPE) \
	  --parameters input_subscription="$(PUBSUB_SUB)" \
	  --parameters kafka_bootstrap_servers="$(KAFKA_BOOTSTRAP)" \
	  --parameters kafka_topic="$(KAFKA_TOPIC)" \
	  --parameters kafka_registry_url="$(REGISTRY_URL)" \
	  --parameters dlq_bucket="$(DLQ_BUCKET)" \
	  --temp-location "gs://$(TEMP_BUCKET)/temp"

cancel-dataflow-job:
	@echo "Cancelling running Vertex ML Logs Dataflow jobs..."
	@JOBS=$$(gcloud dataflow jobs list \
	  --project $(PROJECT_ID) \
	  --region $(REGION) \
	  --filter="state=Running AND name~vertex-ml-logs" \
	  --format="value(id)"); \
	if [ -z "$$JOBS" ]; then \
	  echo "No running vertex-ml-logs jobs found."; \
	else \
	  for JOB_ID in $$JOBS; do \
	    echo "Cancelling $$JOB_ID ..."; \
	    gcloud dataflow jobs cancel $$JOB_ID \
	      --project $(PROJECT_ID) \
	      --region $(REGION); \
	  done; \
	fi

# ── Test log generation ───────────────────────────────────────────────────────
#
# Each target publishes COUNT (default 3) entries of a specific log type to
# the Pub/Sub topic attached to PUBSUB_SUB.  The Dataflow pipeline picks them
# up, maps them to Avro, and writes to the corresponding Kafka topic.
#
# Override count:  make generate-test-log-batch COUNT=10
# Skip publishing: make print-test-log LOG_TYPE=monitoring

# Generic target — uses LOG_TYPE variable (default: online_prediction)
generate-test-log:
	$(PYTHON) scripts/generate_test_log.py \
	  --project_id $(PROJECT_ID) \
	  --subscription $(PUBSUB_SUB) \
	  --log_type $(LOG_TYPE) \
	  --count $(COUNT)

# ── Per-type targets ──────────────────────────────────────────────────────────

# online_prediction — container log format (jsonPayload.message + top-level labels)
# Matches real production entries from aiplatform.googleapis.com/prediction_container
generate-test-log-online:
	$(PYTHON) scripts/generate_test_log.py \
	  --project_id $(PROJECT_ID) \
	  --subscription $(PUBSUB_SUB) \
	  --log_type online_prediction \
	  --count $(COUNT)

# online_prediction — audit log format (protoPayload PredictionService.Predict RPC)
# Alternative format; the pipeline mapper handles both
generate-test-log-online-audit:
	$(PYTHON) scripts/generate_test_log.py \
	  --project_id $(PROJECT_ID) \
	  --subscription $(PUBSUB_SUB) \
	  --log_type online_prediction_audit \
	  --count $(COUNT)

# batch_prediction — BatchPredictionJob lifecycle audit log
generate-test-log-batch:
	$(PYTHON) scripts/generate_test_log.py \
	  --project_id $(PROJECT_ID) \
	  --subscription $(PUBSUB_SUB) \
	  --log_type batch_prediction \
	  --count $(COUNT)

# monitoring — infra violation alert (monitoring.googleapis.com/ViolationOpenEventv1)
# No protoPayload/jsonPayload — all signal is in top-level labels
generate-test-log-monitoring:
	$(PYTHON) scripts/generate_test_log.py \
	  --project_id $(PROJECT_ID) \
	  --subscription $(PUBSUB_SUB) \
	  --log_type monitoring \
	  --count $(COUNT)

# monitoring_ml — ML model monitoring drift/skew result
# protoPayload + jsonPayload with feature metrics and thresholds
generate-test-log-monitoring-ml:
	$(PYTHON) scripts/generate_test_log.py \
	  --project_id $(PROJECT_ID) \
	  --subscription $(PUBSUB_SUB) \
	  --log_type monitoring_ml \
	  --count $(COUNT)

# training — CustomJob / TrainingPipeline audit log with worker pool specs + metrics
generate-test-log-training:
	$(PYTHON) scripts/generate_test_log.py \
	  --project_id $(PROJECT_ID) \
	  --subscription $(PUBSUB_SUB) \
	  --log_type training \
	  --count $(COUNT)

# Generate one entry of every log type in a single call
# Useful for a quick end-to-end smoke test of the full pipeline
generate-test-log-all:
	@echo "Publishing one entry per log type (6 messages total)..."
	$(PYTHON) scripts/generate_test_log.py \
	  --project_id $(PROJECT_ID) \
	  --subscription $(PUBSUB_SUB) \
	  --log_type online_prediction \
	  --count 1
	$(PYTHON) scripts/generate_test_log.py \
	  --project_id $(PROJECT_ID) \
	  --subscription $(PUBSUB_SUB) \
	  --log_type online_prediction_audit \
	  --count 1
	$(PYTHON) scripts/generate_test_log.py \
	  --project_id $(PROJECT_ID) \
	  --subscription $(PUBSUB_SUB) \
	  --log_type batch_prediction \
	  --count 1
	$(PYTHON) scripts/generate_test_log.py \
	  --project_id $(PROJECT_ID) \
	  --subscription $(PUBSUB_SUB) \
	  --log_type monitoring \
	  --count 1
	$(PYTHON) scripts/generate_test_log.py \
	  --project_id $(PROJECT_ID) \
	  --subscription $(PUBSUB_SUB) \
	  --log_type monitoring_ml \
	  --count 1
	$(PYTHON) scripts/generate_test_log.py \
	  --project_id $(PROJECT_ID) \
	  --subscription $(PUBSUB_SUB) \
	  --log_type training \
	  --count 1
	@echo "Done. 6 messages published -- check Kafka consumers to verify."

# Print generated entries to stdout without publishing (dry-run / inspection)
# Usage: make print-test-log LOG_TYPE=batch_prediction
print-test-log:
	$(PYTHON) scripts/generate_test_log.py \
	  --project_id $(PROJECT_ID) \
	  --subscription $(PUBSUB_SUB) \
	  --log_type $(LOG_TYPE) \
	  --count $(COUNT) \
	  --print_only

# ── Cloud Logging sink test ───────────────────────────────────────────────────
#
# These targets write entries DIRECTLY to Cloud Logging (bypassing Pub/Sub).
# A configured log sink then routes matching entries to Pub/Sub → Dataflow → Kafka.
#
# Use these to verify the full end-to-end path including the log sink:
#   Cloud Logging → (log sink filter) → Pub/Sub → Dataflow → Avro → Kafka
#
# Prerequisites: ADC credentials with roles/logging.logWriter on the project.
#
# Override count:  make write-cloud-log-batch COUNT=5

# Generic target — uses LOG_TYPE variable (default: online_prediction)
write-cloud-log:
	$(PYTHON) scripts/generate_test_log.py \
	  --project_id $(PROJECT_ID) \
	  --log_type $(LOG_TYPE) \
	  --count $(COUNT) \
	  --cloud_logging_only

# online_prediction — container log format (jsonPayload.message + top-level labels)
write-cloud-log-online:
	$(PYTHON) scripts/generate_test_log.py \
	  --project_id $(PROJECT_ID) \
	  --log_type online_prediction \
	  --count $(COUNT) \
	  --cloud_logging_only

# online_prediction — audit log format (protoPayload PredictionService.Predict RPC)
write-cloud-log-online-audit:
	$(PYTHON) scripts/generate_test_log.py \
	  --project_id $(PROJECT_ID) \
	  --log_type online_prediction_audit \
	  --count $(COUNT) \
	  --cloud_logging_only

# batch_prediction — BatchPredictionJob lifecycle audit log
write-cloud-log-batch:
	$(PYTHON) scripts/generate_test_log.py \
	  --project_id $(PROJECT_ID) \
	  --log_type batch_prediction \
	  --count $(COUNT) \
	  --cloud_logging_only

# monitoring — infra violation alert (monitoring.googleapis.com/ViolationOpenEventv1)
# No protoPayload/jsonPayload — all signal is in top-level labels (passed correctly)
write-cloud-log-monitoring:
	$(PYTHON) scripts/generate_test_log.py \
	  --project_id $(PROJECT_ID) \
	  --log_type monitoring \
	  --count $(COUNT) \
	  --cloud_logging_only

# monitoring_ml — ML model monitoring drift/skew result
write-cloud-log-monitoring-ml:
	$(PYTHON) scripts/generate_test_log.py \
	  --project_id $(PROJECT_ID) \
	  --log_type monitoring_ml \
	  --count $(COUNT) \
	  --cloud_logging_only

# training — CustomJob / TrainingPipeline audit log
write-cloud-log-training:
	$(PYTHON) scripts/generate_test_log.py \
	  --project_id $(PROJECT_ID) \
	  --log_type training \
	  --count $(COUNT) \
	  --cloud_logging_only

# Write one entry per non-audit log type via Cloud Logging (sink routing smoke test)
#
# NOTE: Only log types that use non-audit log names can be written by user code.
#   WRITABLE via Cloud Logging:  online_prediction, monitoring
#   AUDIT-PROTECTED (skip here): online_prediction_audit, batch_prediction,
#                                monitoring_ml, training
#   For audit-log types, use generate-test-log-* to publish directly to Pub/Sub.
write-cloud-log-all:
	@echo "Writing non-audit log entries to Cloud Logging (2 entries -- sink routing test)..."
	$(PYTHON) scripts/generate_test_log.py \
	  --project_id $(PROJECT_ID) \
	  --log_type online_prediction \
	  --count 1 \
	  --cloud_logging_only
	$(PYTHON) scripts/generate_test_log.py \
	  --project_id $(PROJECT_ID) \
	  --log_type monitoring \
	  --count 1 \
	  --cloud_logging_only
	@echo "Done. 2 entries written to Cloud Logging."
	@echo "Audit-log types (batch, monitoring_ml, training, online_prediction_audit)"
	@echo "cannot be written by user code -- use 'make generate-test-log-all' for those."

consume-kafka:
	python scripts/test_kafka_consumer.py \
	  --bootstrap_servers "$(KAFKA_BOOTSTRAP)" \
	  --topic "$(KAFKA_TOPIC)" \
	  --registry_url "$(REGISTRY_URL)" \
	  --timeout 30

consume-kafka-batch:
	python scripts/test_kafka_consumer.py \
	  --bootstrap_servers "$(KAFKA_BOOTSTRAP)" \
	  --topic "$(KAFKA_TOPIC_BATCH)" \
	  --registry_url "$(REGISTRY_URL)" \
	  --timeout 30

consume-kafka-monitoring:
	python scripts/test_kafka_consumer.py \
	  --bootstrap_servers "$(KAFKA_BOOTSTRAP)" \
	  --topic "$(KAFKA_TOPIC_MONITORING)" \
	  --registry_url "$(REGISTRY_URL)" \
	  --timeout 30

consume-kafka-training:
	python scripts/test_kafka_consumer.py \
	  --bootstrap_servers "$(KAFKA_BOOTSTRAP)" \
	  --topic "$(KAFKA_TOPIC_TRAINING)" \
	  --registry_url "$(REGISTRY_URL)" \
	  --timeout 30

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -f .last-image-tag
	