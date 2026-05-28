#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# Local Cloud Build equivalent
#
# Runs the same steps as cloudbuild.yaml but locally using docker + gcloud.
# Use this when you want to build, push and stage the Flex Template without
# waiting for a Cloud Build trigger.
#
# Prerequisites:
#   gcloud auth application-default login
#   gcloud auth configure-docker us-east1-docker.pkg.dev
#   docker daemon running
#   pip install -r requirements.txt -r requirements-dev.txt (for schema step)
#
# Usage:
#   ./scripts/build_and_deploy.sh                    # uses defaults below
#   ./scripts/build_and_deploy.sh --skip-lint        # skip lint+test
#   ./scripts/build_and_deploy.sh --skip-push        # build but don't push
#   ./scripts/build_and_deploy.sh --skip-schema      # skip schema registration
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Config (override via env vars) ────────────────────────────────────────────
PROJECT_ID="${PROJECT_ID:-project-1c03ae00-17f3-43f4-86a}"
REGION="${REGION:-us-east1}"
AR_REPO="${AR_REPO:-vertex-ml-logs}"
IMAGE_NAME="${IMAGE_NAME:-pubsub-to-kafka-avro}"
TEMPLATE_BUCKET="${TEMPLATE_BUCKET:-${PROJECT_ID}-df-templates-dev}"
REGISTRY_URL="${REGISTRY_URL:-https://managedkafka.googleapis.com/v1/projects/${PROJECT_ID}/locations/${REGION}/schemaRegistries/dev_schema_registry}"

IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/${IMAGE_NAME}"
IMAGE_TAG=$(git rev-parse --short HEAD 2>/dev/null || echo "local-$(date +%Y%m%d%H%M)")

# ── Flags ─────────────────────────────────────────────────────────────────────
SKIP_LINT=false
SKIP_PUSH=false
SKIP_SCHEMA=false

for arg in "$@"; do
  case "$arg" in
    --skip-lint)   SKIP_LINT=true ;;
    --skip-push)   SKIP_PUSH=true ;;
    --skip-schema) SKIP_SCHEMA=true ;;
    *) echo "Unknown argument: $arg"; exit 1 ;;
  esac
done

# ── Helpers ───────────────────────────────────────────────────────────────────
step() { echo; echo "══════════════════════════════════════════════"; echo "  ► $1"; echo "══════════════════════════════════════════════"; }
ok()   { echo "  ✓ $1"; }
info() { echo "    $1"; }

# ── Start ─────────────────────────────────────────────────────────────────────
echo
echo "Vertex ML Logs — Local Build & Deploy"
echo "======================================"
info "Project   : ${PROJECT_ID}"
info "Region    : ${REGION}"
info "Image     : ${IMAGE}:${IMAGE_TAG}"
info "Template  : gs://${TEMPLATE_BUCKET}/templates/pubsub-to-kafka-avro.json"
info "Registry  : ${REGISTRY_URL}"

# ── Step 1: Auth ──────────────────────────────────────────────────────────────
step "Step 1 — Configure Docker auth"
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet
ok "Docker configured for ${REGION}-docker.pkg.dev"

# ── Step 2: Lint + Test ───────────────────────────────────────────────────────
if [ "$SKIP_LINT" = false ]; then
  step "Step 2 — Lint & Test"
  echo "  Running black ..."
  python -m black --check pipeline/ scripts/ tests/ || {
    echo "  ✗ black failed. Run: python -m black pipeline/ scripts/ tests/"
    exit 1
  }
  echo "  Running isort ..."
  python -m isort --check pipeline/ scripts/ tests/ || {
    echo "  ✗ isort failed. Run: python -m isort pipeline/ scripts/ tests/"
    exit 1
  }
  echo "  Running flake8 ..."
  python -m flake8 pipeline/ scripts/ tests/ --max-line-length=110 || {
    echo "  ✗ flake8 failed."
    exit 1
  }
  echo "  Running pytest ..."
  python -m pytest tests/ -v --tb=short || {
    echo "  ✗ Tests failed."
    exit 1
  }
  ok "Lint and tests passed"
else
  info "Skipping lint and tests (--skip-lint)"
fi

# ── Step 3: Docker build ──────────────────────────────────────────────────────
step "Step 3 — Docker build"
docker build \
  -t "${IMAGE}:${IMAGE_TAG}" \
  -t "${IMAGE}:latest" \
  -f pipeline/Dockerfile \
  .
ok "Image built: ${IMAGE}:${IMAGE_TAG}"

# ── Step 4: Docker push ───────────────────────────────────────────────────────
if [ "$SKIP_PUSH" = false ]; then
  step "Step 4 — Docker push"
  docker push "${IMAGE}:${IMAGE_TAG}"
  docker push "${IMAGE}:latest"
  echo "${IMAGE_TAG}" > .last-image-tag
  ok "Pushed ${IMAGE}:${IMAGE_TAG}"
  ok "Pushed ${IMAGE}:latest"
else
  info "Skipping push (--skip-push)"
  echo "${IMAGE_TAG}" > .last-image-tag
fi

# ── Step 5: Register Avro schemas ─────────────────────────────────────────────
if [ "$SKIP_SCHEMA" = false ]; then
  step "Step 5 — Register Avro schemas"
  python scripts/register_schemas.py \
    --registry_url "${REGISTRY_URL}"
  ok "Schemas registered"
else
  info "Skipping schema registration (--skip-schema)"
fi

# ── Step 6: Stage Flex Template ───────────────────────────────────────────────
step "Step 6 — Stage Flex Template"
gcloud dataflow flex-template build \
  "gs://${TEMPLATE_BUCKET}/templates/pubsub-to-kafka-avro.json" \
  --image "${IMAGE}:${IMAGE_TAG}" \
  --sdk-language PYTHON \
  --metadata-file pipeline/metadata.json \
  --project "${PROJECT_ID}"

# Write latest tag reference
echo "${IMAGE_TAG}" | gsutil cp - \
  "gs://${TEMPLATE_BUCKET}/templates/latest-image-tag.txt"

ok "Template staged to gs://${TEMPLATE_BUCKET}/templates/pubsub-to-kafka-avro.json"

# ── Done ──────────────────────────────────────────────────────────────────────
echo
echo "══════════════════════════════════════════════"
echo "  ✓ Build & Deploy complete"
echo "══════════════════════════════════════════════"
echo
info "Image    : ${IMAGE}:${IMAGE_TAG}"
info "Template : gs://${TEMPLATE_BUCKET}/templates/pubsub-to-kafka-avro.json"
echo
echo "  Next steps:"
echo "    make tf-apply        — apply Terraform infrastructure (first time)"
echo "    make run-dataflow-job LOG_TYPE=online_prediction"
echo "    make generate-test-log LOG_TYPE=online_prediction --count 5"
echo "    make consume-kafka"
echo
