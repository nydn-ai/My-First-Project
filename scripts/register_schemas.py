#!/usr/bin/env python3
"""
Register Vertex ML Log Avro schemas in the Schema Registry.

Registers (in order, idempotent):
  1. com.bumble.avro.ml.vertex.VertexPredictionLog  — online prediction logs
  2. com.bumble.avro.ml.vertex.VertexBatchLog       — batch prediction job logs
  3. com.bumble.avro.ml.vertex.VertexMonitoringLog  — model monitoring logs
  4. com.bumble.avro.ml.vertex.VertexTrainingLog    — training job logs

All schemas are self-contained (no cross-schema references).
Payload sub-records (PredictionPayload, BatchPayload, MonitoringPayload, TrainingPayload)
are nested inline. TrainingPayload includes a nested TrainingMetric array record.

Usage:
  # GCP (uses ADC automatically):
  python scripts/register_schemas.py \\
    --registry_url https://managedkafka.googleapis.com/v1/projects/P/locations/L/schemaRegistries/R

  # Local Docker:
  python scripts/register_schemas.py --registry_url http://localhost:8081
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)

SCHEMAS_DIR = Path(__file__).parent.parent / "schemas"

# Schemas to register — order matters only when there are cross-references.
# Both schemas here are self-contained so order doesn't matter, but we register
# prediction first to match the subject naming convention.
SCHEMA_PLAN = [
    (
        "com.bumble.avro.ml.vertex.VertexPredictionLog",
        "vertex_prediction_log.avsc",
        [],  # self-contained — PredictionPayload is inline
    ),
    (
        "com.bumble.avro.ml.vertex.VertexBatchLog",
        "vertex_batch_log.avsc",
        [],  # self-contained — BatchPayload is inline
    ),
    (
        "com.bumble.avro.ml.vertex.VertexMonitoringLog",
        "vertex_monitoring_log.avsc",
        [],  # self-contained — MonitoringPayload is inline
    ),
    (
        "com.bumble.avro.ml.vertex.VertexTrainingLog",
        "vertex_training_log.avsc",
        [],  # self-contained — TrainingPayload + TrainingMetric are inline
    ),
]


def _get_headers(registry_url: str) -> dict:
    """Return auth headers — Google ADC for GCP, plain for localhost."""
    base = registry_url.lower()
    is_local = any(m in base for m in ("localhost", "127.0.0.1", "0.0.0.0"))
    if is_local:
        return {"Content-Type": "application/vnd.schemaregistry.v1+json"}

    import google.auth
    import google.auth.transport.requests

    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    creds.refresh(google.auth.transport.requests.Request())
    return {
        "Authorization": f"Bearer {creds.token}",
        "Content-Type": "application/vnd.schemaregistry.v1+json",
    }


def _strip_x_meta(schema_dict: dict) -> dict:
    """Remove x- extension keys before registering (some registries reject them)."""
    return {k: v for k, v in schema_dict.items() if not k.startswith("x-")}


def check_schema_exists(
    registry_url: str,
    subject: str,
    headers: dict,
) -> tuple[bool, int]:
    """
    Check whether a subject already exists in the Schema Registry.

    Returns (exists: bool, latest_schema_id: int).
    Does NOT register anything.
    """
    import requests

    url = f"{registry_url.rstrip('/')}/subjects/{subject}/versions/latest"
    resp = requests.get(url, headers=headers, timeout=15)

    if resp.status_code == 200:
        schema_id = resp.json().get("id", -1)
        return True, schema_id
    elif resp.status_code == 404:
        return False, -1
    else:
        logger.error(
            "✗ Error checking subject %s [%d]: %s",
            subject,
            resp.status_code,
            resp.text,
        )
        sys.exit(1)


def register_schema(
    registry_url: str,
    subject: str,
    schema_path: Path,
    references: list,
    headers: dict,
    dry_run: bool = False,
    strip_x_meta: bool = True,
    missing_only: bool = False,
) -> int:
    """
    Register (or update) a schema subject.

    Args:
        missing_only: If True, skip registration when the subject already
                      exists (provision-if-missing semantics).  Useful in
                      test environments where some schemas may have been
                      deployed by a PR pipeline while others are new.
    """
    import requests

    # ── Provision-if-missing: skip subjects that already exist ────────────────
    if missing_only and not dry_run:
        exists, existing_id = check_schema_exists(registry_url, subject, headers)
        if exists:
            logger.info(
                "↷ Already exists (skipping):  subject=%-55s  id=%s",
                subject,
                existing_id,
            )
            return existing_id

    raw_str = schema_path.read_text()

    # Validate JSON
    try:
        schema_dict = json.loads(raw_str)
    except json.JSONDecodeError as exc:
        logger.error("Invalid JSON in %s: %s", schema_path.name, exc)
        sys.exit(1)

    if strip_x_meta:
        schema_dict = _strip_x_meta(schema_dict)

    schema_str = json.dumps(schema_dict)

    payload = {
        "schema": schema_str,
        "schemaType": "AVRO",
    }
    if references:
        payload["references"] = references

    if dry_run:
        logger.info(
            "[DRY-RUN] Would register subject=%s from %s", subject, schema_path.name
        )
        return -1

    url = f"{registry_url.rstrip('/')}/subjects/{subject}/versions"
    resp = requests.post(url, json=payload, headers=headers, timeout=30)

    if resp.status_code in (200, 201):
        schema_id = resp.json().get("id", "?")
        logger.info("✓ Registered  subject=%-60s  id=%s", subject, schema_id)
        return schema_id
    elif resp.status_code == 409:
        logger.info("~ Already up-to-date: %s", subject)
        check = requests.get(
            f"{registry_url.rstrip('/')}/subjects/{subject}/versions/latest",
            headers=headers,
            timeout=15,
        )
        if check.status_code == 200:
            return check.json().get("id", -1)
        return -1
    else:
        logger.error(
            "✗ Failed to register %s [%d]: %s",
            subject,
            resp.status_code,
            resp.text,
        )
        sys.exit(1)


def check_all_schemas(registry_url: str, headers: dict) -> bool:
    """
    Verify that all subjects in SCHEMA_PLAN exist in the registry.

    Returns True if all exist, False (+ non-zero exit) if any are missing.
    Used by CI pipelines that pre-provision schemas via PR: confirm they
    landed before starting Dataflow.
    """
    logger.info("Checking all required subjects in: %s", registry_url)
    logger.info("")

    all_ok = True
    for subject, filename, _ in SCHEMA_PLAN:
        exists, schema_id = check_schema_exists(registry_url, subject, headers)
        if exists:
            logger.info("✓ Found    subject=%-60s  id=%s", subject, schema_id)
        else:
            logger.error("✗ Missing  subject=%s", subject)
            all_ok = False

    logger.info("")
    if all_ok:
        logger.info("All required schemas are registered.")
    else:
        logger.error(
            "One or more schemas are missing. "
            "Run without --check to register them, or check the PR pipeline."
        )
        sys.exit(1)

    return all_ok


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Register Avro schemas in Schema Registry.\n\n"
            "Modes:\n"
            "  (default)          Register / update all schemas (idempotent).\n"
            "  --missing-only     Provision only subjects that do not yet exist.\n"
            "                     Safe to run when schemas may already be deployed\n"
            "                     via a PR pipeline; newly-added schemas are added.\n"
            "  --check            Verify all subjects exist; exit non-zero if any\n"
            "                     are missing.  Does NOT register anything.\n"
            "  --dry-run          Print what would be registered without doing it.\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--registry_url", required=True, help="Schema Registry base URL."
    )
    parser.add_argument(
        "--schemas_dir",
        default=str(SCHEMAS_DIR),
        help=f"Directory containing .avsc files (default: {SCHEMAS_DIR}).",
    )

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry_run",
        "--dry-run",
        action="store_true",
        help="Print what would be registered without actually registering.",
    )
    mode.add_argument(
        "--missing_only",
        "--missing-only",
        action="store_true",
        help=(
            "Only register subjects that are not yet in the registry. "
            "Subjects that already exist are left untouched. "
            "Use in test environments alongside a PR-based schema pipeline."
        ),
    )
    mode.add_argument(
        "--check",
        action="store_true",
        help=(
            "Verify all required subjects exist. Exit 0 if all present, "
            "non-zero if any are missing. Nothing is registered."
        ),
    )

    parser.add_argument(
        "--keep_x_meta",
        "--keep-x-meta",
        action="store_true",
        help="Keep x-meta extension fields in the registered schema (default: strip them).",
    )
    args = parser.parse_args()

    schemas_dir = Path(args.schemas_dir)
    if not schemas_dir.exists():
        logger.error("Schemas directory not found: %s", schemas_dir)
        sys.exit(1)

    logger.info("Schema Registry: %s", args.registry_url)
    if not args.check:
        logger.info("Schemas dir:     %s", schemas_dir)
    if args.missing_only:
        logger.info("Mode:            provision-if-missing (existing subjects skipped)")
    elif args.check:
        logger.info("Mode:            check-only (nothing will be registered)")
    elif args.dry_run:
        logger.info("Mode:            dry-run")
    logger.info("")

    headers = _get_headers(args.registry_url)

    # ── Check-only mode ───────────────────────────────────────────────────────
    if args.check:
        check_all_schemas(registry_url=args.registry_url, headers=headers)
        return

    # ── Register / provision mode ─────────────────────────────────────────────
    registered = {}
    for subject, filename, references in SCHEMA_PLAN:
        schema_path = schemas_dir / filename
        if not schema_path.exists():
            logger.error("Schema file not found: %s", schema_path)
            sys.exit(1)

        schema_id = register_schema(
            registry_url=args.registry_url,
            subject=subject,
            schema_path=schema_path,
            references=references,
            headers=headers,
            dry_run=args.dry_run,
            strip_x_meta=not args.keep_x_meta,
            missing_only=args.missing_only,
        )
        registered[subject] = schema_id

    logger.info("")
    logger.info("Done.")
    for subject, sid in registered.items():
        logger.info("  %-60s  id=%s", subject, sid)


if __name__ == "__main__":
    main()
