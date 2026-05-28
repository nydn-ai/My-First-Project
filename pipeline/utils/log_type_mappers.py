"""
Cloud Logging entry → Avro record mappers.

Schema routing:
  online_prediction  →  VertexPredictionLog  (PredictionPayload)
  batch_prediction   →  VertexBatchLog       (BatchPayload)
  monitoring         →  VertexMonitoringLog  (MonitoringPayload)
  training           →  VertexTrainingLog    (TrainingPayload)

Each mapper produces a flat dict whose keys match the target Avro schema.
The nested payload sub-record is built by the respective _build_*_payload()
helper and keyed as "payload".
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# Nanosecond timestamp regex (Cloud Logging uses 9-digit fractional seconds)
_NS_RE = re.compile(r"(\.\d{6})\d+(Z|[+-]\d{2}:\d{2})$")

# Monitoring alert message patterns — parse metric value and threshold from
# terse_message: "... is below the threshold of 1.000 with a value of 0.000."
# Uses \d+(?:\.\d+)? instead of [\d.]+ so a trailing period is NOT consumed.
_THRESHOLD_RE = re.compile(r"threshold of (\d+(?:\.\d+)?)")
_METRIC_VALUE_RE = re.compile(r"value of (\d+(?:\.\d+)?)")

# ──────────────────────────────────────────────────────────────────────────────
# Subject constants
# ──────────────────────────────────────────────────────────────────────────────

SUBJECT_PREDICTION = "com.bumble.avro.ml.vertex.VertexPredictionLog"
SUBJECT_BATCH = "com.bumble.avro.ml.vertex.VertexBatchLog"
SUBJECT_MONITORING = "com.bumble.avro.ml.vertex.VertexMonitoringLog"
SUBJECT_TRAINING = "com.bumble.avro.ml.vertex.VertexTrainingLog"

# Per log-type routing
_LOG_TYPE_SUBJECTS: Dict[str, str] = {
    "online_prediction": SUBJECT_PREDICTION,
    "batch_prediction": SUBJECT_BATCH,
    "monitoring": SUBJECT_MONITORING,
    "training": SUBJECT_TRAINING,
}


# ──────────────────────────────────────────────────────────────────────────────
# Shared top-level field builder
# ──────────────────────────────────────────────────────────────────────────────


def _build_top_level(entry: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the flat top-level fields shared by all four log schemas, then
    merge in the pre-built payload dict.

    Handles two Cloud Logging entry formats:
      - Audit log:      protoPayload, resource.labels.project_id
      - Container log:  jsonPayload.message, resource.labels.resource_container
                        top-level labels (deployed_model_id, replica_id)
    """
    resource = entry.get("resource") or {}
    resource_labels = resource.get("labels") or {}
    proto = entry.get("protoPayload") or {}
    json_payload = entry.get("jsonPayload") or {}

    ts_str = entry.get("timestamp") or datetime.now(timezone.utc).isoformat()

    # raw_payload_json: prefer protoPayload/jsonPayload; fall back to top-level
    # labels for entries like Cloud Monitoring alerts that carry all data there.
    top_labels = entry.get("labels") or {}
    raw_src = proto or json_payload or (top_labels if top_labels else None)
    raw_json = json.dumps(raw_src, default=str) if raw_src else None

    # project_id extraction — three formats seen in production:
    #   audit logs:         resource.labels.project_id = "project-abc123"
    #   container logs:     resource.labels.resource_container = "projects/817343037939"
    #   monitoring alerts:  resource.labels.resource_container = "817343037939"  (bare number)
    resource_container = resource_labels.get("resource_container", "")
    project_id = (
        resource_labels.get("project_id")
        or (
            # strip "projects/" prefix if present, otherwise use as-is
            resource_container.split("/")[-1]
            if resource_container
            else None
        )
        or _deep_get(entry, ["resource", "labels", "project_id"])
    )

    return {
        "id": str(uuid.uuid4()),
        "ts": _parse_ts_millis(ts_str),
        "insert_time": int(datetime.now(timezone.utc).timestamp() * 1000),
        "event_time": _parse_ts_millis(ts_str),
        "log_name": entry.get("logName", ""),
        "resource_type": resource.get("type", ""),
        "resource_labels": {str(k): str(v) for k, v in resource_labels.items()},
        "project_id": project_id,
        "location": (resource_labels.get("location") or resource_labels.get("region")),
        "endpoint_id": _extract_endpoint_id(entry),
        "model_id": _extract_model_id(entry),
        "model_version": _extract_model_version(entry),
        "severity": entry.get("severity", "DEFAULT"),
        "trace": entry.get("trace"),
        "span_id": entry.get("spanId"),
        "insert_id": entry.get("insertId"),
        "payload": payload,
        "raw_payload_json": raw_json,
    }


# ──────────────────────────────────────────────────────────────────────────────
# VertexPredictionLog mapper
# ──────────────────────────────────────────────────────────────────────────────


class VertexPredictionLogMapper:
    """
    Maps any prediction-type Cloud Logging entry (online, batch, training)
    to a VertexPredictionLog dict with a PredictionPayload sub-record.
    """

    def map(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        proto = entry.get("protoPayload") or {}
        json_payload = entry.get("jsonPayload") or {}
        http_req = entry.get("httpRequest") or {}
        payload = _build_prediction_payload(entry, proto, json_payload, http_req)
        return _build_top_level(entry, payload)


def _build_prediction_payload(
    entry: Dict, proto: Dict, json_payload: Dict, http_req: Dict
) -> Dict:
    resp = proto.get("response") or {}
    req = proto.get("request") or {}
    status = proto.get("status") or {}

    # Top-level labels carry deployed_model_id in container logs
    # (audit logs have it in protoPayload.response.deployedModelId)
    top_labels = entry.get("labels") or {}

    # "google.cloud.aiplatform.v1.PredictionService.Predict" → "Predict"
    method_full = proto.get("methodName") or ""
    method = method_full.split(".")[-1] if method_full else None

    latency_raw = (
        proto.get("latencyMs") or proto.get("latency") or json_payload.get("latency_ms")
    )
    latency_ms = _to_long(_strip_unit_suffix(latency_raw, "ms", "s"))

    status_code = _to_int(
        http_req.get("status") or status.get("code") or json_payload.get("status_code")
    )

    return {
        "method": method,
        "deployed_model_id": _to_str(
            # container logs: top-level labels.deployed_model_id
            top_labels.get("deployed_model_id")
            # audit logs: protoPayload.response.deployedModelId
            or resp.get("deployedModelId")
            or resp.get("deployed_model_id")
            or json_payload.get("deployed_model_id")
        ),
        "latency_ms": latency_ms,
        "status_code": status_code,
        "request_size_bytes": _to_long(
            http_req.get("requestSize") or json_payload.get("request_size_bytes")
        ),
        "response_size_bytes": _to_long(
            http_req.get("responseSize") or json_payload.get("response_size_bytes")
        ),
        "request_payload_json": json.dumps(req, default=str) if req else None,
        "response_payload_json": json.dumps(resp, default=str) if resp else None,
        "error_message": _to_str(
            status.get("message")
            or json_payload.get("error_message")
            or json_payload.get("error")
        ),
    }


# ──────────────────────────────────────────────────────────────────────────────
# VertexBatchLog mapper
# ──────────────────────────────────────────────────────────────────────────────


class VertexBatchLogMapper:
    """
    Maps a Vertex AI batch prediction Cloud Logging entry to a VertexBatchLog
    dict with a BatchPayload sub-record.

    Handles job lifecycle audit log entries from protoPayload, extracting
    job id, state, input/output URIs, instance counts, and timestamps.
    """

    def map(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        proto = entry.get("protoPayload") or {}
        json_payload = entry.get("jsonPayload") or {}
        payload = _build_batch_payload(entry, proto, json_payload)
        return _build_top_level(entry, payload)


def _build_batch_payload(entry: Dict, proto: Dict, json_payload: Dict) -> Dict:
    resp = proto.get("response") or {}
    req = proto.get("request") or {}
    resource_name = proto.get("resourceName") or ""

    # Job ID from resourceName
    job_id = None
    if "/batchPredictionJobs/" in resource_name:
        job_id = resource_name.split("/batchPredictionJobs/")[-1].split("/")[0]
    job_id = (
        job_id
        or json_payload.get("batch_job_id")
        or _to_str(resp.get("name", "").split("/")[-1] or None)
    )

    job_name = (
        resp.get("displayName")
        or req.get("displayName")
        or json_payload.get("batch_job_name")
    )
    job_state = resp.get("state") or req.get("state") or json_payload.get("job_state")

    # Input config
    input_uri = (
        _deep_get(req, ["inputConfig", "gcsSource", "uris", 0])
        or _deep_get(resp, ["inputConfig", "gcsSource", "uris", 0])
        or json_payload.get("input_uri")
    )
    input_format = (
        _deep_get(req, ["inputConfig", "instancesFormat"])
        or _deep_get(resp, ["inputConfig", "instancesFormat"])
        or json_payload.get("input_format")
    )

    # Output config
    output_uri = (
        _deep_get(req, ["outputConfig", "gcsDestination", "outputUriPrefix"])
        or _deep_get(resp, ["outputConfig", "gcsDestination", "outputUriPrefix"])
        or json_payload.get("output_uri")
    )
    output_format = (
        _deep_get(req, ["outputConfig", "predictionsFormat"])
        or _deep_get(resp, ["outputConfig", "predictionsFormat"])
        or json_payload.get("output_format")
    )

    # Counts
    output_info = resp.get("outputInfo") or {}
    instance_count = _to_long(
        output_info.get("bigqueryOutputDataset")  # proxy if present
        or json_payload.get("instance_count")
    )
    success_count = _to_long(json_payload.get("success_count"))
    failure_count = _to_long(
        resp.get("partialFailures")
        and len(resp.get("partialFailures", []))
        or json_payload.get("failure_count")
    )

    # Timestamps
    create_time = _to_ts_millis_or_none(
        resp.get("createTime") or json_payload.get("create_time")
    )
    start_time = _to_ts_millis_or_none(
        resp.get("startTime") or json_payload.get("start_time")
    )
    end_time = _to_ts_millis_or_none(
        resp.get("endTime") or json_payload.get("end_time")
    )

    # Error
    status = proto.get("status") or resp.get("error") or {}
    error_code = _to_str(status.get("code") or json_payload.get("error_code"))
    error_message = _to_str(status.get("message") or json_payload.get("error_message"))

    return {
        "batch_job_id": _to_str(job_id),
        "batch_job_name": _to_str(job_name),
        "job_state": _to_str(job_state),
        "input_uri": _to_str(input_uri),
        "input_format": _to_str(input_format),
        "output_uri": _to_str(output_uri),
        "output_format": _to_str(output_format),
        "instance_count": instance_count,
        "success_count": success_count,
        "failure_count": failure_count,
        "create_time": create_time,
        "start_time": start_time,
        "end_time": end_time,
        "error_code": error_code,
        "error_message": error_message,
    }


# ──────────────────────────────────────────────────────────────────────────────
# VertexMonitoringLog mapper
# ──────────────────────────────────────────────────────────────────────────────


class VertexMonitoringLogMapper:
    """
    Maps a Vertex AI monitoring Cloud Logging entry to a VertexMonitoringLog
    dict with a MonitoringPayload sub-record.

    Handles two source formats:
      - protoPayload  (audit log for monitoring job lifecycle events)
      - jsonPayload   (monitoring alert / metric result events)
    """

    def map(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        proto = entry.get("protoPayload") or {}
        json_payload = entry.get("jsonPayload") or {}
        payload = _build_monitoring_payload(entry, proto, json_payload)
        return _build_top_level(entry, payload)


def _build_monitoring_payload(entry: Dict, proto: Dict, json_payload: Dict) -> Dict:
    resp = proto.get("response") or {}
    req = proto.get("request") or {}
    resource_name = proto.get("resourceName") or ""

    # Top-level labels — present in Cloud Monitoring violation events
    # (monitoring.googleapis.com/ViolationOpenEventv1 and siblings).
    # These entries carry NO protoPayload / jsonPayload; all fields are here.
    top_labels = entry.get("labels") or {}

    # ── Monitoring job / policy identity ──────────────────────────────────────
    # ML monitoring jobs:  resourceName → /modelDeploymentMonitoringJobs/<id>
    # Infra alert events:  labels.policy_id
    job_id = None
    if "/modelDeploymentMonitoringJobs/" in resource_name:
        job_id = resource_name.split("/modelDeploymentMonitoringJobs/")[-1].split("/")[
            0
        ]
    job_id = (
        job_id
        or json_payload.get("monitoring_job_id")
        or top_labels.get("policy_id")
        or _to_str(resp.get("name", "").split("/")[-1] or None)
    )

    job_name = (
        resp.get("displayName")
        or req.get("displayName")
        or json_payload.get("monitoring_job_name")
        or top_labels.get("policy_display_name")
    )

    # ── Monitor / objective type ───────────────────────────────────────────────
    # Violation events: labels.activity_type_name = "ViolationOpenEventv1"
    monitor_type = (
        json_payload.get("monitor_type")
        or json_payload.get("alert_type")
        or json_payload.get("monitorType")
        or top_labels.get("activity_type_name")
    )
    objective_type = (
        json_payload.get("objective_type") or json_payload.get("objectiveType") or None
    )

    # ── Metric / threshold — parsed from terse_message if not explicit ─────────
    # Real format: "... is below the threshold of 1.000 with a value of 0.000."
    terse_msg = top_labels.get("terse_message") or ""

    feature_name = json_payload.get("feature_name") or json_payload.get("featureName")
    metric_name = (
        json_payload.get("metric_name")
        or json_payload.get("metricName")
        or _extract_metric_name_from_policy(top_labels.get("policy_display_name") or "")
    )

    # Try to parse metric_value from terse_message, then fall back to jsonPayload
    metric_value = _to_double(
        json_payload.get("metric_value")
        or json_payload.get("drift_score")
        or json_payload.get("skew_score")
        or _re_first(_METRIC_VALUE_RE, terse_msg)
    )
    threshold_value = _to_double(
        json_payload.get("threshold_value")
        or json_payload.get("threshold")
        or _re_first(_THRESHOLD_RE, terse_msg)
    )

    # Violation open/close events are by definition alert conditions
    alert_triggered = _to_bool(
        json_payload.get("alert_triggered")
        or json_payload.get("drift_detected")
        or json_payload.get("alertTriggered")
        or (
            True
            if top_labels.get("activity_type_name") == "ViolationOpenEventv1"
            else None
        )
    )

    # ── Dataset URIs ───────────────────────────────────────────────────────────
    baseline_uri = json_payload.get("baseline_dataset_uri") or _deep_get(
        req,
        ["modelMonitoringObjectiveConfig", "trainingDataset", "gcsSource", "uris", 0],
    )
    target_uri = json_payload.get("target_dataset_uri")
    prediction_log_source = json_payload.get(
        "prediction_log_source"
    ) or json_payload.get("predictionLogSource")

    # ── Window timestamps ──────────────────────────────────────────────────────
    # Violation events: labels.started_at is a Unix epoch seconds string
    started_at_raw = top_labels.get("started_at")
    started_at_ms = None
    if started_at_raw:
        try:
            started_at_ms = int(float(started_at_raw)) * 1000
        except (TypeError, ValueError):
            started_at_ms = _to_ts_millis_or_none(started_at_raw)

    window_start = (
        _to_ts_millis_or_none(json_payload.get("window_start"))
        or started_at_ms
        or _to_ts_millis_or_none(_deep_get(resp, ["nextScheduleTime"]))
    )
    window_end = _to_ts_millis_or_none(
        json_payload.get("window_end")
    ) or _to_ts_millis_or_none(_deep_get(resp, ["updateTime"]))

    return {
        "monitoring_job_id": _to_str(job_id),
        "monitoring_job_name": _to_str(job_name),
        "monitor_type": _to_str(monitor_type),
        "objective_type": _to_str(objective_type),
        "feature_name": _to_str(feature_name),
        "metric_name": _to_str(metric_name),
        "metric_value": metric_value,
        "threshold_value": threshold_value,
        "alert_triggered": alert_triggered,
        "baseline_dataset_uri": _to_str(baseline_uri),
        "target_dataset_uri": _to_str(target_uri),
        "prediction_log_source": _to_str(prediction_log_source),
        "window_start": window_start,
        "window_end": window_end,
    }


# ──────────────────────────────────────────────────────────────────────────────
# VertexTrainingLog mapper
# ──────────────────────────────────────────────────────────────────────────────


class VertexTrainingLogMapper:
    """
    Maps a Vertex AI training Cloud Logging entry to a VertexTrainingLog dict
    with a TrainingPayload sub-record.

    Handles CustomJob, TrainingPipeline, and HyperparameterTuningJob audit log
    entries from protoPayload, plus supplementary jsonPayload metric events.
    """

    def map(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        proto = entry.get("protoPayload") or {}
        json_payload = entry.get("jsonPayload") or {}
        payload = _build_training_payload(entry, proto, json_payload)
        return _build_top_level(entry, payload)


def _build_training_payload(entry: Dict, proto: Dict, json_payload: Dict) -> Dict:
    resp = proto.get("response") or {}
    req = proto.get("request") or {}
    resource_name = proto.get("resourceName") or ""

    # ── Job ID — from resource name path ──────────────────────────────────────
    job_id = None
    for segment in (
        "/trainingPipelines/",
        "/customJobs/",
        "/hyperparameterTuningJobs/",
    ):
        if segment in resource_name:
            job_id = resource_name.split(segment)[-1].split("/")[0]
            break
    job_id = job_id or json_payload.get("training_job_id")

    job_name = (
        resp.get("displayName")
        or req.get("displayName")
        or json_payload.get("training_job_name")
    )
    job_state = resp.get("state") or req.get("state") or json_payload.get("job_state")

    # ── Worker pool config — first spec is the master/chief ───────────────────
    # CustomJob: workerPoolSpecs[]
    # TrainingPipeline: trainingTaskInputs.workerPoolSpecs[]
    worker_specs = (
        req.get("workerPoolSpecs")
        or resp.get("workerPoolSpecs")
        or _deep_get(req, ["trainingTaskInputs", "workerPoolSpecs"])
        or _deep_get(resp, ["trainingTaskInputs", "workerPoolSpecs"])
        or []
    )
    first_spec = worker_specs[0] if worker_specs else {}
    machine_spec = first_spec.get("machineSpec") or {}
    container_spec = first_spec.get("containerSpec") or {}

    machine_type = _to_str(
        machine_spec.get("machineType") or json_payload.get("machine_type")
    )
    accelerator_type = _to_str(
        machine_spec.get("acceleratorType") or json_payload.get("accelerator_type")
    )
    accelerator_count = _to_int(
        machine_spec.get("acceleratorCount") or json_payload.get("accelerator_count")
    )
    container_uri = _to_str(
        container_spec.get("imageUri") or json_payload.get("container_uri")
    )
    args_raw = container_spec.get("args") or json_payload.get("args")
    args = [str(a) for a in args_raw] if args_raw else None

    # ── Hyperparameters (stringify all values for the map<string,string>) ─────
    hp_raw = (
        req.get("hyperparameters")
        or resp.get("hyperparameters")
        or _deep_get(req, ["trainingTaskInputs", "hyperparameters"])
        or json_payload.get("hyperparameters")
    )
    hyperparameters: Optional[Dict[str, str]] = None
    if isinstance(hp_raw, dict) and hp_raw:
        hyperparameters = {str(k): str(v) for k, v in hp_raw.items()}

    # ── Worker pool identifier ────────────────────────────────────────────────
    worker_pool_id = _to_str(
        first_spec.get("workerPoolId")
        or first_spec.get("replicaCount")
        and "master"  # infer label
        or json_payload.get("worker_pool_id")
    )
    replica_type = _to_str(
        first_spec.get("replicaType")
        or ("MASTER" if first_spec else None)
        or json_payload.get("replica_type")
    )

    # ── Metrics (list of TrainingMetric records) ───────────────────────────────
    # jsonPayload may carry a flat metrics dict or a list
    metrics: Optional[List[Dict]] = None
    metrics_raw = json_payload.get("metrics")
    if isinstance(metrics_raw, list):
        metrics = []
        for m in metrics_raw:
            if isinstance(m, dict):
                val = m.get("metric_value") or m.get("value")
                metrics.append(
                    {
                        "metric_name": str(
                            m.get("metric_name") or m.get("name") or "unknown"
                        ),
                        "metric_value": (
                            float(val) if val is not None and _is_numeric(val) else None
                        ),
                        "metric_value_text": (
                            str(val)
                            if val is not None and not _is_numeric(val)
                            else None
                        ),
                    }
                )
    elif isinstance(metrics_raw, dict):
        metrics = [
            {
                "metric_name": str(k),
                "metric_value": float(v) if _is_numeric(v) else None,
                "metric_value_text": str(v) if not _is_numeric(v) else None,
            }
            for k, v in metrics_raw.items()
        ]

    # ── Artifact URIs ──────────────────────────────────────────────────────────
    artifact_uris: Optional[List[str]] = None
    model_output = _deep_get(resp, ["modelToUpload", "artifactUri"])
    if model_output:
        artifact_uris = [str(model_output)]
    elif json_payload.get("artifact_uris"):
        raw = json_payload["artifact_uris"]
        artifact_uris = [str(u) for u in (raw if isinstance(raw, list) else [raw])]

    # ── Error ─────────────────────────────────────────────────────────────────
    status = proto.get("status") or {}
    error_code = _to_str(status.get("code") or json_payload.get("error_code"))
    error_message = _to_str(status.get("message") or json_payload.get("error_message"))

    return {
        "training_job_id": _to_str(job_id),
        "training_job_name": _to_str(job_name),
        "job_state": _to_str(job_state),
        "worker_pool_id": worker_pool_id,
        "replica_type": replica_type,
        "machine_type": machine_type,
        "accelerator_type": accelerator_type,
        "accelerator_count": accelerator_count,
        "container_uri": container_uri,
        "args": args,
        "hyperparameters": hyperparameters,
        "metrics": metrics,
        "artifact_uris": artifact_uris,
        "error_code": error_code,
        "error_message": error_message,
    }


def _re_first(pattern: re.Pattern, text: str) -> Optional[str]:
    """Return the first capture group of a regex match, or None."""
    m = pattern.search(text)
    return m.group(1) if m else None


def _extract_metric_name_from_policy(policy_display_name: str) -> Optional[str]:
    """
    Heuristically derive a metric name from a Cloud Monitoring policy name.

    Examples:
      "Vertex endpoint wasp-live-staging - Active replicas zero (warning)"
      → "replica_count"
    """
    name_lower = policy_display_name.lower()
    if "replica" in name_lower:
        return "replica_count"
    if "cpu" in name_lower:
        return "cpu_utilization"
    if "memory" in name_lower or "mem" in name_lower:
        return "memory_utilization"
    if "latency" in name_lower:
        return "request_latency"
    if "error" in name_lower:
        return "error_rate"
    return None


def _is_numeric(val: Any) -> bool:
    try:
        float(val)
        return True
    except (TypeError, ValueError):
        return False


# ──────────────────────────────────────────────────────────────────────────────
# Registry / factory
# ──────────────────────────────────────────────────────────────────────────────

_PREDICTION_MAPPER = VertexPredictionLogMapper()
_BATCH_MAPPER = VertexBatchLogMapper()
_MONITORING_MAPPER = VertexMonitoringLogMapper()
_TRAINING_MAPPER = VertexTrainingLogMapper()

_MAPPER_REGISTRY = {
    "online_prediction": _PREDICTION_MAPPER,
    "batch_prediction": _BATCH_MAPPER,
    "monitoring": _MONITORING_MAPPER,
    "training": _TRAINING_MAPPER,
}

LOG_TYPES = tuple(_MAPPER_REGISTRY.keys())


def get_mapper(log_type: str):
    """Return the mapper instance for the given log type."""
    try:
        return _MAPPER_REGISTRY[log_type]
    except KeyError:
        raise ValueError(
            f"Unknown log_type '{log_type}'. Valid: {list(_MAPPER_REGISTRY)}"
        )


def get_subject(log_type: str) -> str:
    """Return the Schema Registry subject for the given log type."""
    try:
        return _LOG_TYPE_SUBJECTS[log_type]
    except KeyError:
        raise ValueError(
            f"Unknown log_type '{log_type}'. Valid: {list(_LOG_TYPE_SUBJECTS)}"
        )


# ──────────────────────────────────────────────────────────────────────────────
# Shared entity extractors
# ──────────────────────────────────────────────────────────────────────────────


def _extract_endpoint_id(entry: Dict) -> Optional[str]:
    resource_labels = _deep_get(entry, ["resource", "labels"]) or {}
    if resource_labels.get("endpoint_id"):
        return resource_labels["endpoint_id"]
    resource_name = _deep_get(entry, ["protoPayload", "resourceName"]) or ""
    if "/endpoints/" in resource_name:
        return resource_name.split("/endpoints/")[-1].split("/")[0]
    return None


def _extract_model_id(entry: Dict) -> Optional[str]:
    proto = entry.get("protoPayload") or {}
    resp = proto.get("response") or {}
    req = proto.get("request") or {}
    raw = (
        resp.get("model")
        or req.get("model")
        or (entry.get("jsonPayload") or {}).get("model_id")
    )
    if raw:
        s = str(raw)
        return s.split("/models/")[-1].split("/")[0] if "/models/" in s else s
    return None


def _extract_model_version(entry: Dict) -> Optional[str]:
    resp = (entry.get("protoPayload") or {}).get("response") or {}
    val = (
        resp.get("modelVersionId")
        or resp.get("model_version_id")
        or (entry.get("jsonPayload") or {}).get("model_version")
    )
    return str(val) if val else None


# ──────────────────────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────────────────────


def _parse_ts_millis(ts_str: str) -> int:
    s = _NS_RE.sub(r"\1\2", str(ts_str)).replace("Z", "+00:00")
    try:
        return int(datetime.fromisoformat(s).timestamp() * 1_000)
    except Exception:
        return int(datetime.now(timezone.utc).timestamp() * 1_000)


def _to_ts_millis_or_none(val: Any) -> Optional[int]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return int(val)
    try:
        return _parse_ts_millis(str(val))
    except Exception:
        return None


def _deep_get(obj: Any, path: List) -> Any:
    for key in path:
        if isinstance(obj, dict):
            obj = obj.get(key)
        elif isinstance(obj, (list, tuple)):
            try:
                obj = obj[int(key)]
            except (IndexError, ValueError, TypeError):
                return None
        else:
            return None
    return obj


def _to_str(val: Any) -> Optional[str]:
    return str(val) if val is not None else None


def _to_int(val: Any) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(float(str(val)))
    except (TypeError, ValueError):
        return None


def _to_long(val: Any) -> Optional[int]:
    return _to_int(val)


def _to_double(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _to_bool(val: Any) -> Optional[bool]:
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    return str(val).lower() in ("true", "1", "yes")


def _strip_unit_suffix(val: Any, *suffixes: str) -> Any:
    if not isinstance(val, str):
        return val
    for sfx in suffixes:
        if val.endswith(sfx):
            return val[: -len(sfx)].strip()
    return val
