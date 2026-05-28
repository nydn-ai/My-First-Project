#!/usr/bin/env python3
"""
Generate test Cloud Logging entries and publish them to a Pub/Sub topic.

Simulates what a Cloud Logging log sink would send to Pub/Sub when
Vertex AI log entries matching the filter ``severity = "NOTICE"`` arrive.

The script:
  1. Finds the Pub/Sub topic that the given subscription is attached to.
  2. Builds a realistic Cloud Logging entry in protoPayload format.
  3. Publishes it as a Pub/Sub message (matching log-sink output format).

Usage:
  python scripts/generate_test_log.py \\
    --project_id project-1c03ae00-17f3-43f4-86a \\
    --subscription projects/project-1c03ae00-17f3-43f4-86a/subscriptions/debug-sub \\
    --log_type online_prediction \\
    --count 3

  # Also write directly to Cloud Logging (requires log sink to be configured):
  python scripts/generate_test_log.py ... --write_to_cloud_logging
"""
from __future__ import annotations

import argparse
import json
import logging
import random
import string
import sys
import uuid
from datetime import datetime, timedelta, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Sample data generators
# ──────────────────────────────────────────────────────────────────────────────


def _rand_id(length: int = 19) -> str:
    return "".join(random.choices(string.digits, k=length))


def _now_ns() -> str:
    """Return current UTC time as a nanosecond-precision ISO string."""
    now = datetime.now(timezone.utc)
    ns = random.randint(0, 999)
    return now.strftime(f"%Y-%m-%dT%H:%M:%S.%f{ns:03d}Z")


# Schema: com.bumble.avro.ml.vertex.VertexPredictionLog
#
# Two generator variants for online prediction:
#   _make_online_prediction_entry       — container log format (real production format)
#   _make_online_prediction_audit_entry — protoPayload audit log format (alternative)
#
# Real Cloud Logging sink entries from aiplatform.googleapis.com/prediction_container
# use jsonPayload.message, top-level labels (deployed_model_id, replica_id), and
# resource_container instead of project_id in resource.labels.


def _make_online_prediction_entry(project_id: str) -> dict:
    """
    Generates a container log entry matching the real format seen from
    aiplatform.googleapis.com/prediction_container.

    These are stdout/stderr lines from the prediction server container,
    captured by Cloud Logging and routed to Pub/Sub via a log sink.
    """
    endpoint_id = random.choice(
        [
            "wasp-candidate-staging",
            "falcon-ranker-v2",
            f"endpoint-{_rand_id(8)}",
        ]
    )
    deployed_model_id = str(random.randint(10**18, 10**19 - 1))
    replica_suffix = "".join(
        random.choices(string.ascii_lowercase + string.digits, k=5)
    )
    replica_id = f"predictor-resource-pool-{_rand_id(18)}-{replica_suffix}"

    # Simulate a variety of container log messages
    severity = random.choice(["INFO", "INFO", "INFO", "ERROR", "WARNING"])
    messages = {
        "INFO": [
            "[{ts}] [{pid}] [INFO] Booting worker with pid: {pid}",
            "[{ts}] [{pid}] [INFO] Worker exiting (pid: {pid})",
            "[{ts}] [{pid}] [INFO] Finished server process [{pid}]",
            "[{ts}] [{pid}] [INFO] Handling request for /v1/endpoints:predict",
        ],
        "ERROR": [
            "[{ts}] [{pid}] [ERROR] Exception in ASGI application",
            "[{ts}] [{pid}] [ERROR] Worker failed to start: CUDA out of memory",
            "[{ts}] [{pid}] [ERROR] Unhandled exception during prediction",
        ],
        "WARNING": [
            "[{ts}] [{pid}] [WARNING] High latency detected: {latency}ms",
            "[{ts}] [{pid}] [WARNING] Memory usage above 80%",
        ],
    }
    ts_simple = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S +0000")
    pid = random.randint(1, 20)
    latency = random.randint(200, 2000)
    msg_template = random.choice(messages[severity])
    message = msg_template.format(ts=ts_simple, pid=pid, latency=latency)

    now = _now_ns()
    receive_now = _now_ns()

    # Numeric project container reference (as seen in real logs)
    # Use a plausible GCP project number
    project_number = "817343037939"

    return {
        "insertId": str(uuid.uuid4()).replace("-", "")[:20],
        "jsonPayload": {
            "message": message,
        },
        "resource": {
            "type": "aiplatform.googleapis.com/Endpoint",
            "labels": {
                "endpoint_id": endpoint_id,
                "location": "us-east1",
                "resource_container": f"projects/{project_number}",
            },
        },
        "timestamp": now,
        "receiveTimestamp": receive_now,
        "severity": severity,
        # Top-level labels — populated by the Vertex AI platform
        "labels": {
            "deployed_model_id": deployed_model_id,
            "replica_id": replica_id,
        },
        "logName": (
            f"projects/{project_id}/logs/aiplatform.googleapis.com%2Fprediction_container"
        ),
    }


def _make_online_prediction_audit_entry(project_id: str) -> dict:
    """
    Generates a protoPayload audit log entry for a Predict RPC call.
    This is an alternative format; the pipeline mapper handles both.
    """
    endpoint_id = _rand_id()
    model_id = _rand_id()
    deployed_model_id = _rand_id()
    request_id = str(uuid.uuid4()).replace("-", "")[:16]
    now = _now_ns()
    receive_now = _now_ns()

    return {
        "protoPayload": {
            "@type": "type.googleapis.com/google.cloud.audit.AuditLog",
            "status": {},
            "authenticationInfo": {
                "principalEmail": f"service-account@{project_id}.iam.gserviceaccount.com"
            },
            "requestMetadata": {
                "callerIp": f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
                "callerSuppliedUserAgent": "python-requests/2.31.0",
            },
            "serviceName": "aiplatform.googleapis.com",
            "methodName": "google.cloud.aiplatform.v1.PredictionService.Predict",
            "resourceName": (
                f"projects/{project_id}/locations/us-east1/endpoints/{endpoint_id}"
            ),
            "requestId": request_id,
            "request": {
                "@type": "type.googleapis.com/google.cloud.aiplatform.v1.PredictRequest",
                "endpoint": (
                    f"projects/{project_id}/locations/us-east1/endpoints/{endpoint_id}"
                ),
                "instances": [{"values": [random.random() for _ in range(5)]}],
            },
            "response": {
                "@type": "type.googleapis.com/google.cloud.aiplatform.v1.PredictResponse",
                "predictions": [[random.random() for _ in range(3)]],
                "deployedModelId": deployed_model_id,
                "model": f"projects/{project_id}/models/{model_id}",
                "modelVersionId": str(random.randint(1, 5)),
                "modelDisplayName": "fraud-detection-v2",
                "predictionsCount": 1,
            },
            "latencyMs": str(random.randint(10, 500)),
        },
        "insertId": str(uuid.uuid4()),
        "httpRequest": {
            "requestMethod": "POST",
            "requestUrl": (
                f"https://us-east1-aiplatform.googleapis.com/v1/projects/{project_id}"
                f"/locations/us-east1/endpoints/{endpoint_id}:predict"
            ),
            "requestSize": str(random.randint(200, 2000)),
            "status": 200,
            "responseSize": str(random.randint(100, 5000)),
            "userAgent": "python-requests/2.31.0",
            "latency": f"{random.randint(10, 500)}ms",
        },
        "resource": {
            "type": "aiplatform.googleapis.com/Endpoint",
            "labels": {
                "endpoint_id": endpoint_id,
                "location": "us-east1",
                "project_id": project_id,
            },
        },
        "timestamp": now,
        "receiveTimestamp": receive_now,
        "severity": "NOTICE",
        "trace": f"projects/{project_id}/traces/{uuid.uuid4().hex}",
        "spanId": uuid.uuid4().hex[:16],
        "logName": f"projects/{project_id}/logs/cloudaudit.googleapis.com%2Factivity",
    }


def _make_batch_prediction_entry(project_id: str) -> dict:
    """Generates a VertexBatchLog-compatible Cloud Logging entry."""
    job_id = _rand_id()
    model_id = _rand_id()
    now = _now_ns()

    state = random.choice(
        [
            "JOB_STATE_SUCCEEDED",
            "JOB_STATE_RUNNING",
            "JOB_STATE_FAILED",
            "JOB_STATE_QUEUED",
        ]
    )
    input_uri = f"gs://{project_id}-data/input/batch_{job_id}.jsonl"
    output_uri = f"gs://{project_id}-data/output/{job_id}/"

    # Simulate timestamps: create → start → end (if terminal state)
    from datetime import timedelta

    create_dt = datetime.now(timezone.utc) - timedelta(hours=2)
    start_dt = create_dt + timedelta(minutes=2)
    end_dt = start_dt + timedelta(minutes=random.randint(10, 120))
    create_iso = create_dt.strftime("%Y-%m-%dT%H:%M:%S.000000Z")
    start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%S.000000Z")
    end_iso = (
        end_dt.strftime("%Y-%m-%dT%H:%M:%S.000000Z")
        if state in ("JOB_STATE_SUCCEEDED", "JOB_STATE_FAILED")
        else None
    )

    error_code = "8" if state == "JOB_STATE_FAILED" else None  # 8 = RESOURCE_EXHAUSTED
    error_message = (
        "Quota exceeded for batch prediction workers."
        if state == "JOB_STATE_FAILED"
        else None
    )

    return {
        "protoPayload": {
            "@type": "type.googleapis.com/google.cloud.audit.AuditLog",
            "status": {"code": int(error_code)} if error_code else {},
            "serviceName": "aiplatform.googleapis.com",
            "methodName": "google.cloud.aiplatform.v1.JobService.CreateBatchPredictionJob",
            "resourceName": (
                f"projects/{project_id}/locations/us-east1/batchPredictionJobs/{job_id}"
            ),
            "request": {
                "@type": "type.googleapis.com/google.cloud.aiplatform.v1.CreateBatchPredictionJobRequest",
                "displayName": f"batch-job-{job_id[:8]}",
                "model": f"projects/{project_id}/models/{model_id}",
                "inputConfig": {
                    "instancesFormat": "jsonl",
                    "gcsSource": {"uris": [input_uri]},
                },
                "outputConfig": {
                    "predictionsFormat": "jsonl",
                    "gcsDestination": {"outputUriPrefix": output_uri},
                },
            },
            "response": {
                "@type": "type.googleapis.com/google.cloud.aiplatform.v1.BatchPredictionJob",
                "name": f"projects/{project_id}/locations/us-east1/batchPredictionJobs/{job_id}",
                "displayName": f"batch-job-{job_id[:8]}",
                "model": f"projects/{project_id}/models/{model_id}",
                "state": state,
                "createTime": create_iso,
                "startTime": start_iso,
                **({"endTime": end_iso} if end_iso else {}),
                "inputConfig": {
                    "instancesFormat": "jsonl",
                    "gcsSource": {"uris": [input_uri]},
                },
                "outputConfig": {
                    "predictionsFormat": "jsonl",
                    "gcsDestination": {"outputUriPrefix": output_uri},
                },
                **(
                    {"error": {"code": int(error_code), "message": error_message}}
                    if error_code
                    else {}
                ),
            },
        },
        "insertId": str(uuid.uuid4()),
        "resource": {
            "type": "aiplatform.googleapis.com/BatchPredictionJob",
            "labels": {
                "job_id": job_id,
                "location": "us-east1",
                "project_id": project_id,
            },
        },
        "timestamp": now,
        "receiveTimestamp": now,
        "severity": "NOTICE",
        "logName": f"projects/{project_id}/logs/cloudaudit.googleapis.com%2Factivity",
    }


def _make_monitoring_entry(project_id: str) -> dict:
    """
    Generates a Cloud Monitoring violation event matching the real production
    format (monitoring.googleapis.com/ViolationOpenEventv1).

    These entries carry no protoPayload / jsonPayload — all signal is in the
    top-level labels dict.  severity is absent (defaults to DEFAULT).
    """
    endpoint_id = random.choice(
        [
            "wasp-live-staging",
            "falcon-ranker-v2",
            f"endpoint-{_rand_id(8)}",
        ]
    )
    policy_id = str(random.randint(10**18, 10**19 - 1))
    violation_id = (
        f"0.{''.join(random.choices(string.ascii_lowercase + string.digits, k=12))}"
    )
    project_number = "817343037939"
    region = "europe-west4"

    # Pick a violation scenario
    scenario = random.choice(
        [
            {
                "policy_display_name": f"Vertex endpoint {endpoint_id} - Active replicas zero (warning)",
                "metric": "replica_count",
                "value": 0.0,
                "threshold": 1.0,
                "condition": "below",
            },
            {
                "policy_display_name": f"Vertex endpoint {endpoint_id} - CPU utilization high",
                "metric": "cpu_utilization",
                "value": round(random.uniform(0.85, 0.99), 3),
                "threshold": 0.8,
                "condition": "above",
            },
            {
                "policy_display_name": f"Vertex endpoint {endpoint_id} - Request latency P99 high",
                "metric": "request_latency",
                "value": round(random.uniform(2.0, 10.0), 3),
                "threshold": 2.0,
                "condition": "above",
            },
        ]
    )

    resource_desc = (
        f"{project_id} Vertex AI Endpoint labels "
        f"{{project_id={project_id}, resource_container={project_number}, "
        f"location={region}, endpoint_id={endpoint_id}}}"
    )
    condition_phrase = (
        f"is below the threshold of {scenario['threshold']:.3f} "
        f"with a value of {scenario['value']:.3f}."
        if scenario["condition"] == "below"
        else f"is above the threshold of {scenario['threshold']:.3f} "
        f"with a value of {scenario['value']:.3f}."
    )
    terse_message = f"{scenario['metric'].replace('_', ' ').title()} for {resource_desc} {condition_phrase}"
    verbose_message = terse_message

    # started_at: Unix epoch seconds (integer as string)
    started_at = str(
        int(datetime.now(timezone.utc).timestamp()) - random.randint(60, 600)
    )

    # Timestamp without sub-second precision (as seen in real entries)
    now_simple = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    receive_now = _now_ns()

    return {
        "insertId": "".join(
            random.choices(string.ascii_lowercase + string.digits, k=20)
        ),
        "resource": {
            "type": "aiplatform.googleapis.com/Endpoint",
            "labels": {
                # bare numeric project number — no "projects/" prefix
                "resource_container": project_number,
                "endpoint_id": endpoint_id,
                "location": region,
            },
        },
        "timestamp": now_simple,
        "receiveTimestamp": receive_now,
        # No "severity" field — defaults to DEFAULT in Cloud Logging
        "labels": {
            "started_at": started_at,
            "violation_id": violation_id,
            "resource_name": resource_desc,
            "policy_id": policy_id,
            "terse_message": terse_message,
            "policy_display_name": scenario["policy_display_name"],
            "resource_id": "",
            "activity_type_name": "ViolationOpenEventv1",
            "verbose_message": verbose_message,
        },
        "logName": (
            f"projects/{project_id}/logs/monitoring.googleapis.com%2FViolationOpenEventv1"
        ),
    }


def _make_monitoring_ml_entry(project_id: str) -> dict:
    """
    Generates a Vertex AI model monitoring job entry (protoPayload + jsonPayload).
    This is the ML-specific format (feature drift / skew), distinct from the
    infra violation alerts produced by _make_monitoring_entry().
    Use --log_type monitoring_ml to generate this format.
    """
    job_id = _rand_id()
    endpoint_id = _rand_id()
    model_id = _rand_id()
    now = _now_ns()
    feature = random.choice(
        ["age", "income", "credit_score", "num_transactions", "session_duration"]
    )
    monitor_type = random.choice(["drift", "skew", "alert_evaluation"])
    metric_value = round(random.uniform(0.05, 0.95), 4)
    threshold = 0.3
    alert = metric_value > threshold

    window_end_dt = datetime.now(timezone.utc)
    window_start_dt = window_end_dt - timedelta(days=1)
    window_end_iso = window_end_dt.strftime("%Y-%m-%dT%H:%M:%S.000000Z")
    window_start_iso = window_start_dt.strftime("%Y-%m-%dT%H:%M:%S.000000Z")

    return {
        "protoPayload": {
            "@type": "type.googleapis.com/google.cloud.audit.AuditLog",
            "serviceName": "aiplatform.googleapis.com",
            "methodName": (
                "google.cloud.aiplatform.v1"
                ".ModelDeploymentMonitoringService.RunModelDeploymentMonitoringJob"
            ),
            "resourceName": (
                f"projects/{project_id}/locations/us-east1/modelDeploymentMonitoringJobs/{job_id}"
            ),
            "request": {
                "name": f"projects/{project_id}/locations/us-east1/modelDeploymentMonitoringJobs/{job_id}",
                "modelDeploymentMonitoringJob": {
                    "displayName": f"monitor-{endpoint_id[:8]}",
                    "endpoint": f"projects/{project_id}/locations/us-east1/endpoints/{endpoint_id}",
                    "modelMonitoringObjectiveConfig": {
                        "trainingDataset": {
                            "gcsSource": {
                                "uris": [
                                    f"gs://{project_id}-data/baseline/training_{model_id}.jsonl"
                                ]
                            }
                        }
                    },
                },
            },
            "response": {
                "name": f"projects/{project_id}/locations/us-east1/modelDeploymentMonitoringJobs/{job_id}",
                "displayName": f"monitor-{endpoint_id[:8]}",
                "state": "JOB_STATE_RUNNING",
                "updateTime": window_end_iso,
                "nextScheduleTime": window_end_iso,
            },
        },
        "jsonPayload": {
            "monitoring_job_id": job_id,
            "monitoring_job_name": f"monitor-{endpoint_id[:8]}",
            "monitor_type": monitor_type,
            "objective_type": (
                "training_prediction_skew"
                if monitor_type == "skew"
                else "prediction_drift"
            ),
            "feature_name": feature,
            "metric_name": f"{monitor_type}_{feature}",
            "metric_value": metric_value,
            "threshold_value": threshold,
            "alert_triggered": alert,
            "baseline_dataset_uri": f"gs://{project_id}-data/baseline/training_{model_id}.jsonl",
            "target_dataset_uri": f"gs://{project_id}-data/predictions/served_{endpoint_id}.jsonl",
            "prediction_log_source": f"projects/{project_id}/locations/us-east1/endpoints/{endpoint_id}",
            "window_start": window_start_iso,
            "window_end": window_end_iso,
        },
        "insertId": str(uuid.uuid4()),
        "resource": {
            "type": "aiplatform.googleapis.com/ModelDeploymentMonitoringJob",
            "labels": {
                "endpoint_id": endpoint_id,
                "location": "us-east1",
                "project_id": project_id,
                "job_id": job_id,
            },
        },
        "timestamp": now,
        "receiveTimestamp": now,
        "severity": "NOTICE",
        "logName": f"projects/{project_id}/logs/cloudaudit.googleapis.com%2Factivity",
    }


def _make_training_entry(project_id: str) -> dict:
    """Generates a VertexTrainingLog-compatible Cloud Logging entry."""
    job_id = _rand_id()
    now = _now_ns()
    framework = random.choice(["tensorflow", "pytorch", "xgboost", "sklearn"])
    state = random.choice(
        [
            "PIPELINE_STATE_RUNNING",
            "PIPELINE_STATE_SUCCEEDED",
            "PIPELINE_STATE_FAILED",
            "PIPELINE_STATE_PENDING",
        ]
    )
    machine = random.choice(["n1-standard-8", "n1-highmem-16", "a2-highgpu-1g"])
    accel = random.choice(
        ["NVIDIA_TESLA_T4", "NVIDIA_TESLA_V100", "NVIDIA_TESLA_A100", ""]
    )
    accel_count = random.randint(1, 4) if accel else 0

    hp = {
        "learning_rate": str(round(random.uniform(1e-5, 1e-2), 6)),
        "batch_size": str(random.choice([16, 32, 64, 128])),
        "epochs": str(random.randint(5, 100)),
        "dropout": str(round(random.uniform(0.1, 0.5), 2)),
    }

    metrics_list = [
        {
            "metric_name": "train_loss",
            "metric_value": round(random.uniform(0.05, 2.0), 4),
            "metric_value_text": None,
        },
        {
            "metric_name": "eval_accuracy",
            "metric_value": round(random.uniform(0.7, 0.99), 4),
            "metric_value_text": None,
        },
        {
            "metric_name": "eval_auc",
            "metric_value": round(random.uniform(0.8, 0.99), 4),
            "metric_value_text": None,
        },
    ]

    artifact_uri = f"gs://{project_id}-models/{job_id}/model/"
    is_terminal = state in ("PIPELINE_STATE_SUCCEEDED", "PIPELINE_STATE_FAILED")
    error_code = "2" if state == "PIPELINE_STATE_FAILED" else None
    error_msg = (
        "OOM error on worker replica 0." if state == "PIPELINE_STATE_FAILED" else None
    )

    from datetime import timedelta

    create_dt = datetime.now(timezone.utc) - timedelta(hours=3)
    create_iso = create_dt.strftime("%Y-%m-%dT%H:%M:%S.000000Z")

    return {
        "protoPayload": {
            "@type": "type.googleapis.com/google.cloud.audit.AuditLog",
            "status": (
                {"code": int(error_code), "message": error_msg} if error_code else {}
            ),
            "serviceName": "aiplatform.googleapis.com",
            "methodName": "google.cloud.aiplatform.v1.JobService.CreateCustomJob",
            "resourceName": (
                f"projects/{project_id}/locations/us-east1/customJobs/{job_id}"
            ),
            "request": {
                "@type": "type.googleapis.com/google.cloud.aiplatform.v1.CreateCustomJobRequest",
                "displayName": f"train-{framework}-{job_id[:8]}",
                "workerPoolSpecs": [
                    {
                        "replicaCount": 1,
                        "machineSpec": {
                            "machineType": machine,
                            **(
                                {
                                    "acceleratorType": accel,
                                    "acceleratorCount": accel_count,
                                }
                                if accel
                                else {}
                            ),
                        },
                        "containerSpec": {
                            "imageUri": (
                                f"us-east1-docker.pkg.dev/{project_id}"
                                f"/ml-training/{framework}:latest"
                            ),
                            "args": [
                                "--epochs",
                                hp["epochs"],
                                "--batch-size",
                                hp["batch_size"],
                                "--learning-rate",
                                hp["learning_rate"],
                            ],
                        },
                    }
                ],
                "hyperparameters": hp,
            },
            "response": {
                "@type": "type.googleapis.com/google.cloud.aiplatform.v1.CustomJob",
                "name": f"projects/{project_id}/locations/us-east1/customJobs/{job_id}",
                "displayName": f"train-{framework}-{job_id[:8]}",
                "state": state,
                "createTime": create_iso,
                **(
                    {"modelToUpload": {"artifactUri": artifact_uri}}
                    if state == "PIPELINE_STATE_SUCCEEDED"
                    else {}
                ),
                **(
                    {"error": {"code": int(error_code), "message": error_msg}}
                    if error_code
                    else {}
                ),
            },
        },
        # jsonPayload carries runtime metrics emitted by the training container
        "jsonPayload": {
            "training_job_id": job_id,
            "training_job_name": f"train-{framework}-{job_id[:8]}",
            "job_state": state,
            "machine_type": machine,
            "accelerator_type": accel or None,
            "accelerator_count": accel_count or None,
            "hyperparameters": hp,
            "metrics": metrics_list if is_terminal else [metrics_list[0]],
            "artifact_uris": (
                [artifact_uri] if state == "PIPELINE_STATE_SUCCEEDED" else None
            ),
            **(
                {"error_code": error_code, "error_message": error_msg}
                if error_code
                else {}
            ),
        },
        "insertId": str(uuid.uuid4()),
        "resource": {
            "type": "aiplatform.googleapis.com/CustomJob",
            "labels": {
                "custom_job_id": job_id,
                "location": "us-east1",
                "project_id": project_id,
            },
        },
        "timestamp": now,
        "receiveTimestamp": now,
        "severity": "NOTICE",
        "logName": f"projects/{project_id}/logs/cloudaudit.googleapis.com%2Factivity",
    }


_GENERATORS = {
    # Container log format — matches real production entries from
    # aiplatform.googleapis.com/prediction_container (jsonPayload.message,
    # top-level labels, resource_container).
    "online_prediction": _make_online_prediction_entry,
    # Audit log format — protoPayload PredictionService.Predict RPC.
    "online_prediction_audit": _make_online_prediction_audit_entry,
    "batch_prediction": _make_batch_prediction_entry,
    # Infra violation alert format — matches real production entries from
    # monitoring.googleapis.com/ViolationOpenEventv1 (no proto/json payload,
    # all data in top-level labels, bare numeric resource_container).
    "monitoring": _make_monitoring_entry,
    # ML model monitoring format — protoPayload + jsonPayload with drift/skew metrics.
    "monitoring_ml": _make_monitoring_ml_entry,
    "training": _make_training_entry,
}


# ──────────────────────────────────────────────────────────────────────────────
# Pub/Sub publisher
# ──────────────────────────────────────────────────────────────────────────────


def get_topic_for_subscription(subscription_path: str) -> str:
    """
    Look up the Pub/Sub topic that a subscription is attached to.
    Requires pubsub.subscriptions.get permission.
    """
    from google.cloud import pubsub_v1

    client = pubsub_v1.SubscriberClient()
    sub = client.get_subscription(subscription=subscription_path)
    return sub.topic


def publish_to_pubsub(
    topic_path: str,
    entries: list,
    project_id: str,
) -> None:
    from google.cloud import pubsub_v1

    publisher = pubsub_v1.PublisherClient()

    for entry in entries:
        data = json.dumps(entry, default=str).encode("utf-8")
        resource_type = entry.get("resource", {}).get("type", "")
        log_name = entry.get("logName", "")
        insert_id = entry.get("insertId", "")

        future = publisher.publish(
            topic_path,
            data=data,
            **{
                "logging.googleapis.com/logName": log_name,
                "logging.googleapis.com/insertId": insert_id,
                "logging.googleapis.com/timestamp": entry.get("timestamp", ""),
                "logging.googleapis.com/resource/type": resource_type,
                "logging.googleapis.com/resource/labels/project_id": project_id,
            },
        )
        msg_id = future.result(timeout=10)
        logger.info(
            "Published message %s to %s (insertId=%s)",
            msg_id,
            topic_path,
            insert_id[:8],
        )


# ──────────────────────────────────────────────────────────────────────────────
# Cloud Logging writer (optional — for full log-sink flow)
# ──────────────────────────────────────────────────────────────────────────────


def write_to_cloud_logging(entries: list, project_id: str) -> None:
    """
    Write entries directly to Cloud Logging.

    If a log sink is configured (e.g. filter on logName or resource type),
    Cloud Logging will automatically route these entries to the Pub/Sub topic,
    where the Dataflow pipeline will pick them up.

    IMPORTANT — audit log restriction:
        Cloud Logging only allows GCP services to write to audit log names
        (``cloudaudit.googleapis.com/activity``, etc.).  User code writing to
        those log names will receive PERMISSION_DENIED.

        Log types that use non-audit log names (writable by user code):
          - online_prediction   → aiplatform.googleapis.com/prediction_container ✅
          - monitoring          → monitoring.googleapis.com/ViolationOpenEventv1  ✅

        Log types that use the protected audit log name (unwritable by user code):
          - online_prediction_audit, batch_prediction, monitoring_ml, training
            → cloudaudit.googleapis.com/activity  ❌ real GCP events only

        For audit-log-based types, use ``generate-test-log-*`` to publish
        synthetic entries directly to Pub/Sub instead.
    """
    from google.api_core.exceptions import PermissionDenied
    from google.cloud import logging as gcloud_logging
    from google.cloud.logging_v2.resource import Resource

    # Audit log names that only GCP services are allowed to write to.
    _AUDIT_LOG_IDS = frozenset(
        {
            "cloudaudit.googleapis.com%2Factivity",
            "cloudaudit.googleapis.com/activity",
            "externalaudit.googleapis.com%2Factivity",
            "externalaudit.googleapis.com/activity",
        }
    )

    client = gcloud_logging.Client(project=project_id)
    skipped = 0

    for entry in entries:
        resource_info = entry.get("resource", {})
        raw_log_name = entry.get("logName", "")
        log_id = raw_log_name.split("/logs/")[-1]

        # Guard: skip audit log entries — user code cannot write them.
        if log_id in _AUDIT_LOG_IDS:
            logger.warning(
                "Skipping %s — audit logs can only be written by GCP services. "
                "Use 'make generate-test-log' to publish this type directly to Pub/Sub.",
                log_id,
            )
            skipped += 1
            continue

        resource = Resource(
            type=resource_info.get("type", "global"),
            labels=resource_info.get("labels", {}),
        )

        logger_cl = client.logger(log_id)

        # monitoring violation entries carry ALL signal in top-level labels
        # (no protoPayload/jsonPayload).  Pass empty struct but always forward labels.
        payload = entry.get("protoPayload") or entry.get("jsonPayload") or {}

        # Top-level entry labels (deployed_model_id, policy_id, endpoint_id, etc.)
        # CRITICAL for monitoring violation entries where labels carry all the data.
        entry_labels = entry.get("labels") or {}

        # Severity: monitoring violation entries have no severity field.
        # "DEFAULT" is the correct Cloud Logging value for "unspecified".
        severity = entry.get("severity", "DEFAULT")

        try:
            logger_cl.log_struct(
                payload,
                severity=severity,
                resource=resource,
                labels=entry_labels,
                insert_id=entry.get("insertId"),
            )
            logger.info(
                "Wrote to Cloud Logging: log=%s insert_id=%s severity=%s labels=%d",
                log_id,
                entry.get("insertId", "?")[:8],
                severity,
                len(entry_labels),
            )
        except PermissionDenied as exc:
            logger.error(
                "PERMISSION_DENIED writing to log '%s': %s\n"
                "Hint: this log ID is protected — only GCP services may write to it.\n"
                "Use 'make generate-test-log' to test this type via direct Pub/Sub publish.",
                log_id,
                exc,
            )
            skipped += 1

    if skipped:
        logger.warning(
            "%d entry/entries skipped (audit log restriction). "
            "See 'make generate-test-log-*' for those log types.",
            skipped,
        )


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Generate and publish test Vertex AI log entries."
    )
    parser.add_argument(
        "--project_id",
        default="project-1c03ae00-17f3-43f4-86a",
        help="GCP project ID.",
    )
    parser.add_argument(
        "--subscription",
        default="projects/project-1c03ae00-17f3-43f4-86a/subscriptions/debug-sub",
        help="Full Pub/Sub subscription path. Used to look up the topic.",
    )
    parser.add_argument(
        "--topic",
        default=None,
        help="Full Pub/Sub topic path. If not set, looked up from --subscription.",
    )
    parser.add_argument(
        "--log_type",
        default="online_prediction",
        choices=list(_GENERATORS.keys()),
        help=(
            "Type of log entry to generate. "
            "'online_prediction' emits the real container log format "
            "(jsonPayload.message + top-level labels). "
            "'online_prediction_audit' emits the protoPayload audit log format."
        ),
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="Number of entries to generate and publish.",
    )
    parser.add_argument(
        "--write_to_cloud_logging",
        action="store_true",
        help=(
            "Also write entries to Cloud Logging in addition to publishing to Pub/Sub. "
            "Requires a log sink that routes matching entries back to Pub/Sub. "
            "Use --cloud_logging_only to skip Pub/Sub and write only to Cloud Logging."
        ),
    )
    parser.add_argument(
        "--cloud_logging_only",
        action="store_true",
        help=(
            "Write entries ONLY to Cloud Logging — skip Pub/Sub publishing entirely. "
            "Use this to test the log sink end-to-end: the sink routes matching entries "
            "from Cloud Logging → Pub/Sub → Dataflow → Kafka automatically."
        ),
    )
    parser.add_argument(
        "--print_only",
        action="store_true",
        help="Print generated entries without publishing.",
    )
    args = parser.parse_args()

    generator = _GENERATORS[args.log_type]
    entries = [generator(args.project_id) for _ in range(args.count)]

    if args.print_only:
        for i, entry in enumerate(entries, 1):
            print(f"\n-- Entry {i}/{args.count} ({'-' * 50})")
            print(json.dumps(entry, indent=2))
        return

    # ── Cloud Logging only (log sink test) ────────────────────────────────────
    if args.cloud_logging_only:
        logger.info(
            "Writing %d %s entries ONLY to Cloud Logging (log sink test) ...",
            args.count,
            args.log_type,
        )
        write_to_cloud_logging(entries, args.project_id)
        logger.info("")
        logger.info("Done. %d entry/entries written to Cloud Logging.", len(entries))
        logger.info(
            "If the log sink filter matches, entries will be routed to Pub/Sub "
            "and picked up by the Dataflow pipeline within the window (default 10 s)."
        )
        return

    # ── Pub/Sub publish path ──────────────────────────────────────────────────

    # Resolve topic
    topic_path = args.topic
    if not topic_path:
        logger.info("Looking up topic for subscription %s ...", args.subscription)
        try:
            topic_path = get_topic_for_subscription(args.subscription)
            logger.info("Topic: %s", topic_path)
        except Exception as exc:
            logger.error("Cannot look up topic: %s", exc)
            logger.error("Pass --topic projects/PROJECT/topics/TOPIC_NAME directly.")
            sys.exit(1)

    logger.info(
        "Publishing %d %s entries to %s ...",
        args.count,
        args.log_type,
        topic_path,
    )
    publish_to_pubsub(topic_path, entries, args.project_id)

    if args.write_to_cloud_logging:
        logger.info("Also writing to Cloud Logging ...")
        write_to_cloud_logging(entries, args.project_id)

    logger.info("")
    logger.info("Done. %d message(s) published.", len(entries))
    logger.info(
        "The Dataflow pipeline should process them within the window "
        "(default 10 s). Check the Kafka consumer to verify."
    )


if __name__ == "__main__":
    main()
