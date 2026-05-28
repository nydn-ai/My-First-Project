output "worker_sa_email" {
  description = "Dataflow worker service account email."
  value       = google_service_account.df_worker.email
}

output "worker_sa_name" {
  description = "Dataflow worker service account resource name."
  value       = google_service_account.df_worker.name
}

output "ar_repository_url" {
  description = "Artifact Registry repository URL for Docker images."
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${var.ar_repo_name}"
}

output "template_bucket_name" {
  description = "GCS bucket name for Flex Template specs."
  value       = google_storage_bucket.df_templates.name
}

output "template_gcs_path" {
  description = "GCS path for the Flex Template spec JSON."
  value       = "gs://${google_storage_bucket.df_templates.name}/templates/pubsub-to-kafka-avro.json"
}

output "staging_bucket_name" {
  description = "GCS bucket name for Dataflow staging."
  value       = google_storage_bucket.df_staging.name
}

output "temp_bucket_name" {
  description = "GCS bucket name for Dataflow temp."
  value       = google_storage_bucket.df_temp.name
}

output "dlq_bucket_name" {
  description = "GCS bucket name for dead-letter messages."
  value       = google_storage_bucket.df_dlq.name
}

output "dataflow_job_name" {
  description = "Dataflow job name (if enable_dataflow_job = true)."
  value       = var.enable_dataflow_job ? google_dataflow_flex_template_job.vertex_ml_logs[0].name : "not deployed"
}

output "pubsub_topic" {
  description = "Pub/Sub ingest topic for Vertex ML log entries."
  value       = google_pubsub_topic.vertex_ml_logs.id
}

output "pubsub_df_subscription" {
  description = "Pub/Sub subscription for the Dataflow job."
  value       = google_pubsub_subscription.df_ingest.id
}

output "pubsub_debug_subscription" {
  description = "Pub/Sub debug pull subscription for manual inspection."
  value       = google_pubsub_subscription.debug_sub.id
}

output "log_sink_name" {
  description = "Cloud Logging sink that routes Vertex AI logs to Pub/Sub."
  value       = google_logging_project_sink.vertex_ml_logs.name
}

output "log_sink_writer_identity" {
  description = "Service account that the log sink uses to publish to Pub/Sub."
  value       = google_logging_project_sink.vertex_ml_logs.writer_identity
}

output "next_steps" {
  description = "Commands to run after terraform apply."
  value       = <<-EOT
    # 1. Build and push the container image:
    make docker-push

    # 2. Register Avro schemas:
    make register-schemas

    # 3. Stage the Flex Template:
    make stage-template

    # 4. Launch the Dataflow job manually:
    make run-dataflow-job LOG_TYPE=online_prediction

    # 5. Or re-apply Terraform with enable_dataflow_job=true:
    #    terraform apply -var="enable_dataflow_job=true" ...
  EOT
}
