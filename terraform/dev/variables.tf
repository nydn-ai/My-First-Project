variable "project_id" {
  description = "GCP project ID."
  type        = string
  default     = "project-1c03ae00-17f3-43f4-86a"
}

variable "region" {
  description = "GCP region."
  type        = string
  default     = "us-east1"
}

variable "ar_repo_name" {
  description = "Artifact Registry repository name for the pipeline Docker image."
  type        = string
  default     = "vertex-ml-logs"
}

variable "image_name" {
  description = "Docker image name inside the AR repository."
  type        = string
  default     = "pubsub-to-kafka-avro"
}

variable "template_bucket" {
  description = "GCS bucket name for Dataflow Flex Template specs."
  type        = string
  default     = "project-1c03ae00-17f3-43f4-86a-df-templates-dev"
}

variable "kafka_bootstrap_servers" {
  description = "Kafka bootstrap server(s) for the Dataflow job."
  type        = string
  default     = "bootstrap.dev-vertex-log-kafka.us-east1.managedkafka.project-1c03ae00-17f3-43f4-86a.cloud.goog:9092"
}

variable "kafka_topic" {
  description = "Destination Kafka topic."
  type        = string
  default     = "dev-enriched-vertex-logs"
}

variable "kafka_registry_url" {
  description = "Schema Registry base URL."
  type        = string
  default     = "https://managedkafka.googleapis.com/v1/projects/project-1c03ae00-17f3-43f4-86a/locations/us-east1/schemaRegistries/dev_schema_registry"
}

variable "pubsub_subscription" {
  description = "Full Pub/Sub subscription path for the Dataflow source."
  type        = string
  # vertex-ml-logs-df-sub is the primary Dataflow subscription.
  # debug-sub is for manual inspection and test verification only.
  default = "projects/project-1c03ae00-17f3-43f4-86a/subscriptions/vertex-ml-logs-df-sub"
}

variable "log_type" {
  description = "Log type for the Dataflow job: online_prediction | batch_prediction | monitoring | training."
  type        = string
  default     = "online_prediction"

  validation {
    condition     = contains(["online_prediction", "batch_prediction", "monitoring", "training"], var.log_type)
    error_message = "log_type must be one of: online_prediction, batch_prediction, monitoring, training."
  }
}

variable "dataflow_max_workers" {
  description = "Maximum number of Dataflow workers."
  type        = number
  default     = 3
}

variable "dataflow_machine_type" {
  description = "Dataflow worker machine type."
  type        = string
  default     = "n1-standard-2"
}

variable "enable_dataflow_job" {
  description = "Set to true to create the Dataflow job via Terraform. False = infrastructure only."
  type        = bool
  default     = false
}
