terraform {
  required_version = ">= 1.7"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }

    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 5.0"
    }
  }
}

# Uncomment after the GCS state bucket is created (first run uses local state):
# backend "gcs" {
#   bucket = "project-1c03ae00-17f3-43f4-86a-tf-state-dev"
#   prefix = "vertex-ml-logs/dev"
# }

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}
