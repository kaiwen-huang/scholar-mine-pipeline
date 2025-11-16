// Variables for a minimal single-node Dataproc cluster

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for Dataproc and GCS (e.g., us-central1)"
  type        = string
}

variable "zone" {
  description = "GCP zone for Dataproc VMs (e.g., us-central1-a)"
  type        = string
}

variable "cluster_name" {
  description = "Dataproc cluster name"
  type        = string
  default     = "scholarmine-dataproc"
}

variable "bucket_name" {
  description = "GCS bucket for staging/input/output"
  type        = string
}

variable "dataproc_image_version" {
  description = "Dataproc image version (check gcloud for available versions)"
  type        = string
  default     = "2.2-debian12"
}

variable "machine_type" {
  description = "Machine type for the master node"
  type        = string
  default     = "n2-standard-2"
}

variable "disk_size_gb" {
  description = "Boot disk size for the master node"
  type        = number
  default     = 50
}
