// Minimal Dataproc single-node cluster with a staging GCS bucket
// HDFS is enabled by default on Dataproc clusters.

terraform {
  required_version = ">= 1.4.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

resource "google_storage_bucket" "dataproc_bucket" {
  name                        = var.bucket_name
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}

resource "google_dataproc_cluster" "this" {
  name   = var.cluster_name
  region = var.region

  cluster_config {
    // This bucket is used by Dataproc to stage job jars/scripts and logs
    staging_bucket = google_storage_bucket.dataproc_bucket.name

    gce_cluster_config {
      zone = var.zone
      // Optionally add service_account / network tags here if needed
    }

    // Single-node cluster: 1 master, 0 workers
    master_config {
      num_instances = 1
      machine_type  = var.machine_type
      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = var.disk_size_gb
      }
    }

    worker_config {
      num_instances = 2
    }

    software_config {
      image_version = var.dataproc_image_version
      // You can add Hadoop/Spark properties here if required
    }
  }
}
