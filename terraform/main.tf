terraform {
  required_version = ">= 1.0"
  backend "local" {}
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project
  region = var.region
}

# there is pre-defined definition on the terraform website
resource "google_storage_bucket" "data-lake-bucket" {
  name = "${local.data_lake_bucket}_${var.project}"
  location = var.region

  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  force_destroy = true
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset
  project = var.project
  location = var.region
}

resource "google_bigquery_table" "external_table" {
  depends_on = [
    google_bigquery_dataset.dataset
  ]
  dataset_id = var.bq_dataset
  table_id   = var.table_name
  external_data_configuration {
    autodetect    = false
    source_uris   = ["gs://${google_storage_bucket.data-lake-bucket.name}/taxi_data/*"]
    source_format = "CSV"

    csv_options{
      quote = ""
      skip_leading_rows = 1
    }
  }

  schema = file("./schema/ny_trips_schema.json")
}

resource "google_bigquery_table" "external_zone_table" {
  depends_on = [
    google_bigquery_dataset.dataset
  ]
  dataset_id = var.bq_dataset
  table_id   = var.zone_table_name
  external_data_configuration {
    autodetect    = false
    source_uris   = ["gs://${google_storage_bucket.data-lake-bucket.name}/zone_data/*"]
    source_format = "CSV"

    csv_options{
      quote = ""
      skip_leading_rows = 1
    }
  }

  schema = file("./schema/zones_schema.json")
}
