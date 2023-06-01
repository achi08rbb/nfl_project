locals {
  data_lake_bucket = "nfl-data-lake"
  dataproc_bucket  = "nfl-spark-staging"
}

variable "project" {
  description = "nfl-project-de"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "asia-east1"
  type        = string
}

# If you did not set up the `GOOGLE_APPLICATION_CREDENTIALS` variable, uncomment this
variable "credentials" {
  description = "path to the correct google service account"
  default = "~/.google/credentials/google_credentials.json"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
  default     = "nfl_data_all"
}

variable "DATAPROC_CLUSTER" {
  description = "Name of dataproc cluster for spark jobs"
  type        = string
  default     = "nfl-spark-cluster"
}

variable "service_account" {
  description = "Name of service account"
  type        = string
  default     = "nfl-user@nfl-project-de.iam.gserviceaccount.com"
}

# "projects/nfl-project-de/regions/europe-west6/subnetworks/default"


