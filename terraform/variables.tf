variable "credentials" {
  description = "credentials for the login to the gcp"
  default     = "./sensetive_keys.json"
}

variable "location" {
  description = "the location of the project"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "the name of the dataset"
  default     = "ny_taxi_db"
}

variable "bucket_name" {
  description = "the name of the bucket"
  default     = "ny-taxi-project-448909-bucket"
}

variable "project" {
  description = "the name of the project"
  default     = "ny-taxi-project-448909"

}

variable "region" {
  description = "the region of the project"
  default     = "us-central1"

}