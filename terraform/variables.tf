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
  default     = "demo_dataset"
}

variable "bucket_name" {
  description = "the name of the bucket"
  default     = "acquired-clover-447309-c6-bucket"
}

variable "project" {
  description = "the name of the project"
  default     = "acquired-clover-447309-c6"

}

variable "region" {
  description = "the region of the project"
  default     = "us-central1"

}