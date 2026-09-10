variable "localstack_endpoint" {
  description = "S3 API endpoint. From the host: http://localhost:4566, from inside compose: http://localstack:4566."
  type        = string
  default     = "http://localhost:4566"
}

variable "region" {
  description = "AWS region label used by the S3 client (LocalStack ignores the physical location)."
  type        = string
  default     = "eu-central-1"
}

variable "bucket_name" {
  description = "Lakehouse bucket holding the Iceberg warehouse and checkpoints."
  type        = string
  default     = "energy-lake"
}
