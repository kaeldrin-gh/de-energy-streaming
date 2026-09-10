# Infrastructure for the lakehouse bucket.
#
# Locally this targets LocalStack (community edition), which speaks the real
# S3 API - so this file is genuine infrastructure-as-code, not a mock.
# To use a real AWS account: drop the `endpoints`/static keys block below and
# authenticate with the standard AWS credential chain. Everything else
# (bucket layout, Iceberg warehouse path, checkpoints) stays identical.

provider "aws" {
  region                      = var.region
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  s3_use_path_style           = true

  endpoints {
    s3 = var.localstack_endpoint
  }
}

resource "aws_s3_bucket" "lakehouse" {
  bucket        = var.bucket_name
  force_destroy = true

  tags = {
    project = "de-energy-streaming"
    layer   = "storage"
  }
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  versioning_configuration {
    status = "Enabled"
  }
}

output "bucket" {
  value       = aws_s3_bucket.lakehouse.bucket
  description = "Name of the lakehouse bucket (referenced as s3a://<bucket>/warehouse by Spark)."
}
