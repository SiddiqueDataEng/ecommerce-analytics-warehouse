terraform {
  required_version = ">= 1.0"
  
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.80"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "snowflake" {
  account  = var.snowflake_account
  user     = var.snowflake_user
  password = var.snowflake_password
  role     = var.snowflake_role
}

provider "aws" {
  region = var.aws_region
}

# Snowflake Database
resource "snowflake_database" "ecommerce_analytics" {
  name    = var.database_name
  comment = "E-commerce analytics data warehouse"
  
  data_retention_time_in_days = 7
}

# Snowflake Schemas
resource "snowflake_schema" "staging" {
  database = snowflake_database.ecommerce_analytics.name
  name     = "staging"
  comment  = "Staging area for raw data"
}

resource "snowflake_schema" "dim" {
  database = snowflake_database.ecommerce_analytics.name
  name     = "dim"
  comment  = "Dimension tables"
}

resource "snowflake_schema" "fact" {
  database = snowflake_database.ecommerce_analytics.name
  name     = "fact"
  comment  = "Fact tables"
}

resource "snowflake_schema" "agg" {
  database = snowflake_database.ecommerce_analytics.name
  name     = "agg"
  comment  = "Aggregated metrics"
}

resource "snowflake_schema" "meta" {
  database = snowflake_database.ecommerce_analytics.name
  name     = "meta"
  comment  = "Metadata and audit"
}

# Snowflake Warehouse
resource "snowflake_warehouse" "analytics_wh" {
  name           = var.warehouse_name
  warehouse_size = var.warehouse_size
  
  auto_suspend = 60
  auto_resume  = true
  
  initially_suspended = true
  
  comment = "Warehouse for analytics queries"
}

# Snowflake Warehouse for ETL
resource "snowflake_warehouse" "etl_wh" {
  name           = "${var.warehouse_name}_etl"
  warehouse_size = "MEDIUM"
  
  auto_suspend = 60
  auto_resume  = true
  
  initially_suspended = true
  
  comment = "Warehouse for ETL processes"
}

# Snowflake Roles
resource "snowflake_role" "analyst" {
  name    = "analyst_role"
  comment = "Role for data analysts"
}

resource "snowflake_role" "etl" {
  name    = "etl_role"
  comment = "Role for ETL processes"
}

# Grant database usage to roles
resource "snowflake_database_grant" "analyst_usage" {
  database_name = snowflake_database.ecommerce_analytics.name
  privilege     = "USAGE"
  roles         = [snowflake_role.analyst.name]
}

resource "snowflake_database_grant" "etl_usage" {
  database_name = snowflake_database.ecommerce_analytics.name
  privilege     = "USAGE"
  roles         = [snowflake_role.etl.name]
}

# Grant schema usage
resource "snowflake_schema_grant" "analyst_dim" {
  database_name = snowflake_database.ecommerce_analytics.name
  schema_name   = snowflake_schema.dim.name
  privilege     = "USAGE"
  roles         = [snowflake_role.analyst.name]
}

resource "snowflake_schema_grant" "analyst_fact" {
  database_name = snowflake_database.ecommerce_analytics.name
  schema_name   = snowflake_schema.fact.name
  privilege     = "USAGE"
  roles         = [snowflake_role.analyst.name]
}

resource "snowflake_schema_grant" "analyst_agg" {
  database_name = snowflake_database.ecommerce_analytics.name
  schema_name   = snowflake_schema.agg.name
  privilege     = "USAGE"
  roles         = [snowflake_role.analyst.name]
}

# Grant warehouse usage
resource "snowflake_warehouse_grant" "analyst_wh" {
  warehouse_name = snowflake_warehouse.analytics_wh.name
  privilege      = "USAGE"
  roles          = [snowflake_role.analyst.name]
}

resource "snowflake_warehouse_grant" "etl_wh" {
  warehouse_name = snowflake_warehouse.etl_wh.name
  privilege      = "USAGE"
  roles          = [snowflake_role.etl.name]
}

# AWS S3 Bucket for data staging
resource "aws_s3_bucket" "data_staging" {
  bucket = var.s3_bucket_name
  
  tags = var.tags
}

# S3 Bucket versioning
resource "aws_s3_bucket_versioning" "data_staging" {
  bucket = aws_s3_bucket.data_staging.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

# S3 Bucket encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "data_staging" {
  bucket = aws_s3_bucket.data_staging.id
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# S3 Bucket lifecycle
resource "aws_s3_bucket_lifecycle_configuration" "data_staging" {
  bucket = aws_s3_bucket.data_staging.id
  
  rule {
    id     = "archive-old-data"
    status = "Enabled"
    
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    
    expiration {
      days = 365
    }
  }
}

# IAM Role for Snowflake
resource "aws_iam_role" "snowflake_role" {
  name = "${var.project_name}-snowflake-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = var.snowflake_external_id
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.snowflake_external_id
          }
        }
      }
    ]
  })
  
  tags = var.tags
}

# IAM Policy for S3 access
resource "aws_iam_role_policy" "snowflake_s3_policy" {
  name = "${var.project_name}-snowflake-s3-policy"
  role = aws_iam_role.snowflake_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.data_staging.arn,
          "${aws_s3_bucket.data_staging.arn}/*"
        ]
      }
    ]
  })
}

# Snowflake Storage Integration
resource "snowflake_storage_integration" "s3_integration" {
  name    = "s3_integration"
  type    = "EXTERNAL_STAGE"
  enabled = true
  
  storage_allowed_locations = ["s3://${aws_s3_bucket.data_staging.bucket}/"]
  storage_provider          = "S3"
  
  storage_aws_role_arn = aws_iam_role.snowflake_role.arn
  
  comment = "Integration with S3 for data loading"
}

# Snowflake External Stage
resource "snowflake_stage" "s3_stage" {
  name     = "s3_stage"
  database = snowflake_database.ecommerce_analytics.name
  schema   = snowflake_schema.staging.name
  
  url = "s3://${aws_s3_bucket.data_staging.bucket}/"
  
  storage_integration = snowflake_storage_integration.s3_integration.name
  
  file_format = "TYPE = JSON"
  
  comment = "External stage for S3 data"
}

# ECS Cluster for API
resource "aws_ecs_cluster" "api_cluster" {
  name = "${var.project_name}-api-cluster"
  
  tags = var.tags
}

# CloudWatch Log Group
resource "aws_cloudwatch_log_group" "api_logs" {
  name              = "/ecs/${var.project_name}-api"
  retention_in_days = 30
  
  tags = var.tags
}

# Outputs
output "snowflake_database" {
  value = snowflake_database.ecommerce_analytics.name
}

output "snowflake_warehouse" {
  value = snowflake_warehouse.analytics_wh.name
}

output "s3_bucket" {
  value = aws_s3_bucket.data_staging.bucket
}

output "snowflake_role_arn" {
  value = aws_iam_role.snowflake_role.arn
}

output "storage_integration" {
  value = snowflake_storage_integration.s3_integration.name
}
