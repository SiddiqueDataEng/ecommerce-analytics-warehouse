variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "ecommerce-analytics"
}

variable "snowflake_account" {
  description = "Snowflake account identifier"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake username"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake password"
  type        = string
  sensitive   = true
}

variable "snowflake_role" {
  description = "Snowflake role"
  type        = string
  default     = "ACCOUNTADMIN"
}

variable "snowflake_external_id" {
  description = "Snowflake external ID for AWS IAM role"
  type        = string
}

variable "database_name" {
  description = "Name of the Snowflake database"
  type        = string
  default     = "ecommerce_analytics"
}

variable "warehouse_name" {
  description = "Name of the Snowflake warehouse"
  type        = string
  default     = "analytics_wh"
}

variable "warehouse_size" {
  description = "Size of the Snowflake warehouse"
  type        = string
  default     = "SMALL"
  
  validation {
    condition     = contains(["XSMALL", "SMALL", "MEDIUM", "LARGE", "XLARGE"], var.warehouse_size)
    error_message = "Warehouse size must be one of: XSMALL, SMALL, MEDIUM, LARGE, XLARGE"
  }
}

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket for data staging"
  type        = string
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default = {
    Environment = "Production"
    Project     = "E-commerce Analytics"
    ManagedBy   = "Terraform"
  }
}
