# E-commerce Analytics Warehouse

## Overview
Comprehensive customer behavior analysis and product performance analytics platform for e-commerce businesses using Snowflake. Provides RFM segmentation, conversion funnel analysis, cohort retention tracking, and product affinity insights to drive data-driven decision making.

## Technologies
- **Cloud Data Warehouse**: Snowflake
- **Programming**: Python 3.9+, SQL
- **Transformation**: dbt (data build tool)
- **API**: Flask, REST
- **Infrastructure**: Terraform, AWS S3
- **Visualization**: HTML/JavaScript, Looker
- **Testing**: pytest

## Architecture
```
Web Analytics (GA) ──┐
Order System (DB) ────┼──> S3 Staging ──> Snowflake ──> dbt Models ──> BI/API
Product Catalog ──────┘                        │
                                               ├──> Staging
                                               ├──> Dimensions
                                               ├──> Facts
                                               └──> Aggregates
```

## Features

### Customer Analytics
- **RFM Segmentation**: Recency, Frequency, Monetary analysis
  - Champions, Loyal Customers, At Risk, Lost Customers
  - Automated segment assignment
  - Customer lifetime value calculation
- **Cohort Analysis**: Monthly retention tracking
- **Customer Behavior**: Session analysis, engagement metrics
- **Churn Prediction**: At-risk customer identification

### Product Analytics
- **Product Affinity**: Market basket analysis
  - Products frequently bought together
  - Support, confidence, and lift metrics
  - Cross-sell recommendations
- **Product Performance**: Views, conversions, revenue by product
- **Category Analysis**: Performance by category and subcategory

### Conversion Analytics
- **Conversion Funnel**: 5-stage funnel tracking
  - Visit → Product View → Add to Cart → Checkout → Purchase
  - Stage-by-stage conversion rates
  - Drop-off analysis
- **Cart Abandonment**: Tracking and analysis
- **A/B Testing**: Experiment tracking and analysis

### Business Metrics
- **Revenue Analytics**: Daily, weekly, monthly trends
- **Order Analytics**: AOV, order frequency, order composition
- **Customer Acquisition**: New vs returning customers
- **KPI Dashboard**: Real-time business metrics

## Project Structure
```
ecommerce-analytics-warehouse/
├── src/
│   └── ecommerce_etl.py           # ETL pipeline
├── api/
│   └── analytics_api.py           # REST API
├── sql/
│   └── create_ecommerce_schema.sql # Snowflake schema
├── dbt/
│   └── models/
│       ├── rfm_analysis.sql       # RFM model
│       └── conversion_funnel.sql  # Funnel model
├── terraform/
│   ├── main.tf                    # Infrastructure
│   └── variables.tf
├── ui/
│   └── analytics_dashboard.html   # Dashboard
├── tests/
│   └── test_ecommerce_etl.py      # Unit tests
├── config/
│   └── config.yaml                # Configuration
├── Dockerfile
├── requirements.txt
└── README.md
```

## Quick Start

### Prerequisites
- Snowflake account
- AWS account (for S3 staging)
- Terraform 1.0+
- Python 3.9+
- dbt 1.0+

### 1. Deploy Infrastructure
```bash
cd terraform

# Initialize Terraform
terraform init

# Deploy Snowflake and AWS resources
terraform apply \
  -var="snowflake_account=your_account" \
  -var="snowflake_user=your_user" \
  -var="snowflake_password=your_password" \
  -var="snowflake_external_id=your_external_id" \
  -var="s3_bucket_name=your-bucket-name"
```

### 2. Create Database Schema
```bash
# Connect to Snowflake
snowsql -a your_account -u your_user

# Run schema creation
!source sql/create_ecommerce_schema.sql
```

### 3. Configure dbt
```bash
cd dbt

# Initialize dbt profile
dbt init

# Test connection
dbt debug

# Run models
dbt run

# Run tests
dbt test
```

### 4. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 5. Configure Application
```bash
# Set environment variables
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
```

### 6. Run ETL Pipeline
```bash
python src/ecommerce_etl.py
```

### 7. Start API Server
```bash
python api/analytics_api.py
```

### 8. Access Dashboard
Open `ui/analytics_dashboard.html` in browser

## API Endpoints

### Analytics Summary
```bash
GET /api/v1/analytics/summary?start_date=2024-01-01&end_date=2024-12-31
```

### RFM Analysis
```bash
GET /api/v1/customers/rfm?start_date=2024-01-01&end_date=2024-12-31&segment=Champions
```

### Customer Segments
```bash
GET /api/v1/customers/segments
```

### Conversion Funnel
```bash
GET /api/v1/conversion/funnel?start_date=2024-01-01&end_date=2024-12-31
```

### Product Affinity
```bash
GET /api/v1/products/affinity?min_support=0.01&limit=20
```

### Cohort Retention
```bash
GET /api/v1/cohorts/retention?start_date=2024-01-01&end_date=2024-12-31
```

### Customer Behavior
```bash
GET /api/v1/customers/behavior?customer_id=CUST-001
```

### KPIs
```bash
GET /api/v1/metrics/kpis?start_date=2024-01-01&end_date=2024-12-31
```

## dbt Models

### RFM Analysis
```bash
dbt run --models rfm_analysis
```

### Conversion Funnel
```bash
dbt run --models conversion_funnel
```

### Run All Models
```bash
dbt run
```

### Generate Documentation
```bash
dbt docs generate
dbt docs serve
```

## Testing
```bash
# Run unit tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html

# Run dbt tests
dbt test
```

## Docker Deployment
```bash
# Build image
docker build -t ecommerce-analytics-api .

# Run container
docker run -p 5000:5000 \
  -e SNOWFLAKE_ACCOUNT="your_account" \
  -e SNOWFLAKE_USER="your_user" \
  -e SNOWFLAKE_PASSWORD="your_password" \
  ecommerce-analytics-api
```

## Data Pipeline

### 1. Data Ingestion
- Web events from Google Analytics
- Orders from transactional database
- Products from catalog API
- Customer data from CRM

### 2. Staging
- Raw data loaded to Snowflake staging schema
- Data validation and quality checks
- Deduplication

### 3. Transformation (dbt)
- Dimension tables (SCD Type 2)
- Fact tables
- Aggregate tables
- Analytical models

### 4. Analytics
- RFM segmentation
- Conversion funnel
- Cohort analysis
- Product affinity

### 5. Consumption
- REST API
- BI dashboards (Looker)
- Data exports

## Performance Optimization

### Snowflake
- Clustering keys on large tables
- Materialized views for aggregations
- Result caching enabled
- Auto-suspend and auto-resume

### Query Optimization
- Partition pruning
- Predicate pushdown
- Column pruning
- Query result caching

## Security
- Snowflake role-based access control
- AWS IAM roles for S3 access
- Data encryption at rest and in transit
- PII data masking
- Audit logging

## Monitoring
- ETL pipeline monitoring
- Data quality checks
- API performance metrics
- Alert notifications

## License
MIT License

## Support
For issues and questions, please open a GitHub issue.
