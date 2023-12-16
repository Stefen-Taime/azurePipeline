# Data Pipeline Architecture

The following diagram illustrates the architecture of our data pipeline:

![Data Pipeline Architecture](azure2.png)

## Architecture Overview

- **External Sources**: Data is ingested from various external sources through HTTP APIs.
- **Terraform**: Infrastructure as Code (IaC) is used to provision and manage any cloud resources.
- **ETL Pipeline**: Extract, Transform, Load (ETL) processes are orchestrated using:
  - **Data Factory**: Azure's cloud-based data integration service.
  - **Databricks**: Unified data analytics platform.
  - **Key Vault**: Azure service for managing secrets and keys securely.
- **Data Lake**: The central repository for raw and transformed data, designed for big data analytics.
