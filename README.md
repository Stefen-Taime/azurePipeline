# Azure Data Pipeline Architecture

The following diagram illustrates the architecture of our data pipeline:

![Data Pipeline Architecture](azure2.png)

## Architecture Overview

- **External Sources**: Data is ingested from various external sources through HTTP APIs.
- **Terraform**: Infrastructure as Code (IaC) is used to provision and manage cloud resources, such as Key Vault, Data Factory, and the Data Lake.
- **ETL Pipeline**: The Extract, Transform, Load (ETL) processes are orchestrated using:
  - **Data Factory**: Azure's cloud-based data integration service which is configured to copy the CSV file from the HTTP source containing electric vehicle population data.
  - **Databricks**: A unified data analytics platform that processes data using PySpark. Here is an overview of the operations performed by Databricks:
    - Mounting the Azure Data Lake storage containers for `raw-data` and `transformed` data.
    - Reading the data from the mounted storage and cleaning it by dropping rows with missing values.
    - Converting data types and filtering rows based on the model year.
    - Performing various analyses, such as counting the number of electric vehicles by county, distribution of electric vehicle types, and calculating the average electric range.
    - Writing the processed DataFrame to storage in Parquet format.

### Databricks Script Explanation

The Databricks script performs the following operations:

1. **Configuration**: Sets up the connection to Azure Data Lake storage using OAuth credentials.
2. **Mounting Storage**: Mounts the 'raw-data' and 'transformed' containers from Azure Data Lake storage to Databricks.
3. **Data Ingestion**: Reads the electric vehicle data CSV file from the mounted `raw-data` container.
4. **Data Cleaning**: Drops rows with missing values and converts the data type of certain columns.
5. **Data Filtering**: Filters the dataset for electric vehicles from the year 2019 onwards.
6. **Data Selection**: Selects specific columns for analysis.
7. **Data Analysis**: Performs a series of analyses to gain insights into the electric vehicle data, such as adoption rates by county, electric vehicle type distribution, average electric range, model popularity, trends over time, and distribution of Clean Alternative Fuel Vehicle (CAFV) eligibility.
8. **Data Writing**: Writes the processed data to the `transformed` container in the Data Lake in Parquet format.

The analyses provide valuable insights into the electric vehicle market, with counts of electric vehicles by county, types, and trends, as well as other important metrics.

