# Databricks notebook source
from pyspark.sql.functions import col, count, avg
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# Configuration for Azure Data Lake storage
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": "",
    "fs.azure.account.oauth2.client.secret": '',
    "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com//oauth2/token"
}

# Mounting the 'raw-data' container from Azure Data Lake storage to Databricks
dbutils.fs.mount(
    source="abfss://raw-data@etl2024datalake.dfs.core.windows.net",
    mount_point="/mnt/electricMount",
    extra_configs=configs
)

# Mounting the 'transformed' container from Azure Data Lake storage to Databricks
dbutils.fs.mount(
    source="abfss://transformed@etl2024datalake.dfs.core.windows.net",
    mount_point="/mnt/transformedMount",
    extra_configs=configs
)

# Reading data from the mounted storage
electric = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/electricMount/electric.csv")

# Cleaning data by dropping rows with missing values in specified columns
cleaned_electric = electric.na.drop(subset=["VIN (1-10)", "County", "City"])

# Converting the data type of the 'Postal Code' column to string
cleaned_electric = cleaned_electric.withColumn("Postal Code", col("Postal Code").cast("string"))

# Filtering rows for electric vehicles from the year 2019 onwards
filtered_electric = cleaned_electric.filter(cleaned_electric["Model Year"] >= 2019)

# Selecting specific columns for analysis
prepared_electric = filtered_electric.select("VIN (1-10)", "Make", "Model", "Model Year", "Electric Range")

# Analysis 1: Counting the number of electric vehicles by county
ev_adoption_by_county = electric.groupBy("County").agg(count("VIN (1-10)").alias("Number of EVs"))
print("EV Adoption by County:")
ev_adoption_by_county.show()

# Analysis 2: Distribution of electric vehicle types
ev_type_distribution = electric.groupBy("Electric Vehicle Type").agg(count("VIN (1-10)").alias("Count"))
print("EV Type Distribution:")
ev_type_distribution.show()

# Analysis 3: Calculating the average electric range of vehicles
average_electric_range = electric.select(avg("Electric Range").alias("Average Electric Range"))
print("Average Electric Range:")
average_electric_range.show()

# Analysis 4: Popularity of models
model_popularity = electric.groupBy("Make", "Model").agg(count("VIN (1-10)").alias("Count"))
model_popularity = model_popularity.orderBy(col("Count").desc())
print("Model Popularity:")
model_popularity.show()

# Analysis 5: Electric vehicle trends over time
ev_trends_over_time = electric.groupBy("Model Year").agg(count("VIN (1-10)").alias("Number of EVs"))
ev_trends_over_time = ev_trends_over_time.orderBy("Model Year")
print("EV Trends Over Time:")
ev_trends_over_time.show()

# Analysis 6: Distribution of Clean Alternative Fuel Vehicle (CAFV) Eligibility
cafv_distribution = electric.groupBy("Clean Alternative Fuel Vehicle (CAFV) Eligibility").agg(count("VIN (1-10)").alias("Count"))
print("CAFV Distribution:")
cafv_distribution.show()

# Analysis 7: Distribution of electric vehicles in legislative districts
legislative_district_distribution = electric.groupBy("Legislative District").agg(count("VIN (1-10)").alias("Number of EVs"))
print("Legislative District Distribution:")
legislative_district_distribution.show()

# Writing the processed DataFrame to storage
prepared_electric.repartition(1).write.mode("overwrite").option("header", 'true').csv("/mnt/transformedMount/transformed/electric")
