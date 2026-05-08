# Databricks notebook source
from pyspark.sql.functions import (
    col, to_timestamp, upper, trim , 
    when, current_timestamp, lit, substring, sha2
)

# Fetch secrets

adls_key = dbutils.secrets.get(scope = "property-pulse-scope", key = "adls-account-key")
adls_name = dbutils.secrets.get(scope = "property-pulse-scope", key = "adls-account-name")

spark.conf.set(f"fs.azure.account.key.{adls_name}.dfs.core.windows.net", adls_key)
print("Secrets Loaded") 

# Silver Layer V2 - AMc terminal push test and removed the wrong path with updating the key for adls key

# COMMAND ----------

## Define paths and Read bronze

bronze_path = f"abfss://property-data@{adls_name}.dfs.core.windows.net/bronze/property_events"
silver_path = f"abfss://property-data@{adls_name}.dfs.core.windows.net/silver/property_events_clean"

bronze_df = spark.read.format("delta")\
    .load(bronze_path)

print(f" Bronze record count: {bronze_df.count()}")
bronze_df.printSchema()

# COMMAND ----------

#Transformations
silver_df = bronze_df\
    .withColumn("event_timestamp", to_timestamp(col("timestamp")))\
                .withColumn("event_type", upper(trim(col("event_type"))))\
                .withColumn("city", upper(trim(col("city"))))\
                .withColumn("locality", trim(col("locality")))\
                .withColumn("price", when(col("price").isNull(),0).otherwise(col("price")).cast("long"))\
                .withColumn("bedrooms", col("bedrooms").cast("integer"))\
                .withColumn("area_sqft", col("area_sqft").cast("integer"))\
                .withColumn("ingestion_timestamp", current_timestamp())\
                    .drop("timestamp", "source")\
                        .dropDuplicates([
                            "listing_id",
                            "event_type",
                            "event_timestamp"
                        ])

print(f" Silver record count : {silver_df.count()}")
silver_df.printSchema()
silver_df.show(5, truncate= False)



# COMMAND ----------

##write to silver delta

silver_df.write\
    .format("delta")\
        .mode("overwrite")\
            .option("mergeSchema", "true")\
                .option("schemaEvolutionMode", "rescue")\
                .save(silver_path)

print(f" Silver layer written successfully!")
print(f" Location :{silver_path}")