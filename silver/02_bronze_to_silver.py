# Databricks notebook source
from pyspark.sql.functions import (
    col, to_timestamp, upper, trim , 
    when, current_timestamp, lit, substring, sha2
)

# Fetch secrets

adls_key = dbutils.secrets.get(scope = "property-pulse-scope", key = "adls-account-key")
adls_name = dbutils.secrets.get(scope = "property-pulse-scope", key = "adls-account-name")

spark.conf.set(f"fs.azure.account.key.{adls_name}.dfs.core.windows.net", adls_key)
spark.conf.set("spark.sql.shuffle.partitions", "8")
print("Secrets Loaded") 



# COMMAND ----------

## Define paths and Read bronze

bronze_path = f"abfss://property-data@{adls_name}.dfs.core.windows.net/bronze/property_events"
silver_path = f"abfss://property-data@{adls_name}.dfs.core.windows.net/silver/property_events_clean"

bronze_df = spark.read.format("delta")\
    .load(bronze_path)

print(f" Bronze record count: {bronze_df.count()}")
bronze_df.printSchema()

# COMMAND ----------

print(f"Bronze records: {bronze_df.count()}")

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

silver_df = silver_df.repartition(4)
silver_df.cache()

print(f" Silver record count : {silver_df.count()}")
silver_df.printSchema()
silver_df.show(5, truncate= False)



# COMMAND ----------

#Data Quality checks

from pyspark.sql.functions import col, countDistinct

#Check 1 - Null listing _ID
print("Running data quality checks")
print("-" * 40)

#caching df first so that we can resue it for other checks - not everytime this runs the full. scan
#silver_df.cache()

null_listing = silver_df.filter(col("listing_id").isNull()).count()

print(f"Null listing_id count : {null_listing}")
assert null_listing == 0, "Null listing_id found - failed"
print("Check 1 passed : No null_listing_id found")

# check2 - Invalid event types
valid_events = ["NEW_LISTING", "PRICE_CHANGE", "REMOVED"]
invalid_events = silver_df.filter(~col("event_type").isin(valid_events)).count()
print(f"Invalid event_type count : {invalid_events}")
assert invalid_events == 0 , "Invalid event_type found -failed"
print("Check 2 passed : All event_types are valid")

#check 3 - Negative prices
negative_prices = silver_df.filter(col("price") < 0).count()
print(f" negative price count : {negative_prices}")
assert negative_prices == 0, "Failed : Negative price found!"
print(f" Check 3 passed: No negative prices found")

#Check 4 - Invalid bedrooms
invalid_beds = silver_df.filter((col("bedrooms") <= 0) | (col("bedrooms") > 20)).count()
print(f"Invalid bedrooms count : {invalid_beds}")
assert invalid_beds == 0, "Failed. : Invalid bedrooms counts!"
print(f" Check 4 passed : Bedrooms count valid!")

#Check5 - Record count check
total = silver_df.count()
print(f"total silver records : {total}")
assert total > 0, "Failed: NO records found"
print(f" Check 5 passed : Total records found!")

silver_df.unpersist()
print("-" * 40)
print(f" ALL DATA Quality checks passed")


# COMMAND ----------

#Delta table optimizaitions

print(f" Configuring Delta optimizations..")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

print(f" Auto Compact enabled")
print("Optimizations configured")

# COMMAND ----------

##write to silver delta

silver_df.write\
    .format("delta")\
        .mode("overwrite")\
            .option("mergeSchema", "true")\
                .option("schemaEvolutionMode", "rescue")\
                    .option("delta.enableDeletionVectors", "true")\
                        .option("delta.autoOptimize.autoCompact", "true")\
                                .option("delta.autoOptimize.optimizeWrite", "true")\
                                    .save(silver_path)

print(f" Silver layer written successfully!")
print(f" Location :{silver_path}")

# COMMAND ----------

#Enable Deletion vectors

spark.sql(f"""
          ALTER TABLE delta.`{silver_path}` SET TBLPROPERTIES (delta.enableDeletionVectors = true)
          """)
display(spark.sql(f"DESCRIBE DETAIL delta.`{silver_path}`"))


