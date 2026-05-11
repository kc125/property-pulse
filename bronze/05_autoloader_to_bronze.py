# Databricks notebook source
# ══════════════════════════════════════
# SETUP
# ══════════════════════════════════════
adls_key = dbutils.secrets.get(
             scope = "property-pulse-scope",
             key   = "adls-account-key"
           )

# CORRECT storage account name!
spark.conf.set(
    "fs.azure.account.key.propertypulsedl.dfs.core.windows.net",
    adls_key
)

# Verify connection!
files = dbutils.fs.ls(
    "abfss://property-data@propertypulsedl.dfs.core.windows.net/"
)
print(f"✅ Connected! Found {len(files)} items!")

base           = "abfss://property-data@propertypulsedl.dfs.core.windows.net"
incoming_path  = f"{base}/autoloader/incoming"
bronze_al_path = f"{base}/bronze/autoloader_property_events"
checkpoint_path= f"{base}/autoloader/_checkpoint"
schema_path    = f"{base}/autoloader/_schema"

print(f"✅ Setup complete!")
print(f"📥 Incoming : {incoming_path}")
print(f"📤 Bronze   : {bronze_al_path}")

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType,
    IntegerType
)

schema = StructType([
    StructField("listing_id",  StringType(),  True),
    StructField("event_type",  StringType(),  True),
    StructField("city",        StringType(),  True),
    StructField("locality",    StringType(),  True),
    StructField("price",       LongType(),    True),
    StructField("bedrooms",    IntegerType(), True),
    StructField("area_sqft",   IntegerType(), True),
    StructField("agent_id",    StringType(),  True),
    StructField("timestamp",   StringType(),  True),
    StructField("source",      StringType(),  True)
])

print("✅ Schema defined!")

# COMMAND ----------

# ══════════════════════════════════════
# CREATE CSV FILES IN INCOMING FOLDER!
# ══════════════════════════════════════
import random
from datetime import datetime, timezone

base          = "abfss://property-data@propertypulsedl.dfs.core.windows.net"
incoming_path = f"{base}/autoloader/incoming"

# Sample data
cities = {
    "MUMBAI"    : ["Bandra", "Andheri", "Powai"],
    "BENGALURU" : ["Whitefield", "Koramangala"],
    "CHENNAI"   : ["Anna Nagar", "Velachery"],
    "HYDERABAD" : ["Gachibowli", "Madhapur"],
    "PUNE"      : ["Hinjewadi", "Kharadi"]
}
event_types = ["NEW_LISTING",
               "PRICE_CHANGE",
               "REMOVED"]

# Generate 100 records
records = []
for i in range(100):
    city     = random.choice(list(cities.keys()))
    locality = random.choice(cities[city])
    etype    = random.choice(event_types)
    records.append((
        f"EXT-{random.randint(10000, 19999)}",
        etype,
        city,
        locality,
        random.randint(3000000, 15000000)
        if etype != "REMOVED" else 0,
        random.choice([1, 2, 3, 4]),
        random.randint(500, 3000),
        f"AGT-{random.randint(100, 109)}",
        datetime.now(timezone.utc).isoformat(),
        "external-provider"
    ))

schema_cols = [
    "listing_id", "event_type",
    "city",       "locality",
    "price",      "bedrooms",
    "area_sqft",  "agent_id",
    "timestamp",  "source"
]

csv_df = spark.createDataFrame(
    records, schema_cols
)

# Write CSV to incoming folder!
csv_df.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(f"{incoming_path}/batch_001")

print(f"✅ Batch 001 created!")
print(f"📊 Records : {csv_df.count()}")
csv_df.show(5, truncate=False)

# Verify it landed!
files = dbutils.fs.ls(incoming_path)
print(f"\n📁 Incoming folder contents:")
for f in files:
    print(f"  → {f.name}")

# COMMAND ----------

# Drop batch_002!
records_2 = []
for i in range(50):
    city     = random.choice(list(cities.keys()))
    locality = random.choice(cities[city])
    etype    = random.choice(event_types)
    records_2.append((
        f"EXT-{random.randint(20000, 29999)}",
        etype,
        city,
        locality,
        random.randint(3000000, 15000000)
        if etype != "REMOVED" else 0,
        random.choice([1, 2, 3, 4]),
        random.randint(500, 3000),
        f"AGT-{random.randint(100, 109)}",
        datetime.now(timezone.utc).isoformat(),
        "external-provider-batch2"
    ))

csv_df_2 = spark.createDataFrame(
    records_2, schema_cols
)

csv_df_2.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(f"{incoming_path}/batch_002")

print(f"✅ Batch 002 dropped!")
print(f"📊 Records: {csv_df_2.count()}")

# COMMAND ----------

from pyspark.sql.functions import (
    current_timestamp,
    input_file_name
)

# Read with Auto Loader!
autoloader_stream = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation",
            schema_path) \
    .option("header", "true") \
    .schema(schema) \
    .load(incoming_path)

print("✅ Auto Loader stream created!")
print("👀 Watching for new files...")

# COMMAND ----------

# Add metadata columns!
enriched_stream = autoloader_stream \
    .withColumn(
        "ingestion_timestamp",
        current_timestamp()
    ) \
    .withColumn(
        "source_file_name",
        input_file_name()
    )

# Write to Bronze!
query = enriched_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option(
        "checkpointLocation",
        checkpoint_path
    ) \
    .option("mergeSchema", "true") \
    .option(
        "delta.autoOptimize.autoCompact",
        "true"
    ) \
    .trigger(availableNow=True) \
    .start(bronze_al_path)

# Wait for completion!
query.awaitTermination()
print("✅ Auto Loader complete!")

# COMMAND ----------

# Read AFTER write completes!
result_df = spark.read \
    .format("delta") \
    .load(bronze_al_path)

print("🏆 AUTO LOADER RESULTS")
print("─" * 40)
print(f"✅ Records ingested: {result_df.count()}")

print("\n📊 Sample records:")
result_df.show(5, truncate=False)

print("\n📁 Files processed:")
result_df.select("source_file_name") \
    .distinct() \
    .show(truncate=False)

# COMMAND ----------

existing_df = spark.read \
    .format("delta") \
    .load(bronze_al_path)

print(f"Before: {existing_df.count()}")

dedup_df = existing_df \
    .dropDuplicates(["listing_id"])

print(f"After : {dedup_df.count()}")

dedup_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(bronze_al_path)

print("✅ Duplicates removed!")