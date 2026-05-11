# Databricks notebook source
#create agent master table

adls_key = dbutils.secrets.get(scope="property-pulse-scope", key="adls-account-key")
adls_name = dbutils.secrets.get(scope="property-pulse-scope", key = "adls-account-name")

spark.conf.set("fs.azure.account.key.{adls_name}.dfs.core.windows.net", adls_key)

base = "abfss://property-data@propertypulsedl.dfs.core.windows.net"
agent_path = "abfss://property-data@propertypulsedl.dfs.core.windows.net/reference/agent_master"



# COMMAND ----------

#create agent data

agent_data = [
    ("AGT-100", "Rahul Sharma",
     "MUMBAI",    "Premium",  4.8, 150),
    ("AGT-101", "Priya Patel",
     "BENGALURU", "Standard", 4.2, 89),
    ("AGT-102", "Amit Kumar",
     "CHENNAI",   "Premium",  4.9, 210),
    ("AGT-103", "Sneha Reddy",
     "HYDERABAD", "Standard", 3.8, 45),
    ("AGT-104", "Vikram Singh",
     "PUNE",      "Premium",  4.6, 178),
    ("AGT-105", "Anjali Nair",
     "MUMBAI",    "Standard", 4.1, 92),
    ("AGT-106", "Rajesh Gupta",
     "CHENNAI",   "Elite",    5.0, 320),
    ("AGT-107", "Meera Joshi",
     "BENGALURU", "Elite",    4.9, 280),
    ("AGT-108", "Suresh Iyer",
     "HYDERABAD", "Standard", 3.9, 67),
    ("AGT-109", "Kavitha Rao",
     "PUNE",      "Premium",  4.7, 195)
]

agent_df = spark.createDataFrame(
    agent_data,
    ["agent_id", "agent_name", "base_city", "tier", "rating", "total_deals"]
)

agent_df.write\
    .format("delta")\
        .mode("overwrite")\
            .save(agent_path)
                  
print(f"Agent master created")
print(f"Total agents : {agent_df.count()}")

agent_df.show(truncate = False)

# COMMAND ----------

# DBTITLE 1,Cell 3
# SCD type 1

from delta.tables import DeltaTable
from pyspark.sql.functions import col

print(f" SCD Type 1- updating tiers")
print("-"*40)

#Simulate incoming updates

updates_data = [
    ("AGT-103", "Sneha Reddy",
     "HYDERABAD", "Premium",  3.8, 45),
    ("AGT-108", "Suresh Iyer",
     "HYDERABAD", "Premium",  3.9, 67)
]

updates_df = spark.createDataFrame(
    updates_data,
    ["agent_id", "agent_name", "base_city", "tier", "rating", "total_deals"]
)

print("Incoming Updates")
updates_df.show(truncate=False)

print("before Update")

spark.read\
    .format("delta")\
        .load(agent_path)\
            .filter(col("agent_id").isin(
                ["AGT-103","AGT-108"]
            )).show(truncate=False)

#SCD type1 merge

agent_table = DeltaTable.forPath(spark, agent_path)

agent_table.alias("existing").merge(updates_df.alias("updates"),
                                    "existing.agent_id = updates.agent_id")\
                                        .whenMatchedUpdate(set = {
                                            "tier" : "updates.tier"
                                        })\
                                            .whenNotMatchedInsert(values= {
                                                "agent_id" : "updates.agent_id",
                                                "agent_name" : "updates.agent_name",
                                                "base_city" : "updates.base_city",
                                                "tier" : "updates.tier",
                                                "rating" : "updates.rating",
                                                "total_deals" : "updates.total_deals"
                                            })\
                                             .execute()

print("After Update:")
spark.read\
    .format("delta")\
        .load(agent_path)\
            .filter(col("agent_id").isin(["AGT-103","AGT-108"]))\
                .show(truncate=False)
print("-"*40)
print("SCD 1 Complete")
print("old tier Gone - no histroty")
print("tier updated")

# COMMAND ----------

#SCD type 2

from pyspark.sql.functions import (
    col, current_timestamp, lit
)
from pyspark.sql.types import TimestampType

agent_scd2_path = f"{base}/reference/agent_master_scd2"

agent_df = spark.read.format("delta")\
    .load(agent_path)

agent_scd2_df = agent_df.withColumn("effective_start_date", current_timestamp())\
    .withColumn("effective_end_date", lit(None).cast(TimestampType()))\
    .withColumn("is_current", lit(True))\
    .withColumn("scd_version", lit(1))


# COMMAND ----------


agent_scd2_df.write.format("delta")\
    .mode("overwrite")\
    .save(agent_scd2_path)

print("SCD type 2 created")
print(f" Total records: {agent_scd2_df.count()}")
agent_scd2_df.show(truncate=False)

# COMMAND ----------

#Expire old data record

print("Processing city change for AGT-107")
print("-"*40)

#incoming change

changes_data =[
    ("AGT-107", "Meera Joshi",
     "MUMBAI",   # ← moved from BENGALURU!
     "Elite", 4.9, 280)
]

changes_df = spark.createDataFrame(
    changes_data,
    ["agent_id", "agent_name", "base_city", "tier", "rating", "total_deals"]
)

print("Incoming Change")
#display(changes_df)

print("Before Change  - AGT-107:")

spark.read.format("delta")\
        .load(agent_scd2_path)\
        .filter(col("agent_id") == "AGT-107")\
            .show(truncate=False)

scd2_table = DeltaTable.forPath(spark, agent_scd2_path)

scd2_table.alias("existing")\
    .merge(
        changes_df.alias("changes"),
        "existing.agent_id = changes.agent_id AND existing.is_current = true AND existing.base_city != changes.base_city"
    )\
        .whenMatchedUpdate(set ={
            "is_current" : "false",
            "effective_end_date" : current_timestamp()
        })\
            .execute()

print("Old record expired")
print("After expire - AGT-107:")
display(spark.read.format("delta")\
        .load(agent_scd2_path)\
        .filter(col("agent_id") == "AGT-107"))


# COMMAND ----------

# ══════════════════════════════════════
# STEP 3D — INSERT NEW RECORD
# AGT-107 with new city MUMBAI!
# ══════════════════════════════════════

from pyspark.sql.functions import (
    current_timestamp, lit
)
from pyspark.sql.types import TimestampType

print("➕ Inserting new record for AGT-107...")
print("─" * 40)

# New record with MUMBAI!
new_record = spark.createDataFrame(
    [("AGT-107", "Meera Joshi",
      "MUMBAI", "Elite",
      4.9, 280)],
    ["agent_id", "agent_name",
     "base_city", "tier",
     "rating", "total_deals"]
) \
.withColumn(
    "effective_start_date",
    current_timestamp()
) \
.withColumn(
    "effective_end_date",
    lit(None).cast(TimestampType())
) \
.withColumn(
    "is_current",
    lit(True)
) \
.withColumn(
    "scd_version",
    lit(2)       # ← version 2!
)

# Append new record!
new_record.write \
    .format("delta") \
    .mode("append") \
    .save(agent_scd2_path)

print("✅ New record inserted!")
print("─" * 40)

# Show FULL history for AGT-107!
print("📊 FULL HISTORY — AGT-107:")
spark.read \
    .format("delta") \
    .load(agent_scd2_path) \
    .filter(col("agent_id") == "AGT-107") \
    .orderBy("scd_version") \
    .show(truncate=False)

# COMMAND ----------

# ══════════════════════════════════════
# FINAL VERIFICATION
# ══════════════════════════════════════

final_df = spark.read \
    .format("delta") \
    .load(agent_scd2_path)

final_df.createOrReplaceTempView(
    "agent_master_scd2"
)

print("🏆 SCD TYPE 2 VERIFICATION")
print("─" * 40)
print(f"Total records    : {final_df.count()}")
print(f"Current records  : {final_df.filter(col('is_current') == True).count()}")
print(f"Historical records: {final_df.filter(col('is_current') == False).count()}")

# Query 1 — Current agents only!
print("\n📊 Query 1 — Current agents:")
spark.sql("""
    SELECT
        agent_id,
        agent_name,
        base_city,
        tier,
        scd_version
    FROM agent_master_scd2
    WHERE is_current = true
    ORDER BY agent_id
""").show(truncate=False)

# Query 2 — Full history AGT-107!
print("\n📊 Query 2 — AGT-107 history:")
spark.sql("""
    SELECT
        agent_id,
        agent_name,
        base_city,
        is_current,
        effective_start_date,
        effective_end_date,
        scd_version
    FROM agent_master_scd2
    WHERE agent_id = 'AGT-107'
    ORDER BY scd_version
""").show(truncate=False)

# Query 3 — Who changed city?
print("\n📊 Query 3 — Agents who changed city:")
spark.sql("""
    SELECT
        agent_id,
        agent_name,
        COUNT(*) as versions
    FROM agent_master_scd2
    GROUP BY agent_id, agent_name
    HAVING COUNT(*) > 1
""").show(truncate=False)

print("─" * 40)
print("🏆 SCD TYPE 2 COMPLETE!")

# COMMAND ----------

# ══════════════════════════════════════
# CLEAN DUPLICATE AGT-107 RECORDS
# ══════════════════════════════════════

print("🧹 Cleaning duplicates...")
print("─" * 40)

# Read current SCD2 table
scd2_df = spark.read \
    .format("delta") \
    .load(agent_scd2_path)

print(f"Before clean: {scd2_df.count()} records")

# Keep only DISTINCT records!
clean_df = scd2_df.dropDuplicates([
    "agent_id",
    "base_city",
    "is_current",
    "scd_version"
])

print(f"After clean : {clean_df.count()} records")

# Overwrite with clean data!
clean_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(agent_scd2_path)

print("✅ Duplicates removed!")
print("─" * 40)

# Verify!
final = spark.read \
    .format("delta") \
    .load(agent_scd2_path)

print(f"Total records    : {final.count()}")
print(f"Current records  : {final.filter(col('is_current') == True).count()}")
print(f"Historical records: {final.filter(col('is_current') == False).count()}")

print("\n📊 AGT-107 history:")
final.filter(
    col("agent_id") == "AGT-107"
).orderBy("scd_version") \
 .show(truncate=False)