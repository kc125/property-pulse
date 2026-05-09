# Databricks notebook source
#Fetch Secrets

adls_key = dbutils.secrets.get(
    scope = "property-pulse-scope",
    key = "adls-account-key"
)

adls_name = dbutils.secrets.get(
    scope = "property-pulse-scope",
    key = "adls-account-name"
)

spark.conf.set(
    f"fs.azure.account.key.propertypulsedl.dfs.core.windows.net", adls_key
)

print(f" Secrets Loaded!")

base_path = "abfss://property-data@propertypulsedl.dfs.core.windows.net"
catalog = "property_pulse_dbx"
database = "property_db"

# COMMAND ----------

# DBTITLE 1,Cell 2
#Create Database

spark.sql(f"""
    CREATE DATABASE IF NOT EXISTS {catalog}.{database}
""")
print("Database Created!")

display(spark.sql("SHOW DATABASES"))

# COMMAND ----------

silver_df = spark.read.format("delta") \
    .load(f"{base_path}/silver/property_events_clean")

silver_df.write.format("delta") \
    .mode("overwrite")\
        .saveAsTable(f"{catalog}.{database}.silver_property_events")

print(f" Silver -> {silver_df.count()} rows registered")

# COMMAND ----------

g1 = spark.read.format("delta")\
    .load(f"{base_path}/gold/avg_price_by_city")

g1.write.format("delta").mode("overwrite")\
    .saveAsTable(f"{catalog}.{database}.gold_avg_price_by_city")

print(f"gold_avg_price_by_city : {g1.count()} rows!")


#Gold table2

g2 = spark.read.format("delta")\
    .load(f"{base_path}/gold/listing_count_by_event")

g2.write.format("delta")\
    .mode("overwrite")\
        .saveAsTable(f"{catalog}.{database}.gold_listing_count_by_event")

print(f"gold_listing_count_by_event : {g2.count()} rows!")



# COMMAND ----------


#Gold table3

g3 = spark.read.format("delta")\
    .load(f"{base_path}/gold/price_trend_by_locality")

g3.write.format("delta")\
    .mode("overwrite")\
        .saveAsTable(f"{catalog}.{database}.gold_price_trend_by_locality")

print(f"gold_price_trend_by_locality : {g3.count()} rows!")


#Gold table4

g4 = spark.read.format("delta")\
    .load(f"{base_path}/gold/top_active_cities")

g4.write.format("delta")\
    .mode("overwrite")\
        .saveAsTable(f"{catalog}.{database}.gold_top_active_cities")

print(f"gold_top_active_cities : {g4.count()} rows!")

# COMMAND ----------

print("\n Final Verification")
print("-" * 40)

spark.sql("""
          SHOW TABLES IN
          property_pulse_dbx.property_db
          """).show(truncate = False)

print(f" ALL Tables in Unity Catalog!")