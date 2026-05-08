# Databricks notebook source
# Quick check — do we have Bronze data?
adls_name   = dbutils.secrets.get(
                scope = "property-pulse-scope",
                key   = "adls-account-name"
              )
adls_key    = dbutils.secrets.get(
                scope = "property-pulse-scope",
                key   = "adls-account-key"
              )

spark.conf.set(
    f"fs.azure.account.key.{adls_name}.dfs.core.windows.net",
    adls_key
)

bronze_path = f"abfss://property-data@{adls_name}.dfs.core.windows.net/bronze/property_events"
silver_path = f"abfss://property-data@{adls_name}.dfs.core.windows.net/silver/property_events_clean"

df = spark.read.format("delta").load(bronze_path)
print(f"✅ Bronze records found: {df.count()}")
df.show(3, truncate=False)



# COMMAND ----------

df = spark.read.format("delta").load(silver_path)
print(f" silver records found: {df.count()}")
df.show(3, truncate=False)

# COMMAND ----------

# Verify using SQL directly!
print("🏆 GOLD LAYER SQL VERIFICATION")
print("─" * 40)

spark.sql("""
    SELECT 'avg_price_by_city' AS table_name,
            COUNT(*) AS row_count
    FROM delta.`abfss://property-data@propertypulsedl.dfs.core.windows.net/gold/avg_price_by_city`
    UNION ALL
    SELECT 'listing_count_by_event',
            COUNT(*)
    FROM delta.`abfss://property-data@propertypulsedl.dfs.core.windows.net/gold/listing_count_by_event`
    UNION ALL
    SELECT 'price_trend_by_locality',
            COUNT(*)
    FROM delta.`abfss://property-data@propertypulsedl.dfs.core.windows.net/gold/price_trend_by_locality`
    UNION ALL
    SELECT 'top_active_cities',
            COUNT(*)
    FROM delta.`abfss://property-data@propertypulsedl.dfs.core.windows.net/gold/top_active_cities`
""").show(truncate=False)

print("🏆 ALL GOLD TABLES VERIFIED!")