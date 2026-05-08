# Databricks notebook source
## import & secrets


## Final test - Azure CLI ppush  - Testing CLI workflow

from pyspark.sql.functions import(
    col, avg, count, max, min, sum, lit, udf, explode, split, length, trim, regexp_replace,
    round, date_trunc, desc, to_timestamp
)

adls_key = dbutils.secrets.get(
    scope = "property-pulse-scope",
    key = "adls-account-key"
)

adls_name = dbutils.secrets.get(
    scope = "property-pulse-scope",
    key = "adls-account-name"
)

spark.conf.set(f"fs.azure.account.key.{adls_name}.dfs.core.windows.net", adls_key)

print(f" Secrets loaded")

# COMMAND ----------

silver_path = f"abfss://property-data@{adls_name}.dfs.core.windows.net/silver/property_events_clean"

gold_path_1 = f"abfss://property-data@{adls_name}.dfs.core.windows.net/gold/avg_price_by_city"
gold_path_2 = f"abfss://property-data@{adls_name}.dfs.core.windows.net/gold/listing_count_by_event"
gold_path_3 = f"abfss://property-data@{adls_name}.dfs.core.windows.net/gold/price_trend_by_locality"
gold_path_4 = f"abfss://property-data@{adls_name}.dfs.core.windows.net/gold/top_active_cities"

silver_df = spark.read\
    .format("delta")\
        .load(silver_path)

print(f" Silver record Count: {silver_df.count()}")
silver_df.show(3,truncate=False)


# COMMAND ----------

#Average price by city
silver_df.createOrReplaceTempView("silver_property_events")
print(f" Temp view created : silver_property_events")

avg_price_city = spark.sql("""
                           SELECT
                           city,
                           ROUND(AVG(price),2) AS avg_price,
                           MAX(price) AS max_price,
                           MIN(price) AS min_price,
                           COUNT(listing_id) AS total_listings
                           FROM
                           silver_property_events
                           WHERE
                           event_type != 'REMOVED'
                           AND price > 0
                           GROUP BY city
                           ORDER BY avg_price DESC""")

print(f" Gold Table 1 - Avg price by city:")
avg_price_city.show(truncate=False)

avg_price_city.write\
    .format("delta")\
        .mode("overwrite")\
            .save(gold_path_1)

print(f" avg_price_by_city written!")

# COMMAND ----------

# listing by event_type

listing_count = spark.sql("""
                          SELECT
                          event_type,
                          COUNT(listing_id) AS total_count
                          FROM
                          silver_property_events
                          GROUP BY 
                          event_type
                          ORDER BY
                          total_count DESC
                          """)

print("Gold table 2 -Listing count by event type:")
listing_count.show(truncate= False)

listing_count.write\
    .format("delta")\
        .mode("overwrite")\
            .save(gold_path_2)

print(f" Listing count by event type written ")

# COMMAND ----------

# DBTITLE 1,Untitled
#price trend by locality

price_trend = spark.sql("""
                        SELECT
                        city,
                        locality,
                        DATE_TRUNC('DAY', event_timestamp) AS event_date,
                        ROUND(AVG(price), 2) AS avg_price,
                        COUNT(listing_id) AS listing_count
                        FROM
                        silver_property_events
                        WHERE event_type != 'REMOVED'
                        AND price > 0
                        GROUP BY 
                        city,
                        locality,
                        DATE_TRUNC('DAY', event_timestamp)
                        ORDER BY
                        event_date,
                        avg_price DESC
                        """)

print("Gold table 3 - Price trend by locality:")
price_trend.show(truncate= False)

price_trend.write\
    .format("delta")\
        .mode("overwrite")\
            .save(gold_path_3)

print(f"Price trend by locality written")
                
                

# COMMAND ----------

## top active cities

top_cities = spark.sql("""
                       SELECT
                       city,
                       COUNT(listing_id) AS total_events,
                       COUNT(CASE WHEN event_type = 'NEW_LISTING'
                       THEN 1 END) AS new_listings,
                       COUNT(CASE WHEN event_type = 'PRICE_CHANGE'
                       THEN 1 END) AS price_changes,
                       COUNT(CASE WHEN event_type = 'REMOVED'
                       THEN 1 END) AS removals
                       FROM
                       silver_property_events
                       GROUP BY
                       city
                       ORDER BY
                       total_events DESC
                       """)
                    
print("Gold table4 - Top active Cities:")
top_cities.show(truncate=False)

top_cities.write\
    .format("delta")\
        .mode("overwrite")\
            .save(gold_path_4)

print("top cities active written!")


# COMMAND ----------

