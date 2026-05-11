# Databricks notebook source
#Setup
adls_key = dbutils.secrets.get(
             scope = "property-pulse-scope",
             key   = "adls-account-key"
           )

spark.conf.set(
    "fs.azure.account.key.propertypulsedl.dfs.core.windows.net",
    adls_key
)

base           = "abfss://property-data@propertypulsedl.dfs.core.windows.net"
silver_path    = f"{base}/silver/property_events_clean"
agent_scd2_path= f"{base}/reference/agent_master_scd2"

# Gold output paths
gold_path_5    = f"{base}/gold/agent_performance"
gold_path_6    = f"{base}/gold/agent_vs_city_benchmark"
gold_path_7    = f"{base}/gold/top_agents_by_tier"
gold_path_8    = f"{base}/gold/agent_city_movement_impact"

print("✅ Setup complete!")

# COMMAND ----------

#load silver tables and agent tables
from pyspark.sql.functions import col, to_timestamp

silver_df = spark.read.format("delta")\
    .load(silver_path)

agent_df = spark.read.format("delta")\
    .load(agent_scd2_path)\
        .filter(col("is_current") == True)

#register as temo views!

silver_df.createOrReplaceTempView("silver_property_events")
agent_df.createOrReplaceTempView("agent_master")

print(f" Silver records: {silver_df.count()}")
print(f"Current agents : {agent_df.count()}")
print(f"Silver Schema:")
silver_df.printSchema()
print("\n Agent Schema: ")
agent_df.printSchema()

# COMMAND ----------

# Buildng agent performance

agent_performance = spark.sql("""
    SELECT
        s.agent_id,
        a.agent_name,
        a.base_city,
        a.tier,
        a.rating,
        a.total_deals,
        COUNT(s.listing_id) AS total_listings,
        COUNT(CASE WHEN s.event_type = 'NEW_LISTING' THEN 1 END) AS new_listings,
        COUNT(CASE WHEN s.event_type = 'PRICE_CHANGE' THEN 1 END) AS price_changes,
        COUNT(CASE WHEN s.event_type = 'REMOVAL' THEN 1 END) AS removals,
        ROUND(AVG(CASE WHEN s.price > 0 THEN s.price END), 2) AS avg_listing_price,
        MAX(s.price) AS max_listing_price,
        MIN(CASE WHEN s.price > 0 THEN s.price END) AS min_listing_price
    FROM silver_property_events s
    INNER JOIN agent_master a ON s.agent_id = a.agent_id
    GROUP BY
        s.agent_id,
        a.agent_name,
        a.base_city,
        a.tier,
        a.rating,
        a.total_deals
    ORDER BY total_listings DESC
""")

print(f" Agent performace rows :{agent_performance.count()}")
display(agent_performance)

agent_performance.write\
    .format("delta")\
    .mode("overwrite")\
    .save(gold_path_5)

print("Agent_performance written!")

# COMMAND ----------

# ══════════════════════════════════════
# GOLD TABLE 6 — AGENT VS BENCHMARK
# Is agent pricing above/below average?
# ══════════════════════════════════════

print("📊 Building agent_vs_city_benchmark...")

agent_vs_benchmark = spark.sql("""
    WITH city_avg AS (
        SELECT
            city,
            ROUND(AVG(price), 2)
                AS city_avg_price
        FROM silver_property_events
        WHERE price > 0
        AND event_type != 'REMOVED'
        GROUP BY city
    ),
    agent_avg AS (
        SELECT
            s.agent_id,
            a.agent_name,
            a.tier,
            s.city,
            ROUND(AVG(s.price), 2)
                AS agent_avg_price,
            COUNT(s.listing_id)
                AS total_listings
        FROM silver_property_events s
        INNER JOIN agent_master a
            ON s.agent_id = a.agent_id
        WHERE s.price > 0
        AND s.event_type != 'REMOVED'
        GROUP BY
            s.agent_id,
            a.agent_name,
            a.tier,
            s.city
    )
    SELECT
        aa.agent_id,
        aa.agent_name,
        aa.tier,
        aa.city,
        aa.agent_avg_price,
        ca.city_avg_price,
        aa.total_listings,
        ROUND(
            aa.agent_avg_price
            - ca.city_avg_price, 2)
            AS price_diff,
        ROUND((
            aa.agent_avg_price
            - ca.city_avg_price)
            / ca.city_avg_price * 100, 2)
            AS pct_above_city_avg,
        CASE
            WHEN aa.agent_avg_price
                 > ca.city_avg_price
            THEN 'ABOVE AVERAGE'
            WHEN aa.agent_avg_price
                 < ca.city_avg_price
            THEN 'BELOW AVERAGE'
            ELSE 'AT AVERAGE'
        END AS performance_flag
    FROM agent_avg aa
    INNER JOIN city_avg ca
        ON aa.city = ca.city
    ORDER BY pct_above_city_avg DESC
""")

print(f"✅ Benchmark rows: {agent_vs_benchmark.count()}")
display(agent_vs_benchmark)

agent_vs_benchmark.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path_6)

print("✅ agent_vs_city_benchmark written!")

# COMMAND ----------

# ══════════════════════════════════════
# GOLD TABLE 7 — TOP AGENTS BY TIER
# Window functions + JOIN!
# ══════════════════════════════════════

print("📊 Building top_agents_by_tier...")

top_agents_by_tier = spark.sql("""
    WITH agent_metrics AS (
        SELECT
            s.agent_id,
            a.agent_name,
            a.tier,
            a.rating,
            a.base_city,
            COUNT(s.listing_id)
                AS total_listings,
            ROUND(AVG(
                CASE WHEN s.price > 0
                THEN s.price END), 2)
                AS avg_price
        FROM silver_property_events s
        INNER JOIN agent_master a
            ON s.agent_id = a.agent_id
        GROUP BY
            s.agent_id,
            a.agent_name,
            a.tier,
            a.rating,
            a.base_city
    )
    SELECT
        tier,
        agent_id,
        agent_name,
        base_city,
        rating,
        total_listings,
        avg_price,
        RANK() OVER (
            PARTITION BY tier
            ORDER BY total_listings DESC
        ) AS rank_within_tier,
        ROUND(
            total_listings * 100.0
            / SUM(total_listings)
              OVER (PARTITION BY tier),
            2
        ) AS pct_of_tier_listings
    FROM agent_metrics
    ORDER BY tier, rank_within_tier
""")

print(f"✅ Top agents rows: {top_agents_by_tier.count()}")
top_agents_by_tier.show(truncate=False)

top_agents_by_tier.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path_7)

print("✅ top_agents_by_tier written!")

# COMMAND ----------

# ══════════════════════════════════════
# GOLD TABLE 8 — CITY MOVEMENT IMPACT
# Did moving city affect performance?
# Uses SCD2 history!
# ══════════════════════════════════════

print("📊 Building agent_city_movement_impact...")

# Load ALL SCD2 records
# Including historical!
all_agents_df = spark.read \
    .format("delta") \
    .load(agent_scd2_path)

all_agents_df.createOrReplaceTempView(
    "agent_master_all_versions"
)

city_movement_impact = spark.sql("""
    WITH agent_city_performance AS (
        SELECT
            s.agent_id,
            s.city             AS listing_city,
            COUNT(s.listing_id) AS listings_in_city,
            ROUND(AVG(
                CASE WHEN s.price > 0
                THEN s.price END), 2)
                AS avg_price_in_city
        FROM silver_property_events s
        GROUP BY
            s.agent_id,
            s.city
    ),
    agents_who_moved AS (
        SELECT DISTINCT agent_id
        FROM agent_master_all_versions
        GROUP BY agent_id
        HAVING COUNT(*) > 1
    )
    SELECT
        acp.agent_id,
        a.agent_name,
        acp.listing_city,
        acp.listings_in_city,
        acp.avg_price_in_city,
        CASE
            WHEN am.agent_id IS NOT NULL
            THEN 'MOVED CITY'
            ELSE 'STABLE'
        END AS agent_status,
        a.scd_version,
        a.effective_start_date,
        a.effective_end_date
    FROM agent_city_performance acp
    INNER JOIN agent_master_all_versions a
        ON acp.agent_id = a.agent_id
    LEFT JOIN agents_who_moved am
        ON acp.agent_id = am.agent_id
    ORDER BY
        acp.agent_id,
        a.scd_version
""")

print(f"✅ Movement impact rows: {city_movement_impact.count()}")
city_movement_impact.show(truncate=False)

city_movement_impact.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path_8)

print("✅ agent_city_movement_impact written!")

# COMMAND ----------

# ══════════════════════════════════════
# FINAL VERIFICATION
# ══════════════════════════════════════

print("🏆 GOLD JOINS — FINAL VERIFICATION")
print("─" * 40)

g5 = spark.read.format("delta").load(gold_path_5)
g6 = spark.read.format("delta").load(gold_path_6)
g7 = spark.read.format("delta").load(gold_path_7)
g8 = spark.read.format("delta").load(gold_path_8)

print(f"✅ agent_performance           : {g5.count()} rows")
print(f"✅ agent_vs_city_benchmark     : {g6.count()} rows")
print(f"✅ top_agents_by_tier          : {g7.count()} rows")
print(f"✅ agent_city_movement_impact  : {g8.count()} rows")

print("─" * 40)
print("🏆 ALL JOIN GOLD TABLES COMPLETE!")