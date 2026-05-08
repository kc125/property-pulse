# tests/test_transformations.py

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim, when

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local") \
        .appName("property-pulse-tests") \
        .getOrCreate()

def test_event_type_uppercase(spark):
    """Test event_type is uppercased correctly"""
    data = [("new_listing",), ("price_change",)]
    df = spark.createDataFrame(data, ["event_type"])

    result = df.withColumn(
        "event_type",
        upper(trim(col("event_type")))
    )
    rows = result.collect()

    assert rows[0]["event_type"] == "NEW_LISTING"
    assert rows[1]["event_type"] == "PRICE_CHANGE"
    print("✅ event_type uppercase test passed!")


def test_null_price_replaced(spark):
    """Test null prices replaced with 0"""
    data = [(None,), (5000000,)]
    df = spark.createDataFrame(
            data,
            ["price"]
         )

    result = df.withColumn(
        "price",
        when(col("price").isNull(), 0)
        .otherwise(col("price"))
    )
    rows = result.collect()

    assert rows[0]["price"] == 0
    assert rows[1]["price"] == 5000000
    print("✅ null price replacement test passed!")


def test_removed_events_filtered(spark):
    """Test REMOVED events filtered from price calcs"""
    data = [
        ("NEW_LISTING",  5000000),
        ("REMOVED",      0),
        ("PRICE_CHANGE", 7000000)
    ]
    df = spark.createDataFrame(
            data,
            ["event_type", "price"]
         )

    result = df.filter(
        col("event_type") != "REMOVED"
    )

    assert result.count() == 2
    print("✅ REMOVED events filter test passed!")


def test_price_cast_to_long(spark):
    """Test price is correctly cast to long type"""
    data = [("8500000",), ("12000000",)]
    df = spark.createDataFrame(data, ["price"])

    result = df.withColumn(
        "price",
        col("price").cast("long")
    )

    assert result.schema["price"].dataType.typeName() == "long"
    print("✅ price cast to long test passed!")


def test_duplicate_removal(spark):
    """Test duplicate records are removed"""
    data = [
        ("LST-001", "NEW_LISTING"),
        ("LST-001", "NEW_LISTING"),  # duplicate!
        ("LST-002", "PRICE_CHANGE")
    ]
    df = spark.createDataFrame(
            data,
            ["listing_id", "event_type"]
         )

    result = df.dropDuplicates(
        ["listing_id", "event_type"]
    )

    assert result.count() == 2
    print("✅ duplicate removal test passed!")
