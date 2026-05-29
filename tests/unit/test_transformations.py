"""
Unit tests for PySpark transformations.

Tests use small in-memory DataFrames (no external dependencies required).
Runs in CI/CD via GitHub Actions without a running Spark cluster.
"""

import pytest
from datetime import date
from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType
)


@pytest.fixture(scope="session")
def spark():
    """Creates a local SparkSession for testing (no cluster needed)."""
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-enterprise-etl")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


# ─────────────────────────────────────────────────────────────────
# Fixtures — Sample DataFrames
# ─────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_orders(spark):
    """Sample orders DataFrame for testing."""
    data = [
        ("order_001", "cust_001", "delivered", "2024-01-15 10:00:00", "2024-01-15 11:00:00",
         "2024-01-17 09:00:00", "2024-01-20 14:00:00", "2024-01-22 00:00:00"),
        ("order_002", "cust_002", "delivered", "2024-02-10 15:30:00", "2024-02-10 16:00:00",
         "2024-02-12 08:00:00", "2024-02-15 12:00:00", "2024-02-18 00:00:00"),
        ("order_003", "cust_001", "canceled", "2024-03-05 09:15:00", None, None, None,
         "2024-03-20 00:00:00"),
        ("order_004", "cust_003", "delivered", "2024-03-20 18:00:00", "2024-03-20 19:00:00",
         "2024-03-22 10:00:00", "2024-03-25 16:00:00", "2024-03-23 00:00:00"),  # Late delivery
    ]

    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", StringType(), True),
        StructField("order_approved_at", StringType(), True),
        StructField("order_delivered_carrier_date", StringType(), True),
        StructField("order_delivered_customer_date", StringType(), True),
        StructField("order_estimated_delivery_date", StringType(), True),
    ])

    df = spark.createDataFrame(data, schema)

    # Cast timestamps (mirrors production code)
    ts_cols = [
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for col in ts_cols:
        df = df.withColumn(col, F.to_timestamp(F.col(col), "yyyy-MM-dd HH:mm:ss"))

    return df


@pytest.fixture
def sample_customers(spark):
    """Sample customers DataFrame for testing."""
    data = [
        ("cust_001", "unique_001", "01310", "São Paulo", "SP"),
        ("cust_002", "unique_002", "20040", "Rio de Janeiro", "RJ"),
        ("cust_003", "unique_003", "30112", "Belo Horizonte", "MG"),
    ]
    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_unique_id", StringType(), True),
        StructField("customer_zip_code_prefix", StringType(), True),
        StructField("customer_city", StringType(), True),
        StructField("customer_state", StringType(), True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_payments(spark):
    """Sample order payments DataFrame for testing."""
    data = [
        ("order_001", 1, "credit_card", 3, 350.50),
        ("order_002", 1, "boleto", 1, 120.00),
        ("order_003", 1, "credit_card", 6, 500.00),
        ("order_004", 1, "debit_card", 1, 80.00),
    ]
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("payment_sequential", IntegerType(), True),
        StructField("payment_type", StringType(), True),
        StructField("payment_installments", IntegerType(), True),
        StructField("payment_value", DoubleType(), True),
    ])
    return spark.createDataFrame(data, schema)


# ─────────────────────────────────────────────────────────────────
# Tests — Data Transformations
# ─────────────────────────────────────────────────────────────────

class TestOrderTransformations:
    """Tests for Bronze → Silver order transformations."""

    def test_order_customer_join_preserves_all_orders(self, spark, sample_orders, sample_customers):
        """
        Left join orders ↔ customers should preserve all orders,
        even if customer info is missing.
        """
        joined = sample_orders.join(
            F.broadcast(sample_customers),
            on="customer_id",
            how="left"
        )
        assert joined.count() == sample_orders.count(), \
            "Left join should not drop any orders"

    def test_delivery_delay_calculation(self, spark, sample_orders):
        """
        Delivery delay should be positive for late deliveries (actual > estimated)
        and negative for early deliveries.
        """
        with_delay = sample_orders.withColumn(
            "delivery_delay_days",
            F.when(
                F.col("order_delivered_customer_date").isNotNull() &
                F.col("order_estimated_delivery_date").isNotNull(),
                F.datediff(
                    F.col("order_delivered_customer_date"),
                    F.col("order_estimated_delivery_date")
                )
            ).otherwise(F.lit(None))
        )

        # order_004: delivered 2024-03-25, estimated 2024-03-23 → delay = +2 days
        late_order = with_delay.filter(F.col("order_id") == "order_004")
        delay = late_order.collect()[0]["delivery_delay_days"]
        assert delay == 2, f"Expected delay=2 for late order, got {delay}"

        # order_001: delivered 2024-01-20, estimated 2024-01-22 → delay = -2 days
        early_order = with_delay.filter(F.col("order_id") == "order_001")
        delay = early_order.collect()[0]["delivery_delay_days"]
        assert delay == -2, f"Expected delay=-2 for early order, got {delay}"

    def test_canceled_orders_have_null_delivery_date(self, spark, sample_orders):
        """Canceled orders should have no delivery date."""
        canceled = sample_orders.filter(F.col("order_status") == "canceled")
        assert canceled.count() == 1, "Should have exactly 1 canceled order in test data"

        delivery_date = canceled.collect()[0]["order_delivered_customer_date"]
        assert delivery_date is None, "Canceled orders must have null delivery date"

    def test_no_duplicate_order_ids(self, spark, sample_orders):
        """After deduplication, no duplicate order_ids should exist."""
        deduped = sample_orders.dropDuplicates(["order_id"])
        assert deduped.count() == sample_orders.count(), \
            "Test data should not contain duplicates"

    def test_payment_aggregation(self, spark, sample_payments):
        """Payment aggregation should produce one row per order with correct sum."""
        agg = (
            sample_payments
            .groupBy("order_id")
            .agg(
                F.sum("payment_value").alias("total_payment_value"),
                F.count("payment_sequential").alias("payment_count"),
            )
        )

        assert agg.count() == 4, "Should have 4 distinct orders in payments"

        # Check order_001 payment value
        order_001_value = agg.filter(
            F.col("order_id") == "order_001"
        ).collect()[0]["total_payment_value"]
        assert abs(order_001_value - 350.50) < 0.01, \
            f"Expected 350.50 for order_001, got {order_001_value}"

    def test_order_timestamp_cast(self, spark, sample_orders):
        """Timestamp columns should be properly cast from string."""
        # If cast succeeded, timestamp should not be null for non-null inputs
        non_null_timestamps = sample_orders.filter(
            F.col("order_purchase_timestamp").isNotNull()
        )
        assert non_null_timestamps.count() == sample_orders.count(), \
            "All purchase timestamps should parse successfully"


class TestDataEnrichment:
    """Tests for derived/enriched columns."""

    def test_order_year_month_extraction(self, spark, sample_orders):
        """order_year and order_month should be correctly extracted from timestamp."""
        enriched = (
            sample_orders
            .withColumn("order_year", F.year("order_purchase_timestamp"))
            .withColumn("order_month", F.month("order_purchase_timestamp"))
        )

        # order_001: purchased 2024-01-15
        row = enriched.filter(F.col("order_id") == "order_001").collect()[0]
        assert row["order_year"] == 2024, f"Expected year=2024, got {row['order_year']}"
        assert row["order_month"] == 1, f"Expected month=1, got {row['order_month']}"

    def test_on_time_delivery_flag(self, spark, sample_orders):
        """is_delivered_on_time should be True for early/on-time, False for late."""
        with_delay = sample_orders.withColumn(
            "delivery_delay_days",
            F.when(
                F.col("order_delivered_customer_date").isNotNull(),
                F.datediff(
                    F.col("order_delivered_customer_date"),
                    F.col("order_estimated_delivery_date")
                )
            ).otherwise(F.lit(None))
        ).withColumn(
            "is_delivered_on_time",
            F.when(F.col("delivery_delay_days") <= 0, True)
            .when(F.col("delivery_delay_days") > 0, False)
            .otherwise(None)
        )

        # order_004: late (delay = +2)
        late = with_delay.filter(F.col("order_id") == "order_004").collect()[0]
        assert late["is_delivered_on_time"] is False, "Late delivery should be False"

        # order_001: early (delay = -2)
        early = with_delay.filter(F.col("order_id") == "order_001").collect()[0]
        assert early["is_delivered_on_time"] is True, "Early delivery should be True"


class TestBroadcastJoin:
    """Tests verifying broadcast join behavior."""

    def test_broadcast_join_does_not_drop_rows(self, spark, sample_orders, sample_customers):
        """Broadcast join should preserve all fact table rows."""
        normal_join = sample_orders.join(sample_customers, on="customer_id", how="left")
        broadcast_join = sample_orders.join(
            F.broadcast(sample_customers), on="customer_id", how="left"
        )

        assert normal_join.count() == broadcast_join.count(), \
            "Broadcast join must produce same row count as regular join"

    def test_broadcast_join_schema_matches(self, spark, sample_orders, sample_customers):
        """Broadcast join schema should include all columns from both tables."""
        joined = sample_orders.join(
            F.broadcast(sample_customers), on="customer_id", how="left"
        )
        expected_cols = set(sample_orders.columns) | set(sample_customers.columns)
        actual_cols = set(joined.columns)
        assert expected_cols == actual_cols, \
            f"Missing columns: {expected_cols - actual_cols}"
