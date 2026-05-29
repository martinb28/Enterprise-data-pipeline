"""
Unit tests for the Data Quality validation module.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from src.transformations.data_quality import DataQualityValidator, QualityCheck


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-data-quality")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def validator():
    """Returns a DataQualityValidator instance."""
    return DataQualityValidator()


@pytest.fixture
def clean_orders(spark):
    """A clean orders DataFrame with no quality issues."""
    data = [
        ("order_001", "cust_001", "delivered", 250.0, 2),
        ("order_002", "cust_002", "delivered", 120.5, 1),
        ("order_003", "cust_003", "shipped", 89.99, 3),
    ]
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_status", StringType(), True),
        StructField("total_payment_value", DoubleType(), True),
        StructField("total_items_count", IntegerType(), True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def orders_with_nulls(spark):
    """Orders DataFrame with null values for testing null checks."""
    data = [
        ("order_001", "cust_001", "delivered", 250.0, 2),
        ("order_002", None, "delivered", 120.5, 1),   # Null customer_id
        (None, "cust_003", "shipped", 89.99, 3),       # Null order_id
    ]
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("total_payment_value", DoubleType(), True),
        StructField("total_items_count", IntegerType(), True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def orders_with_duplicates(spark):
    """Orders DataFrame with duplicate order_ids."""
    data = [
        ("order_001", "cust_001", "delivered"),
        ("order_001", "cust_001", "delivered"),  # Duplicate!
        ("order_002", "cust_002", "shipped"),
    ]
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_status", StringType(), True),
    ])
    return spark.createDataFrame(data, schema)


class TestNullRatioCheck:
    """Tests for the null_ratio data quality check."""

    def test_no_nulls_passes(self, validator, clean_orders):
        """Column with no nulls should pass the null check."""
        result = validator.check_null_ratio(clean_orders, "order_id", max_ratio=0.0)
        assert result.passed is True, f"Should pass with 0 nulls, got: {result.actual}"

    def test_high_null_ratio_fails(self, validator, orders_with_nulls):
        """Column with 33% nulls should fail when threshold is 0%."""
        result = validator.check_null_ratio(orders_with_nulls, "order_id", max_ratio=0.0)
        assert result.passed is False, "Should fail: 33% nulls exceeds 0% threshold"

    def test_null_ratio_within_threshold_passes(self, validator, orders_with_nulls):
        """Column with 33% nulls should pass when threshold is 50%."""
        result = validator.check_null_ratio(orders_with_nulls, "order_id", max_ratio=0.50)
        assert result.passed is True, "Should pass: 33% nulls is below 50% threshold"

    def test_check_returns_correct_type(self, validator, clean_orders):
        """Null check should return a QualityCheck dataclass."""
        result = validator.check_null_ratio(clean_orders, "order_id")
        assert isinstance(result, QualityCheck)
        assert result.check_name == "null_ratio"
        assert result.column == "order_id"

    def test_severity_is_preserved(self, validator, orders_with_nulls):
        """Severity level should be preserved in the check result."""
        warning_result = validator.check_null_ratio(
            orders_with_nulls, "order_id", max_ratio=0.0, severity="warning"
        )
        assert warning_result.severity == "warning"

        error_result = validator.check_null_ratio(
            orders_with_nulls, "order_id", max_ratio=0.0, severity="error"
        )
        assert error_result.severity == "error"


class TestDuplicateCheck:
    """Tests for the duplicate ratio data quality check."""

    def test_no_duplicates_passes(self, validator, clean_orders):
        """DataFrame with no duplicates should pass."""
        result = validator.check_duplicates(clean_orders, ["order_id"])
        assert result.passed is True, "Should pass: no duplicates in clean data"

    def test_duplicates_fail(self, validator, orders_with_duplicates):
        """DataFrame with duplicates should fail with zero tolerance."""
        result = validator.check_duplicates(
            orders_with_duplicates, ["order_id"], max_ratio=0.0
        )
        assert result.passed is False, "Should fail: 1 out of 3 rows is duplicate"

    def test_duplicates_within_threshold_pass(self, validator, orders_with_duplicates):
        """Duplicates should pass if within the configured tolerance."""
        result = validator.check_duplicates(
            orders_with_duplicates, ["order_id"], max_ratio=0.50
        )
        assert result.passed is True, "Should pass: duplicate ratio below 50% threshold"


class TestMinRowCountCheck:
    """Tests for the minimum row count check."""

    def test_sufficient_rows_passes(self, validator, clean_orders):
        """DataFrame with enough rows should pass."""
        result = validator.check_min_row_count(clean_orders, min_rows=2)
        assert result.passed is True

    def test_too_few_rows_fails(self, validator, clean_orders):
        """DataFrame with fewer rows than minimum should fail."""
        result = validator.check_min_row_count(clean_orders, min_rows=100)
        assert result.passed is False, "Should fail: 3 rows < 100 minimum"


class TestValueRangeCheck:
    """Tests for numeric value range checks."""

    def test_values_in_range_pass(self, validator, clean_orders):
        """Values within range should pass."""
        result = validator.check_value_range(
            clean_orders, "total_payment_value", min_val=0, max_val=1000
        )
        assert result.passed is True

    def test_negative_values_fail(self, spark, validator):
        """Negative payment values should trigger a warning."""
        data = [("order_001", -50.0), ("order_002", 200.0)]
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("total_payment_value", DoubleType(), True),
        ])
        df = spark.createDataFrame(data, schema)

        result = validator.check_value_range(df, "total_payment_value", min_val=0)
        assert result.passed is False, "Negative values should fail the range check"


class TestQualityReport:
    """Tests for the QualityReport aggregation."""

    def test_report_passes_with_clean_data(self, validator, clean_orders):
        """
        Report should pass when all critical checks pass.
        Note: validate_orders expects specific columns, so we test the DQ components.
        """
        null_check = validator.check_null_ratio(clean_orders, "order_id", max_ratio=0.0)
        dup_check = validator.check_duplicates(clean_orders, ["order_id"])
        count_check = validator.check_min_row_count(clean_orders, min_rows=1)

        assert null_check.passed
        assert dup_check.passed
        assert count_check.passed

    def test_error_severity_blocks_pipeline(self, validator, orders_with_nulls):
        """Error-severity checks should indicate pipeline should stop."""
        result = validator.check_null_ratio(
            orders_with_nulls, "order_id", max_ratio=0.0, severity="error"
        )
        assert not result.passed
        assert result.severity == "error"
