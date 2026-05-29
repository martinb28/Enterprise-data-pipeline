"""
Silver Layer — Bronze to Silver Transformation.

Reads raw CSV files from the Bronze layer, applies:
  - Schema enforcement
  - Data cleaning (nulls, duplicates, type casting)
  - Table joins (optimized with broadcast for dimension tables)
  - Partitioned Parquet write to Silver layer

Azure equivalent: Databricks notebook run via ADF pipeline.
"""

from datetime import date
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType
)

from src.utils.config import get_config, get_storage_path
from src.utils.logger import get_logger, log_dataframe_stats, log_stage, pipeline_step
from src.utils.spark_session import get_or_create_spark

logger = get_logger(__name__)

# ─────────────────────────────────────────────────────────────────
# Schema Definitions — Explicit schemas avoid Spark schema inference
# (better performance + fail-fast on schema drift)
# ─────────────────────────────────────────────────────────────────

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("order_status", StringType(), nullable=True),
    StructField("order_purchase_timestamp", StringType(), nullable=True),
    StructField("order_approved_at", StringType(), nullable=True),
    StructField("order_delivered_carrier_date", StringType(), nullable=True),
    StructField("order_delivered_customer_date", StringType(), nullable=True),
    StructField("order_estimated_delivery_date", StringType(), nullable=True),
])

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("customer_unique_id", StringType(), nullable=True),
    StructField("customer_zip_code_prefix", StringType(), nullable=True),
    StructField("customer_city", StringType(), nullable=True),
    StructField("customer_state", StringType(), nullable=True),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("order_item_id", IntegerType(), nullable=True),
    StructField("product_id", StringType(), nullable=True),
    StructField("seller_id", StringType(), nullable=True),
    StructField("shipping_limit_date", StringType(), nullable=True),
    StructField("price", DoubleType(), nullable=True),
    StructField("freight_value", DoubleType(), nullable=True),
])

ORDER_PAYMENTS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("payment_sequential", IntegerType(), nullable=True),
    StructField("payment_type", StringType(), nullable=True),
    StructField("payment_installments", IntegerType(), nullable=True),
    StructField("payment_value", DoubleType(), nullable=True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("product_category_name", StringType(), nullable=True),
    StructField("product_name_length", IntegerType(), nullable=True),
    StructField("product_description_length", IntegerType(), nullable=True),
    StructField("product_photos_qty", IntegerType(), nullable=True),
    StructField("product_weight_g", IntegerType(), nullable=True),
    StructField("product_length_cm", IntegerType(), nullable=True),
    StructField("product_height_cm", IntegerType(), nullable=True),
    StructField("product_width_cm", IntegerType(), nullable=True),
])

SELLERS_SCHEMA = StructType([
    StructField("seller_id", StringType(), nullable=False),
    StructField("seller_zip_code_prefix", StringType(), nullable=True),
    StructField("seller_city", StringType(), nullable=True),
    StructField("seller_state", StringType(), nullable=True),
])

REVIEWS_SCHEMA = StructType([
    StructField("review_id", StringType(), nullable=False),
    StructField("order_id", StringType(), nullable=False),
    StructField("review_score", IntegerType(), nullable=True),
    StructField("review_creation_date", StringType(), nullable=True),
    StructField("review_answer_timestamp", StringType(), nullable=True),
])


class BronzeToSilverTransformer:
    """
    Reads raw Bronze data and produces clean, enriched Silver tables.

    Key optimizations demonstrated:
    - Explicit schemas (no schema inference)
    - Broadcast joins for small dimension tables (products, sellers)
    - Adaptive Query Execution (AQE) enabled
    - Partitioned writes for efficient downstream reads
    - Deduplication using dropDuplicates on primary keys
    """

    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark or get_or_create_spark("bronze-to-silver")
        self.config = get_config()

    def _read_csv(self, bucket: str, date_str: str, filename: str, schema: StructType) -> DataFrame:
        """
        Reads a raw CSV from the Bronze layer with explicit schema.

        Args:
            bucket: S3 bucket name.
            date_str: Date partition string.
            filename: CSV filename.
            schema: Explicit schema to apply.

        Returns:
            Spark DataFrame.
        """
        path = f"s3a://{bucket}/olist/raw/{date_str}/{filename}"
        logger.info(f"Reading Bronze: {path}")

        df = (
            self.spark.read
            .option("header", "true")
            .option("mode", "PERMISSIVE")   # Don't fail on bad rows
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .schema(schema)
            .csv(path)
        )

        # Filter out corrupt rows
        if "_corrupt_record" in df.columns:
            corrupt_count = df.filter(F.col("_corrupt_record").isNotNull()).count()
            if corrupt_count > 0:
                logger.warning(f"⚠️  {corrupt_count} corrupt rows found in {filename}, dropping")
            df = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

        return df

    @pipeline_step("Transform: Orders Enriched")
    def transform_orders(self, date_str: str) -> DataFrame:
        """
        Creates the enriched orders Silver table.

        Joins:
            orders ← customers (broadcast, small dimension)

        Transformations:
            - Cast string timestamps to proper TimestampType
            - Calculate delivery delay in days
            - Calculate order total amount from items + payments
            - Add ingestion_date partition column

        Returns:
            Enriched orders DataFrame.
        """
        bucket = self.config["storage"]["buckets"]["bronze"]

        # Read fact and dimension tables
        orders_raw = self._read_csv(bucket, date_str, "olist_orders_dataset.csv", ORDERS_SCHEMA)
        customers_raw = self._read_csv(bucket, date_str, "olist_customers_dataset.csv", CUSTOMERS_SCHEMA)
        payments_raw = self._read_csv(bucket, date_str, "olist_order_payments_dataset.csv", ORDER_PAYMENTS_SCHEMA)
        items_raw = self._read_csv(bucket, date_str, "olist_order_items_dataset.csv", ORDER_ITEMS_SCHEMA)

        # ── Deduplicate ────────────────────────────────────────────
        orders = orders_raw.dropDuplicates(["order_id"])
        customers = customers_raw.dropDuplicates(["customer_id"])

        # ── Cast timestamps ────────────────────────────────────────
        ts_cols = [
            "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ]
        for col in ts_cols:
            orders = orders.withColumn(
                col,
                F.to_timestamp(F.col(col), "yyyy-MM-dd HH:mm:ss")
            )

        # ── Aggregate payment amounts per order ────────────────────
        payment_agg = (
            payments_raw
            .groupBy("order_id")
            .agg(
                F.sum("payment_value").alias("total_payment_value"),
                F.count("payment_sequential").alias("payment_count"),
                F.first("payment_type").alias("primary_payment_type"),
            )
        )

        # ── Aggregate item amounts per order ───────────────────────
        items_agg = (
            items_raw
            .groupBy("order_id")
            .agg(
                F.sum("price").alias("total_items_price"),
                F.sum("freight_value").alias("total_freight_value"),
                F.count("order_item_id").alias("total_items_count"),
            )
        )

        # ── Join orders with customers using BROADCAST ─────────────
        # Broadcast is optimal here because customers is a dimension table
        # This avoids a shuffle join and significantly speeds up execution
        orders_with_customers = orders.join(
            F.broadcast(customers),   # ← Broadcast hint: small table replicated to all workers
            on="customer_id",
            how="left",
        )

        # ── Join with payment and item aggregations ─────────────────
        enriched = (
            orders_with_customers
            .join(payment_agg, on="order_id", how="left")
            .join(items_agg, on="order_id", how="left")
        )

        # ── Derived columns ─────────────────────────────────────────
        enriched = (
            enriched
            .withColumn(
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
            .withColumn(
                "is_delivered_on_time",
                F.when(F.col("delivery_delay_days") <= 0, True)
                .when(F.col("delivery_delay_days") > 0, False)
                .otherwise(None)
            )
            .withColumn("order_year", F.year("order_purchase_timestamp"))
            .withColumn("order_month", F.month("order_purchase_timestamp"))
            .withColumn("ingestion_date", F.lit(date_str))
        )

        # ── Filter out rows with no order_id ───────────────────────
        enriched = enriched.filter(F.col("order_id").isNotNull())

        log_dataframe_stats(logger, enriched, "orders_enriched")
        return enriched

    @pipeline_step("Transform: Products Enriched")
    def transform_products(self, date_str: str) -> DataFrame:
        """
        Creates the enriched products Silver table.

        Joins:
            products ← category_translation (broadcast)

        Returns:
            Enriched products DataFrame.
        """
        bucket = self.config["storage"]["buckets"]["bronze"]
        products = self._read_csv(bucket, date_str, "olist_products_dataset.csv", PRODUCTS_SCHEMA)

        # Attempt to load translation table
        try:
            translation_schema = StructType([
                StructField("product_category_name", StringType(), True),
                StructField("product_category_name_english", StringType(), True),
            ])
            translation = self._read_csv(
                bucket, date_str,
                "product_category_name_translation.csv",
                translation_schema
            )
            products = products.join(
                F.broadcast(translation),
                on="product_category_name",
                how="left"
            )
        except Exception:
            logger.warning("Category translation file not found — using original category names")
            products = products.withColumn(
                "product_category_name_english",
                F.col("product_category_name")
            )

        products = (
            products
            .dropDuplicates(["product_id"])
            .filter(F.col("product_id").isNotNull())
            .withColumn("ingestion_date", F.lit(date_str))
        )

        log_dataframe_stats(logger, products, "products_enriched")
        return products

    @pipeline_step("Transform: Sellers Enriched")
    def transform_sellers(self, date_str: str) -> DataFrame:
        """
        Creates the enriched sellers Silver table with review metrics.

        Returns:
            Enriched sellers DataFrame.
        """
        bucket = self.config["storage"]["buckets"]["bronze"]
        sellers = self._read_csv(bucket, date_str, "olist_sellers_dataset.csv", SELLERS_SCHEMA)
        items = self._read_csv(bucket, date_str, "olist_order_items_dataset.csv", ORDER_ITEMS_SCHEMA)
        reviews = self._read_csv(bucket, date_str, "olist_order_reviews_dataset.csv", REVIEWS_SCHEMA)

        # Aggregate seller metrics from order items
        seller_items = (
            items.groupBy("seller_id")
            .agg(
                F.count("order_id").alias("total_orders"),
                F.sum("price").alias("total_revenue"),
                F.avg("price").alias("avg_item_price"),
            )
        )

        # Aggregate review scores per seller (via orders → items → sellers join)
        seller_reviews = (
            reviews
            .join(items.select("order_id", "seller_id").dropDuplicates(), on="order_id", how="inner")
            .groupBy("seller_id")
            .agg(F.avg("review_score").alias("avg_review_score"))
        )

        enriched = (
            sellers
            .dropDuplicates(["seller_id"])
            .join(F.broadcast(seller_items), on="seller_id", how="left")
            .join(F.broadcast(seller_reviews), on="seller_id", how="left")
            .withColumn("ingestion_date", F.lit(date_str))
        )

        log_dataframe_stats(logger, enriched, "sellers_enriched")
        return enriched

    def _write_silver(self, df: DataFrame, path_key: str, partition_cols: list = None) -> None:
        """
        Writes a DataFrame to the Silver layer as Parquet (partitioned).

        Args:
            df: DataFrame to write.
            path_key: Config key for the output path.
            partition_cols: Columns to partition by.
        """
        output_path = get_storage_path("silver", path_key)
        logger.info(f"Writing Silver: {output_path}")

        writer = df.write.mode("overwrite").format("parquet")

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.save(output_path)
        logger.info(f"✅ Written to Silver: {output_path}")

    def run(self, ingestion_date: date = None) -> None:
        """
        Executes the full Bronze → Silver transformation pipeline.

        Args:
            ingestion_date: Date partition to process. Defaults to today.
        """
        if ingestion_date is None:
            ingestion_date = date.today()

        date_str = ingestion_date.strftime("%Y-%m-%d")

        with log_stage(logger, f"Bronze → Silver Pipeline | date={date_str}"):
            # Transform all tables
            orders_df = self.transform_orders(date_str)
            products_df = self.transform_products(date_str)
            sellers_df = self.transform_sellers(date_str)

            # Write to Silver (partitioned by year/month for efficient queries)
            self._write_silver(orders_df, "silver_orders", partition_cols=["order_year", "order_month"])
            self._write_silver(products_df, "silver_products")
            self._write_silver(sellers_df, "silver_sellers")

            logger.info("🎉 Bronze → Silver pipeline complete")


if __name__ == "__main__":
    transformer = BronzeToSilverTransformer()
    transformer.run()
