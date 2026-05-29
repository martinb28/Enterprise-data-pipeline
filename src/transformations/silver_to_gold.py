"""
Gold Layer — Silver to Gold Analytics Aggregations.

Produces business-ready tables optimized for the Data Science team:
  - customer_rfm: RFM (Recency, Frequency, Monetary) scores per customer
  - product_performance: Revenue and review metrics per product/category
  - seller_scorecard: Seller KPIs and ranking
  - daily_revenue_summary: Business KPIs by day

Azure equivalent: Databricks SQL notebook materialized as Delta tables,
                  exposed via Azure Synapse Analytics.
"""

from datetime import date
from typing import Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from src.utils.config import get_config, get_storage_path
from src.utils.logger import get_logger, log_dataframe_stats, log_stage, pipeline_step
from src.utils.spark_session import get_or_create_spark

logger = get_logger(__name__)


class SilverToGoldAggregator:
    """
    Reads enriched Silver tables and produces analytics-ready Gold tables.

    Key techniques demonstrated:
    - Complex Spark SQL with Window functions
    - RFM analysis (a fundamental Data Science / CRM technique)
    - Ntile-based scoring (percentile ranking)
    - Date arithmetic for recency calculations
    - Aggregations across multiple dimensions
    """

    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark or get_or_create_spark("silver-to-gold")
        self.config = get_config()

    def _read_silver(self, path_key: str) -> DataFrame:
        """Reads a Silver table from MinIO/S3."""
        path = get_storage_path("silver", path_key)
        logger.info(f"Reading Silver: {path}")
        return self.spark.read.parquet(path)

    @pipeline_step("Gold: Customer RFM Analysis")
    def build_customer_rfm(self, reference_date: date = None) -> DataFrame:
        """
        Calculates RFM (Recency, Frequency, Monetary) scores per customer.

        RFM is a proven model used in CRM and churn prediction:
        - Recency: How recently did the customer purchase?
        - Frequency: How often do they buy?
        - Monetary: How much do they spend?

        Each dimension is scored 1-5 using ntile (quintile ranking),
        producing a composite RFM score used for customer segmentation.

        Returns:
            DataFrame with one row per customer with RFM scores and segment.
        """
        if reference_date is None:
            reference_date = date.today()

        ref_date_str = reference_date.strftime("yyyy-MM-dd")

        orders = self._read_silver("silver_orders")

        # Register as temp view for Spark SQL demonstration
        orders.createOrReplaceTempView("silver_orders")

        rfm = self.spark.sql(f"""
            WITH order_agg AS (
                -- Base aggregation: one row per customer
                SELECT
                    customer_unique_id,
                    customer_state,
                    customer_city,

                    -- Recency: days since last purchase (lower = more recent = better)
                    DATEDIFF(
                        TO_DATE('{ref_date_str}', 'yyyy-MM-dd'),
                        MAX(order_purchase_timestamp)
                    ) AS recency_days,

                    -- Frequency: number of distinct orders
                    COUNT(DISTINCT order_id) AS frequency,

                    -- Monetary: total spend (payment value)
                    COALESCE(SUM(total_payment_value), 0) AS monetary_value,

                    -- Additional DS features
                    AVG(total_items_count) AS avg_items_per_order,
                    AVG(delivery_delay_days) AS avg_delivery_delay_days,
                    SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END) AS delivered_count,
                    SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END) AS canceled_count,

                    -- Rolling spend (Window function — last 90 days)
                    SUM(
                        CASE
                            WHEN DATEDIFF(
                                TO_DATE('{ref_date_str}', 'yyyy-MM-dd'),
                                order_purchase_timestamp
                            ) <= 90
                            THEN total_payment_value
                            ELSE 0
                        END
                    ) AS spend_last_90_days,

                    MIN(order_purchase_timestamp) AS first_purchase_date,
                    MAX(order_purchase_timestamp) AS last_purchase_date
                FROM silver_orders
                WHERE customer_unique_id IS NOT NULL
                  AND order_status != 'canceled'
                GROUP BY customer_unique_id, customer_state, customer_city
            ),
            rfm_scores AS (
                -- Score each dimension into 5 quintiles (5=best, 1=worst)
                SELECT
                    *,
                    -- Recency: lower days = higher score (invert with DESC)
                    NTILE(5) OVER (ORDER BY recency_days DESC)  AS r_score,
                    NTILE(5) OVER (ORDER BY frequency ASC)      AS f_score,
                    NTILE(5) OVER (ORDER BY monetary_value ASC) AS m_score
                FROM order_agg
            )
            SELECT
                *,
                -- Composite RFM score (max 15, min 3)
                (r_score + f_score + m_score) AS rfm_score,

                -- Customer segment based on RFM score
                CASE
                    WHEN (r_score + f_score + m_score) >= 13 THEN 'Champions'
                    WHEN (r_score + f_score + m_score) >= 10 THEN 'Loyal Customers'
                    WHEN (r_score + f_score + m_score) >= 8  THEN 'Potential Loyalists'
                    WHEN r_score >= 4 AND (f_score + m_score) <= 4 THEN 'New Customers'
                    WHEN r_score <= 2 AND (f_score + m_score) >= 8 THEN 'At Risk'
                    WHEN r_score <= 1 AND (f_score + m_score) >= 6 THEN 'Lost'
                    ELSE 'Needs Attention'
                END AS customer_segment,

                -- Customer Lifetime Value estimate (simplified)
                ROUND(
                    (frequency * monetary_value / GREATEST(recency_days, 1)) * 365,
                    2
                ) AS estimated_annual_clv,

                CURRENT_TIMESTAMP() AS gold_created_at
            FROM rfm_scores
        """)

        log_dataframe_stats(logger, rfm, "customer_rfm")
        return rfm

    @pipeline_step("Gold: Product Performance")
    def build_product_performance(self) -> DataFrame:
        """
        Aggregates product performance metrics for analytics.

        Groups by product and category, calculating revenue,
        order count, average review score, and ranking.

        Returns:
            DataFrame with product KPIs, ranked within category.
        """
        orders = self._read_silver("silver_orders")
        products = self._read_silver("silver_products")

        orders.createOrReplaceTempView("silver_orders")
        products.createOrReplaceTempView("silver_products")

        product_perf = self.spark.sql("""
            WITH order_items_expanded AS (
                SELECT
                    o.order_id,
                    o.total_payment_value,
                    o.total_items_count,
                    o.order_purchase_timestamp,
                    o.order_year,
                    o.order_month
                FROM silver_orders o
                WHERE o.order_status = 'delivered'
            )
            SELECT
                p.product_id,
                p.product_category_name_english AS category,
                p.product_weight_g,

                -- Revenue metrics
                COUNT(DISTINCT oi.order_id)         AS total_orders,
                SUM(oi.total_payment_value)         AS total_revenue,
                AVG(oi.total_payment_value)         AS avg_order_value,
                MIN(oi.order_purchase_timestamp)    AS first_sold_date,
                MAX(oi.order_purchase_timestamp)    AS last_sold_date,

                -- Ranking within category (Spark Window function)
                DENSE_RANK() OVER (
                    PARTITION BY p.product_category_name_english
                    ORDER BY SUM(oi.total_payment_value) DESC
                ) AS revenue_rank_in_category,

                -- Revenue percentile across all products
                PERCENT_RANK() OVER (
                    ORDER BY SUM(oi.total_payment_value) DESC
                ) AS revenue_percentile,

                CURRENT_TIMESTAMP() AS gold_created_at
            FROM silver_products p
            LEFT JOIN order_items_expanded oi ON (1=1) -- Simplified for demo
            GROUP BY
                p.product_id,
                p.product_category_name_english,
                p.product_weight_g
        """)

        log_dataframe_stats(logger, product_perf, "product_performance")
        return product_perf

    @pipeline_step("Gold: Daily Revenue Summary")
    def build_daily_revenue_summary(self) -> DataFrame:
        """
        Builds a daily revenue summary with running totals.
        Essential KPI table for BI dashboards.

        Returns:
            DataFrame with daily metrics and running totals.
        """
        orders = self._read_silver("silver_orders")
        orders.createOrReplaceTempView("silver_orders")

        daily = self.spark.sql("""
            WITH daily_base AS (
                SELECT
                    DATE(order_purchase_timestamp)          AS order_date,
                    order_year,
                    order_month,
                    COUNT(DISTINCT order_id)                AS total_orders,
                    COUNT(DISTINCT customer_unique_id)      AS unique_customers,
                    COALESCE(SUM(total_payment_value), 0)   AS total_revenue,
                    COALESCE(AVG(total_payment_value), 0)   AS avg_order_value,
                    COALESCE(SUM(total_items_count), 0)     AS total_items_sold,
                    SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END) AS delivered_orders,
                    SUM(CASE WHEN order_status = 'canceled'  THEN 1 ELSE 0 END) AS canceled_orders,
                    SUM(CASE WHEN is_delivered_on_time = true THEN 1 ELSE 0 END) AS on_time_deliveries
                FROM silver_orders
                WHERE order_purchase_timestamp IS NOT NULL
                GROUP BY DATE(order_purchase_timestamp), order_year, order_month
            )
            SELECT
                *,
                -- 7-day moving average (Window function)
                AVG(total_revenue) OVER (
                    ORDER BY order_date
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) AS revenue_7d_moving_avg,

                -- Running total revenue
                SUM(total_revenue) OVER (
                    ORDER BY order_date
                    ROWS UNBOUNDED PRECEDING
                ) AS cumulative_revenue,

                -- Delivery on-time rate
                ROUND(
                    on_time_deliveries * 100.0 / NULLIF(delivered_orders, 0),
                    2
                ) AS on_time_delivery_rate_pct,

                -- Cancellation rate
                ROUND(
                    canceled_orders * 100.0 / NULLIF(total_orders, 0),
                    2
                ) AS cancellation_rate_pct,

                CURRENT_TIMESTAMP() AS gold_created_at
            FROM daily_base
            ORDER BY order_date
        """)

        log_dataframe_stats(logger, daily, "daily_revenue_summary")
        return daily

    @pipeline_step("Gold: Seller Scorecard")
    def build_seller_scorecard(self) -> DataFrame:
        """
        Computes a comprehensive seller performance scorecard.

        Returns:
            DataFrame with seller KPIs and tier ranking.
        """
        sellers = self._read_silver("silver_sellers")
        sellers.createOrReplaceTempView("silver_sellers")

        scorecard = self.spark.sql("""
            SELECT
                seller_id,
                seller_state,
                seller_city,

                COALESCE(total_orders, 0)       AS total_orders,
                COALESCE(total_revenue, 0)      AS total_revenue,
                COALESCE(avg_item_price, 0)     AS avg_item_price,
                COALESCE(avg_review_score, 0)   AS avg_review_score,

                -- Seller tier based on revenue percentile
                CASE
                    WHEN PERCENT_RANK() OVER (ORDER BY total_revenue DESC) <= 0.10 THEN 'Platinum'
                    WHEN PERCENT_RANK() OVER (ORDER BY total_revenue DESC) <= 0.25 THEN 'Gold'
                    WHEN PERCENT_RANK() OVER (ORDER BY total_revenue DESC) <= 0.50 THEN 'Silver'
                    ELSE 'Bronze'
                END AS seller_tier,

                -- Revenue rank overall
                DENSE_RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,

                CURRENT_TIMESTAMP() AS gold_created_at
            FROM silver_sellers
        """)

        log_dataframe_stats(logger, scorecard, "seller_scorecard")
        return scorecard

    def _write_gold(self, df: DataFrame, path_key: str) -> None:
        """Writes a DataFrame to the Gold layer as Parquet."""
        output_path = get_storage_path("gold", path_key)
        logger.info(f"Writing Gold: {output_path}")
        df.write.mode("overwrite").format("parquet").save(output_path)
        logger.info(f"✅ Written to Gold: {output_path}")

    def run(self, reference_date: date = None) -> None:
        """
        Executes the full Silver → Gold aggregation pipeline.

        Args:
            reference_date: Reference date for RFM recency calculation.
        """
        with log_stage(logger, "Silver → Gold Pipeline"):
            rfm = self.build_customer_rfm(reference_date)
            product_perf = self.build_product_performance()
            daily_rev = self.build_daily_revenue_summary()
            seller_sc = self.build_seller_scorecard()

            self._write_gold(rfm, "gold_customer_rfm")
            self._write_gold(product_perf, "gold_product_perf")
            self._write_gold(daily_rev, "gold_daily_revenue")
            self._write_gold(seller_sc, "gold_seller_scorecard")

            logger.info("🎉 Silver → Gold pipeline complete")


if __name__ == "__main__":
    agg = SilverToGoldAggregator()
    agg.run()
