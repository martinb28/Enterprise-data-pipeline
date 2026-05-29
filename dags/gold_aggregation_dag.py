"""
Gold Aggregation DAG — Produces DS-ready analytics tables from Silver.

Schedule: Daily, triggered after Silver processing completes.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

from src.utils.config import get_config
from src.utils.logger import get_logger

logger = get_logger(__name__)
config = get_config()

DEFAULT_ARGS = {
    "owner": config["airflow"]["default_args"]["owner"],
    "retries": config["airflow"]["default_args"]["retries"],
    "retry_delay": timedelta(minutes=config["airflow"]["default_args"]["retry_delay_minutes"]),
    "email_on_failure": config["airflow"]["default_args"]["email_on_failure"],
}


def _run_gold_aggregations(execution_date=None, **context):
    """Task: Executes Silver → Gold aggregation pipeline."""
    from datetime import date
    from src.transformations.silver_to_gold import SilverToGoldAggregator

    ref_date = (
        date.fromisoformat(str(execution_date)[:10])
        if execution_date
        else date.today()
    )

    logger.info(f"Starting Gold aggregation | reference_date={ref_date}")
    agg = SilverToGoldAggregator()
    agg.run(ref_date)
    logger.info("✅ Gold aggregation complete")


def _run_rfm_quality_check(**context):
    """Task: Validates RFM Gold table quality."""
    from src.utils.spark_session import get_or_create_spark
    from src.utils.config import get_storage_path
    from src.transformations.data_quality import DataQualityValidator

    spark = get_or_create_spark("gold-quality")
    validator = DataQualityValidator()

    path = get_storage_path("gold", "gold_customer_rfm")
    rfm_df = spark.read.parquet(path)

    report = validator.validate_rfm(rfm_df)
    context["ti"].xcom_push(key="rfm_dq_report", value=report.to_json())

    if not report.passed:
        raise ValueError(f"RFM table quality failed: {report.error_count} errors")

    logger.info(f"✅ RFM quality passed | {report.total_rows:,} customers segmented")


def _generate_summary_stats(**context):
    """Task: Generates and logs a human-readable pipeline summary."""
    from src.utils.spark_session import get_or_create_spark
    from src.utils.config import get_storage_path

    spark = get_or_create_spark("pipeline-summary")

    rfm_path = get_storage_path("gold", "gold_customer_rfm")
    rfm_df = spark.read.parquet(rfm_path)

    # Customer segment distribution
    segment_counts = (
        rfm_df.groupBy("customer_segment")
        .count()
        .orderBy("count", ascending=False)
        .collect()
    )

    logger.info("=" * 60)
    logger.info("📊 PIPELINE SUMMARY — Gold Layer")
    logger.info("=" * 60)
    logger.info(f"Total customers segmented: {rfm_df.count():,}")
    logger.info("Customer Segment Distribution:")
    for row in segment_counts:
        logger.info(f"  {row['customer_segment']:25s}: {row['count']:>8,}")
    logger.info("=" * 60)


with DAG(
    dag_id="gold_aggregation",
    description="Silver → Gold analytics aggregation (RFM, revenue, sellers)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval=config["airflow"]["schedules"]["gold"],
    catchup=False,
    tags=["etl", "gold", "analytics", "rfm"],
    doc_md="""
## Gold Aggregation DAG

Reads Silver tables and produces analytics-ready Gold tables
for the Data Science team.

### Pipeline:
```
wait_for_silver → aggregate_gold → validate_rfm → generate_summary → end
```

### Output:
- `s3a://gold/customer_rfm/` — RFM scores + customer segments
- `s3a://gold/product_performance/` — Revenue per product/category
- `s3a://gold/seller_scorecard/` — Seller KPIs and tiers
- `s3a://gold/daily_revenue_summary/` — Daily KPIs with running totals
""",
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver_processing",
        external_dag_id="silver_processing",
        external_task_id="end",
        timeout=3600,
        poke_interval=30,
        mode="reschedule",
    )

    aggregate_gold = PythonOperator(
        task_id="aggregate_gold_tables",
        python_callable=_run_gold_aggregations,
        doc_md="Builds RFM, product performance, seller scorecard, and daily revenue Gold tables",
    )

    validate_rfm = PythonOperator(
        task_id="validate_rfm_quality",
        python_callable=_run_rfm_quality_check,
        doc_md="Validates RFM Gold table: null checks, score ranges, duplicates",
    )

    generate_summary = PythonOperator(
        task_id="generate_summary_stats",
        python_callable=_generate_summary_stats,
        doc_md="Logs customer segment distribution and pipeline statistics",
    )

    start >> wait_for_silver >> aggregate_gold >> validate_rfm >> generate_summary >> end
