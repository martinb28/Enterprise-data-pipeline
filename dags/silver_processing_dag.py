"""
Silver Processing DAG — Transforms Bronze raw data to Silver (cleaned/enriched).

Schedule: Daily, triggered after Bronze ingestion completes.
Azure equivalent: ADF pipeline running Databricks notebook activity.
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


def _run_bronze_to_silver(execution_date=None, **context):
    """Task: Executes Bronze → Silver PySpark transformation."""
    from datetime import date
    from src.transformations.bronze_to_silver import BronzeToSilverTransformer

    ingest_date = (
        date.fromisoformat(str(execution_date)[:10])
        if execution_date
        else date.today()
    )

    logger.info(f"Starting Silver transformation | date={ingest_date}")
    transformer = BronzeToSilverTransformer()
    transformer.run(ingest_date)
    logger.info("✅ Silver transformation complete")


def _run_data_quality(execution_date=None, **context):
    """Task: Validates Silver data quality and raises on errors."""
    from src.utils.spark_session import get_or_create_spark
    from src.utils.config import get_storage_path
    from src.transformations.data_quality import DataQualityValidator

    spark = get_or_create_spark("data-quality")
    validator = DataQualityValidator()

    # Read Silver orders
    path = get_storage_path("silver", "silver_orders")
    orders_df = spark.read.parquet(path)

    report = validator.validate_orders(orders_df)

    # Push report to XCom
    context["ti"].xcom_push(key="dq_report", value=report.to_json())

    if not report.passed:
        raise ValueError(
            f"Data quality FAILED on orders_enriched: "
            f"{report.error_count} errors, {report.warning_count} warnings. "
            f"Check Airflow logs for details."
        )

    logger.info(f"✅ Data quality passed | {report.total_rows:,} rows validated")


with DAG(
    dag_id="silver_processing",
    description="Bronze → Silver transformation with PySpark and data quality checks",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval=config["airflow"]["schedules"]["silver"],
    catchup=False,
    tags=["etl", "silver", "spark", "transformation"],
    doc_md="""
## Silver Processing DAG

Reads raw Bronze CSV files, applies PySpark transformations
(joins, deduplication, type casting), and writes clean Parquet to Silver.

### Pipeline:
```
wait_for_bronze → transform_to_silver → validate_data_quality → end
```

### Output:
- `s3a://silver/orders/enriched/` (partitioned by year/month)
- `s3a://silver/products/enriched/`
- `s3a://silver/sellers/enriched/`
""",
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Wait for Bronze DAG to complete before starting
    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze_ingestion",
        external_dag_id="bronze_ingestion",
        external_task_id="end",
        timeout=3600,
        poke_interval=30,
        mode="reschedule",
        doc_md="Waits for the Bronze ingestion DAG to complete successfully",
    )

    transform = PythonOperator(
        task_id="transform_bronze_to_silver",
        python_callable=_run_bronze_to_silver,
        doc_md="Runs PySpark Bronze → Silver transformation (joins, cleaning, enrichment)",
    )

    data_quality = PythonOperator(
        task_id="validate_data_quality",
        python_callable=_run_data_quality,
        doc_md="Runs data quality checks on Silver tables (nulls, duplicates, ranges)",
    )

    start >> wait_for_bronze >> transform >> data_quality >> end
