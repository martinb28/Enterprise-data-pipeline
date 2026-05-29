"""
Bronze Ingestion DAG — Downloads Olist dataset and uploads to Bronze layer.

Schedule: Daily at midnight UTC
Azure equivalent: Azure Data Factory pipeline with scheduled trigger.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from src.utils.config import get_config
from src.utils.logger import get_logger

logger = get_logger(__name__)

config = get_config()

DEFAULT_ARGS = {
    "owner": config["airflow"]["default_args"]["owner"],
    "retries": config["airflow"]["default_args"]["retries"],
    "retry_delay": timedelta(
        minutes=config["airflow"]["default_args"]["retry_delay_minutes"]
    ),
    "email_on_failure": config["airflow"]["default_args"]["email_on_failure"],
    "depends_on_past": False,
}


def _run_olist_ingestion(execution_date=None, **context):
    """
    Task: Downloads Olist dataset from Kaggle and uploads to Bronze/MinIO.
    Falls back to Faker-generated data if Kaggle credentials are missing.
    """
    import os
    from datetime import date

    logger.info(f"Starting Bronze ingestion | execution_date={execution_date}")

    ingest_date = (
        date.fromisoformat(str(execution_date)[:10])
        if execution_date
        else date.today()
    )

    # Try Kaggle first, fallback to synthetic data
    kaggle_user = os.environ.get("KAGGLE_USERNAME")
    kaggle_key = os.environ.get("KAGGLE_KEY")

    if kaggle_user and kaggle_key:
        logger.info("Using Kaggle API for data download")
        from src.ingestion.olist_loader import run_ingestion
        stats = run_ingestion(ingest_date)
    else:
        logger.warning("Kaggle credentials not found — generating synthetic data with Faker")
        from src.ingestion.faker_generator import FakerGenerator
        gen = FakerGenerator(num_customers=10_000)
        stats = gen.generate_and_upload(date_partition=str(ingest_date))

    logger.info(f"Bronze ingestion complete: {stats}")

    # Push stats to XCom so downstream tasks can access them
    context["ti"].xcom_push(key="ingestion_stats", value=stats)
    return stats


def _validate_bronze_upload(**context):
    """
    Task: Validates that expected files were uploaded to Bronze.
    """
    from src.ingestion.storage_client import StorageClient
    from datetime import date

    client = StorageClient()
    execution_date = context["execution_date"]
    date_str = str(execution_date)[:10]
    bucket = get_config()["storage"]["buckets"]["bronze"]
    prefix = f"olist/raw/{date_str}/"

    objects = client.list_objects(bucket, prefix)
    logger.info(f"Found {len(objects)} objects in s3://{bucket}/{prefix}")

    if len(objects) == 0:
        raise ValueError(f"No files found in Bronze at {prefix}. Ingestion may have failed.")

    logger.info(f"✅ Bronze validation passed: {len(objects)} files present")
    return {"files_found": len(objects), "prefix": prefix}


with DAG(
    dag_id="bronze_ingestion",
    description="Daily ingestion of Olist E-Commerce data to Bronze layer",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval=config["airflow"]["schedules"]["bronze"],
    catchup=False,
    tags=["etl", "bronze", "ingestion", "olist"],
    doc_md="""
## Bronze Ingestion DAG

Downloads the Olist E-Commerce dataset (or generates synthetic data) and
uploads raw CSV files to the Bronze layer in MinIO (Azure Data Lake equivalent).

### Pipeline:
```
start → ingest_data → validate_bronze → trigger_silver → end
```

### Output:
- `s3a://bronze/olist/raw/{date}/olist_*.csv`

### Monitoring:
- Check Airflow logs for ingestion statistics
- Verify file counts in MinIO console (http://localhost:9001)
""",
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    ingest_data = PythonOperator(
        task_id="ingest_olist_data",
        python_callable=_run_olist_ingestion,
        doc_md="Downloads Olist dataset from Kaggle API and uploads to Bronze/MinIO",
    )

    validate_bronze = PythonOperator(
        task_id="validate_bronze_upload",
        python_callable=_validate_bronze_upload,
        doc_md="Validates that expected files are present in Bronze layer",
    )

    # DAG execution order
    start >> ingest_data >> validate_bronze >> end
