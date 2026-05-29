"""
SparkSession factory for the Enterprise Data Pipeline.
Provides a configured SparkSession with Delta Lake and MinIO/S3A support.
"""

from pyspark.sql import SparkSession

from src.utils.config import get_config
from src.utils.logger import get_logger

logger = get_logger(__name__)


def create_spark_session(app_name: str = None) -> SparkSession:
    """
    Creates and returns a configured SparkSession.

    Features:
    - Delta Lake extensions enabled
    - S3A filesystem configured for MinIO (Azure Data Lake simulation)
    - Adaptive Query Execution (AQE) enabled
    - Optimized for local development or cluster submission

    Args:
        app_name: Override the default app name from config.

    Returns:
        Configured SparkSession instance.
    """
    config = get_config()
    spark_cfg = config["spark"]
    storage_cfg = config["storage"]

    name = app_name or spark_cfg.get("app_name", "enterprise-etl-pipeline")
    master = spark_cfg.get("master", "local[*]")

    logger.info(f"Creating SparkSession | app={name} | master={master}")

    builder = SparkSession.builder.appName(name).master(master)

    # Apply all config from pipeline_config.yaml
    for key, value in spark_cfg.get("config", {}).items():
        builder = builder.config(key, str(value))

    # Override S3A credentials with runtime values (in case env vars differ)
    builder = (
        builder
        .config("spark.hadoop.fs.s3a.endpoint", storage_cfg["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", storage_cfg["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", storage_cfg["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        f"✅ SparkSession ready | version={spark.version} | "
        f"app_id={spark.sparkContext.applicationId}"
    )
    return spark


def get_or_create_spark(app_name: str = None) -> SparkSession:
    """
    Returns existing SparkSession or creates a new one.
    Use this in DAGs and scripts to avoid multiple SparkSession creation.
    """
    try:
        spark = SparkSession.getActiveSession()
        if spark is not None:
            logger.info("Reusing existing SparkSession")
            return spark
    except Exception:
        pass

    return create_spark_session(app_name)
