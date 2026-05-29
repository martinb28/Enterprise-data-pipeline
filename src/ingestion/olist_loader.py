"""
Bronze Layer — Olist E-Commerce Dataset Ingestion.

Downloads the Olist Brazilian E-Commerce dataset from Kaggle and uploads
the raw CSV files to the Bronze layer (MinIO / Azure Data Lake).

Kaggle dataset: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
"""

import os
import zipfile
from datetime import date
from pathlib import Path
from typing import List

from src.ingestion.storage_client import StorageClient
from src.utils.config import get_config, get_storage_path
from src.utils.logger import get_logger, log_stage, pipeline_step

logger = get_logger(__name__)

DATASET_ID = "olistbr/brazilian-ecommerce"


class OlistLoader:
    """
    Orchestrates the download and upload of the Olist E-Commerce dataset.

    Flow:
        Kaggle API → Local temp storage → Bronze bucket (s3a://bronze/olist/raw/{date}/)

    Azure equivalent:
        This simulates an Azure Data Factory pipeline that pulls data from an
        external source and lands it in Azure Data Lake Storage Gen2 (Bronze layer).
    """

    def __init__(self):
        self.config = get_config()
        self.storage = StorageClient()
        self.local_path = Path(self.config["dataset"]["kaggle"]["dataset_id"].split("/")[-1])
        self.raw_path = Path(self.config["dataset"]["local_path"])
        self.files = self.config["dataset"]["kaggle"]["files"]

    def _validate_kaggle_credentials(self) -> None:
        """Verifies Kaggle API credentials are configured."""
        username = os.environ.get("KAGGLE_USERNAME")
        key = os.environ.get("KAGGLE_KEY")

        if not username or not key:
            raise EnvironmentError(
                "Kaggle credentials not found. Set KAGGLE_USERNAME and KAGGLE_KEY "
                "in your .env file. Get them at: https://www.kaggle.com/settings/account"
            )
        logger.info(f"✅ Kaggle credentials found for user: {username}")

    @pipeline_step("Download Olist Dataset from Kaggle")
    def download(self) -> Path:
        """
        Downloads the Olist dataset from Kaggle API.

        Returns:
            Path to the directory containing extracted CSV files.
        """
        self._validate_kaggle_credentials()

        # Import here to avoid hard dependency if using faker instead
        import kaggle  # noqa: F401

        self.raw_path.mkdir(parents=True, exist_ok=True)

        logger.info(f"Downloading dataset: {DATASET_ID}")
        os.system(
            f"kaggle datasets download -d {DATASET_ID} "
            f"--path {self.raw_path} --unzip --quiet"
        )

        downloaded_files = list(self.raw_path.glob("*.csv"))
        logger.info(f"Downloaded {len(downloaded_files)} CSV files to {self.raw_path}")

        return self.raw_path

    @pipeline_step("Upload Raw Files to Bronze Layer")
    def upload_to_bronze(self, ingestion_date: date = None) -> dict:
        """
        Uploads local CSV files to the Bronze bucket in MinIO/S3.

        Files are partitioned by ingestion date:
            s3a://bronze/olist/raw/2024-01-15/olist_orders_dataset.csv

        Args:
            ingestion_date: Date partition for the upload. Defaults to today.

        Returns:
            Dictionary with upload statistics.
        """
        if ingestion_date is None:
            ingestion_date = date.today()

        date_str = ingestion_date.strftime("%Y-%m-%d")
        stats = {"uploaded": 0, "skipped": 0, "total_bytes": 0}

        for filename in self.files:
            local_file = self.raw_path / filename
            if not local_file.exists():
                logger.warning(f"File not found, skipping: {local_file}")
                stats["skipped"] += 1
                continue

            # S3 key: olist/raw/2024-01-15/olist_orders_dataset.csv
            s3_key = f"olist/raw/{date_str}/{filename}"
            bucket = self.config["storage"]["buckets"]["bronze"]

            # Idempotency check — don't re-upload if already exists
            if self.storage.object_exists(bucket, s3_key):
                logger.info(f"⏭️  Already exists, skipping: s3://{bucket}/{s3_key}")
                stats["skipped"] += 1
                continue

            file_size = local_file.stat().st_size
            self.storage.upload_file(str(local_file), bucket, s3_key)
            stats["uploaded"] += 1
            stats["total_bytes"] += file_size

        logger.info(
            f"📦 Bronze upload complete | "
            f"Uploaded: {stats['uploaded']} | "
            f"Skipped: {stats['skipped']} | "
            f"Total: {stats['total_bytes'] / 1024 / 1024:.1f} MB"
        )
        return stats

    def run(self, ingestion_date: date = None) -> dict:
        """
        Full ingestion pipeline: download + upload.

        Args:
            ingestion_date: Date partition. Defaults to today.

        Returns:
            Upload statistics dictionary.
        """
        with log_stage(logger, "Bronze Ingestion — Olist E-Commerce"):
            self.download()
            return self.upload_to_bronze(ingestion_date)


def run_ingestion(ingestion_date: date = None) -> dict:
    """
    Entry point for Airflow task or CLI execution.

    Args:
        ingestion_date: Date to ingest. Defaults to today.

    Returns:
        Upload statistics.
    """
    loader = OlistLoader()
    return loader.run(ingestion_date)


if __name__ == "__main__":
    result = run_ingestion()
    logger.info(f"Ingestion result: {result}")
