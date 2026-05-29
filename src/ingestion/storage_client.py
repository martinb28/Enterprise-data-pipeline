"""
Storage client — abstraction over MinIO (local) / Azure Data Lake Gen2 (cloud).
Provides a unified interface for reading and writing data across all pipeline layers.
"""

import io
from pathlib import Path
from typing import List, Optional

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from src.utils.config import get_config
from src.utils.logger import get_logger

logger = get_logger(__name__)


class StorageClient:
    """
    S3-compatible storage client that works against MinIO locally
    and Azure Data Lake Gen2 / AWS S3 in production.

    Azure equivalent: BlobServiceClient / DataLakeServiceClient
    """

    def __init__(self):
        config = get_config()
        storage_cfg = config["storage"]

        self._client = boto3.client(
            "s3",
            endpoint_url=storage_cfg["endpoint"],
            aws_access_key_id=storage_cfg["access_key"],
            aws_secret_access_key=storage_cfg["secret_key"],
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        logger.info(f"StorageClient initialized | endpoint={storage_cfg['endpoint']}")

    def upload_file(self, local_path: str, bucket: str, key: str) -> None:
        """
        Uploads a local file to object storage.

        Args:
            local_path: Path to the local file.
            bucket: Target bucket name (e.g., 'bronze').
            key: Object key / path within the bucket.
        """
        file_size = Path(local_path).stat().st_size
        logger.info(f"Uploading {local_path} → s3://{bucket}/{key} ({file_size:,} bytes)")

        self._client.upload_file(local_path, bucket, key)
        logger.info(f"✅ Uploaded: s3://{bucket}/{key}")

    def upload_bytes(self, data: bytes, bucket: str, key: str) -> None:
        """
        Uploads raw bytes to object storage.

        Args:
            data: Bytes to upload.
            bucket: Target bucket name.
            key: Object key / path.
        """
        self._client.put_object(Body=data, Bucket=bucket, Key=key)
        logger.info(f"✅ Uploaded bytes: s3://{bucket}/{key} ({len(data):,} bytes)")

    def download_file(self, bucket: str, key: str, local_path: str) -> None:
        """
        Downloads an object from storage to a local file.

        Args:
            bucket: Source bucket name.
            key: Object key.
            local_path: Local destination path.
        """
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Downloading s3://{bucket}/{key} → {local_path}")
        self._client.download_file(bucket, key, local_path)
        logger.info(f"✅ Downloaded: {local_path}")

    def list_objects(self, bucket: str, prefix: str = "") -> List[str]:
        """
        Lists all object keys in a bucket under the given prefix.

        Args:
            bucket: Bucket name.
            prefix: Optional key prefix filter.

        Returns:
            List of object keys.
        """
        paginator = self._client.get_paginator("list_objects_v2")
        keys = []

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])

        logger.info(f"Listed {len(keys)} objects in s3://{bucket}/{prefix}")
        return keys

    def object_exists(self, bucket: str, key: str) -> bool:
        """
        Checks if an object exists in storage.

        Args:
            bucket: Bucket name.
            key: Object key.

        Returns:
            True if object exists, False otherwise.
        """
        try:
            self._client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def delete_objects(self, bucket: str, prefix: str) -> int:
        """
        Deletes all objects under a given prefix (simulates directory delete).

        Args:
            bucket: Bucket name.
            prefix: Key prefix to delete.

        Returns:
            Number of deleted objects.
        """
        keys = self.list_objects(bucket, prefix)
        if not keys:
            return 0

        objects = [{"Key": k} for k in keys]
        self._client.delete_objects(Bucket=bucket, Delete={"Objects": objects})
        logger.info(f"🗑️  Deleted {len(keys)} objects from s3://{bucket}/{prefix}")
        return len(keys)

    def get_object_size(self, bucket: str, key: str) -> Optional[int]:
        """
        Returns the size in bytes of an object.

        Args:
            bucket: Bucket name.
            key: Object key.

        Returns:
            Size in bytes, or None if not found.
        """
        try:
            resp = self._client.head_object(Bucket=bucket, Key=key)
            return resp["ContentLength"]
        except ClientError:
            return None
