import re
from google.cloud import storage
from utils.logging import get_logger
from exceptions import JobFailure  # Fix: Import JobFailure for clean error propagation

logger = get_logger(__name__)


class GCSService:

    def __init__(self, project_id: str):
        self.client = storage.Client(project=project_id)

    def download_file(self, bucket_name: str, object_name: str, local_path: str):
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(object_name)

        logger.info("Downloading gs://%s/%s", bucket_name, object_name)
        blob.download_to_filename(str(local_path))

    # Fix: Added get_bucket() to support main.py's "gcs.get_bucket().bucket(...)"
    def get_bucket(self) -> storage.Client:
        """
        Returns the raw GCS Client instance.
        """
        return self.client

    # Fix: Corrected indentation so this is a proper method of GCSService
    def bucket(self, bucket_name: str) -> storage.Bucket:
        """
        Returns a specific GCS bucket instance.
        """
        return self.client.bucket(bucket_name)

    # Fix: Corrected indentation so this is a proper method of GCSService
    def find_latest_backup(self, bucket_name: str, backup_cfg) -> str:
        """
        Returns latest backup matching config rules.
        """
        bucket = self.client.bucket(bucket_name)

        # Retrieve list of blobs matching prefix
        blobs = bucket.list_blobs(prefix=backup_cfg.prefix)

        pattern = re.compile(backup_cfg.match_regex)

        # Filter candidates based on the regex pattern
        candidates = [
            b for b in blobs
            if pattern.match(b.name)
        ]

        if not candidates:
            # Fix: Raising JobFailure matches the main.py try/except structure
            raise JobFailure(
                f"No matching backup found in GCS bucket '{bucket_name}' "
                f"with prefix '{backup_cfg.prefix}' and pattern '{backup_cfg.match_regex}'"
            )

        # Find the most recently updated backup file
        latest = max(
            candidates,
            key=lambda b: b.updated
        )

        logger.info(
            "Selected latest backup: %s (Updated: %s)",
            latest.name,
            latest.updated
        )

        return latest.name