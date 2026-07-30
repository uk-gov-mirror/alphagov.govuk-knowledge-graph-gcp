from google.cloud import bigquery
from utils.logging import get_logger

logger = get_logger(__name__)


class BigQueryService:

    def __init__(self, project_id: str):
        self.client = bigquery.Client(project=project_id)

    def ensure_dataset(self, dataset_id: str, location: str):
        dataset_ref = self.client.dataset(dataset_id)

        try:
            self.client.get_dataset(dataset_ref)
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = location
            self.client.create_dataset(dataset)
            logger.info("Created dataset %s", dataset_id)

    def load_from_gcs(self, dataset_id, table_name, uri):
        table_ref = self.client.dataset(dataset_id).table(table_name)

        # 1. Fetch the exact schema of the target table that already exists
        target_table = self.client.get_table(table_ref)
        existing_schema = target_table.schema
        logger.info("Retrieved existing schema for table %s.%s to enforce structure.", dataset_id, table_name)

        # 2. Configure the job to lock to the existing schema and ignore new fields
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=existing_schema,               # Force BigQuery to use the pre-existing schema only
            ignore_unknown_values=True,           # Silently drop extra/new JSON columns
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Empty existing data, keeping the table structure
            # Note: 'autodetect' is omitted (defaults to False) to prevent automatic schema changes
        )

        # 3. Trigger the load job
        job = self.client.load_table_from_uri(uri, table_ref, job_config=job_config)
        job.result()

        logger.info("Loaded %s.%s", dataset_id, table_name)