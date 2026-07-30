from google.cloud import bigquery
from google.cloud.exceptions import NotFound
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

    def sync_schema(self, dataset_id, table_name, source_fields):
        """
        Ensures the target table contains every field in `source_fields`.
        Existing columns, types, and order are preserved; any fields
        present in the source but not yet in the table are appended
        to the end as nullable STRINGs.

        Returns the resulting full table schema, or None if the table
        doesn't exist yet (first-ever load — let load_from_gcs autodetect).
        """
        table_ref = self.client.dataset(dataset_id).table(table_name)

        try:
            table = self.client.get_table(table_ref)
        except NotFound:
            return None

        existing_names = {f.name.lower() for f in table.schema}
        new_fields = [
            bigquery.SchemaField(name, "STRING", mode="NULLABLE")
            for name in sorted(source_fields)
            if name.lower() not in existing_names
        ]

        if not new_fields:
            return table.schema

        logger.info(
            "Adding %d new column(s) to %s.%s: %s",
            len(new_fields), dataset_id, table_name,
            [f.name for f in new_fields]
        )

        table.schema = list(table.schema) + new_fields
        table = self.client.update_table(table, ["schema"])
        return table.schema

    def load_from_gcs(self, dataset_id, table_name, uri, schema=None):
        table_ref = self.client.dataset(dataset_id).table(table_name)

        if schema:
            # Table schema already synced — load deterministically, no autodetect
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE_DATA,
                schema=schema,
                autodetect=False,
                ignore_unknown_values=False,
            )
        else:
            # First-ever load: table doesn't exist yet, let BigQuery infer it
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE_DATA,
                autodetect=True,
                ignore_unknown_values=False,
            )

        job = self.client.load_table_from_uri(uri, table_ref, job_config=job_config)
        job.result()

        logger.info("Loaded %s.%s", dataset_id, table_name)