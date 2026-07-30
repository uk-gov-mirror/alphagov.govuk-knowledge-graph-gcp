from google.cloud import bigquery
from google.cloud import storage
import json
from urllib.parse import urlparse
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

        # 1. Fetch the existing schema from the target table
        target_table = self.client.get_table(table_ref)
        existing_schema = target_table.schema
        existing_field_names = {field.name for field in existing_schema}
        logger.info("Retrieved existing schema for table %s.%s to enforce structure.", dataset_id, table_name)

        # 2.  GCS URI parsing to handle any accidental whitespaces or formatting issues
        cleaned_uri = uri.strip()
        if not cleaned_uri.lower().startswith("gs://"):
            raise ValueError(f"URI must be a GCS path starting with gs://, got: {uri}")
        
        parsed_uri = urlparse(cleaned_uri)
        bucket_name = parsed_uri.netloc.strip()
        blob_path = parsed_uri.path.strip().lstrip("/")
        
        if not bucket_name or not blob_path:
            raise ValueError(f"Invalid GCS URI format: '{uri}'. Must contain both bucket and file path.")

        logger.info("Scanning GCS file 'gs://%s/%s' to discover all JSON fields...", bucket_name, blob_path)
        
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        
        # Helper to map Python types to BigQuery types
        def infer_bq_type(val) -> str:
            if val is None:
                return "STRING"
            elif isinstance(val, bool):
                return "BOOLEAN"
            elif isinstance(val, int):
                return "INT64"
            elif isinstance(val, float):
                return "FLOAT64"
            elif isinstance(val, (dict, list)):
                return "JSON"  # Native JSON is safest for nested structures/arrays
            else:
                return "STRING"

        new_fields = {}
        with blob.open("rt", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    if isinstance(data, dict):
                        for k, v in data.items():
                            if k not in existing_field_names:
                                inferred_type = infer_bq_type(v)
                                if k in new_fields:
                                    # Type Resolution: upgrade from default string (None) to exact type
                                    # Or fallback to STRING if conflicting types are found (e.g., INT64 vs BOOLEAN)
                                    current_type = new_fields[k]
                                    if current_type != inferred_type:
                                        if current_type == "STRING" and inferred_type != "STRING":
                                            new_fields[k] = inferred_type
                                        elif inferred_type == "STRING":
                                            pass
                                        else:
                                            new_fields[k] = "STRING"
                                else:
                                    new_fields[k] = inferred_type
                except Exception:
                    continue

        # 3. If new fields are found, dynamically update the table's schema in BigQuery first
        if new_fields:
            logger.info("Found new fields to append: %s", list(new_fields.keys()))
            updated_schema = list(existing_schema)
            for field_name, field_type in new_fields.items():
                updated_schema.append(
                    bigquery.SchemaField(field_name, field_type, mode="NULLABLE")
                )
            target_table.schema = updated_schema
            target_table = self.client.update_table(target_table, ["schema"])
            logger.info("Successfully appended new fields to BigQuery table schema.")
        else:
            logger.info("No new fields detected in GCS file.")

        # 4. Trigger the load job explicitly locked to the updated target table schema
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=target_table.schema,  # Force BigQuery to use the newly merged schema
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE_DATA,  # Empty existing data, keeping the structure
            ignore_unknown_values=False,  # Keep as False so any syntax errors are caught, but no "No such field" errors can happen now
        )

        job = self.client.load_table_from_uri(cleaned_uri, table_ref, job_config=job_config)
        job.result()

        logger.info("Loaded and refreshed data in %s.%s", dataset_id, table_name)
