import json
import uuid
import os
from models.config import AppConfig, BackupConfig
from services.gcs_service import GCSService
from services.bq_service import BigQueryService
from services.mongo_restore import MongoRestoreService
from services.audit import insert_audit_record, update_audit_record
from utils.sanitizers import sanitize_document
from utils.logging import get_logger
from exceptions import JobFailure


logger = get_logger(__name__)


# =========================================================
# CONFIG
# =========================================================

def load_config():
    # Read the path from the environment, defaulting to non-live for safety
    CONFIG_FILE = os.getenv("CONFIG_PATH", "config/non-live/config.json")

    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        raw = json.load(f)

    backup_cfg = raw.get("backup")

    config = AppConfig(
        project_id=raw["project_id"],
        bucket_name=raw["bucket_name"],
        dataset_id=raw["dataset_id"],
        collection_name=raw["collection_name"],
        table_name=raw.get("table_name"),
        dataset_location=raw.get("dataset_location", "europe-west2"),
        object_name=raw.get("object_name"),
        backup=BackupConfig(**backup_cfg) if backup_cfg else None
    )

    return config, raw


# =========================================================
# MONGO → GCS STREAM
# =========================================================
def normalize_for_bq(doc):
    """
    Ensures every field value is BigQuery-scalar-safe before it's written
    to the NDJSON export. Mongo is schema-less, so the same field name can
    hold a scalar on one document and a list or nested object on another
    (e.g. auth_bypass_ids as a single id sometimes, a list of ids other
    times). Rather than trying to model that as REPEATED/RECORD in
    BigQuery — which breaks the moment a field's shape changes between
    runs, since BigQuery won't let you flip an existing column's mode —
    we serialize lists/dicts to a JSON string. Every field then stays a
    plain NULLABLE STRING, permanently, no matter how Mongo's shape drifts.
    """
    normalized = {}
    for key, value in doc.items():
        if isinstance(value, (list, dict)):
            normalized[key] = json.dumps(value, default=str)
        else:
            normalized[key] = value
    return normalized


def stream_to_gcs(db, collection, bucket, blob_name):
    """
    Streams documents from a Mongo collection to a GCS blob as
    newline-delimited JSON. Also tracks the union of all field
    names encountered so the caller can sync the BigQuery schema
    before loading.
    """
    blob = bucket.blob(blob_name)
    count = 0
    fields = set()

    with blob.open("w", encoding="utf-8") as f:
        for doc in db[collection].find():
            clean = normalize_for_bq(sanitize_document(doc))
            fields.update(clean.keys())
            f.write(json.dumps(clean) + "\n")
            count += 1

            if count % 1000000 == 0:
                logger.info("Processed %s docs", count)

    return count, fields

# =========================================================
# BACKUP RESOLUTION
# =========================================================

def resolve_backup(config, gcs):
    if config.object_name:
        return config.object_name

    if not config.backup:
        raise JobFailure("Backup config not defined")

    return gcs.find_latest_backup(
        config.bucket_name,
        config.backup
    )


# =========================================================
# MAIN PIPELINE
# =========================================================

def main():
    config, raw_config = load_config()

    gcs = GCSService(config.project_id)
    bq = BigQueryService(config.project_id)

    bq_client = getattr(bq, "client", None)
    if bq_client is None:
        raise JobFailure("BigQueryService does not expose a 'client' attribute")

    run_id = uuid.uuid4().hex

    audit_dataset = raw_config.get("audit_dataset", config.dataset_id)
    audit_table = raw_config.get("audit_table", "pipeline_audit")
    audit_table_id = f"{config.project_id}.{audit_dataset}.{audit_table}"

    logger.info("Using audit table: %s", audit_table_id)
    logger.info("Run ID: %s", run_id)

    backup = resolve_backup(config, gcs)

    local_file = f"/tmp/backup_{run_id}.gz"

    uploaded_blobs = []
    total_count = 0
    all_fields = set()

    raw_audit_id = None
    inserted_rows = 0

    bucket = gcs.get_bucket().bucket(config.bucket_name)

    try:
        # =========================================================
        # DOWNLOAD BACKUP
        # =========================================================
        gcs.download_file(
            config.bucket_name,
            backup,
            local_file
        )

        bq.ensure_dataset(
            config.dataset_id,
            config.dataset_location
        )

        # =========================================================
        # AUDIT START
        # =========================================================
        raw_audit_id = insert_audit_record(
            bq_client=bq_client,
            audit_table_id=audit_table_id,
            audit_stage="raw_to_dw_load",
            bq_dataset=config.dataset_id,
            bq_table=config.table_name or config.collection_name,
            file_type="json",
            file_path=f"gs://{config.bucket_name}/{backup}",
            status="processing",
        )

        with MongoRestoreService() as mongo:
            mongo.restore(local_file, config.collection_name)

            dbs = [
                db for db in mongo.client.list_database_names()
                if db not in ["admin", "local", "config"]
            ]

            # =========================================================
            # STREAM TO GCS
            # =========================================================
            for db_name in dbs:
                db = mongo.client[db_name]

                if config.collection_name not in db.list_collection_names():
                    continue

                blob_name = f"temp/{run_id}/{db_name}_{config.collection_name}.json"

                count, fields = stream_to_gcs(
                    db,
                    config.collection_name,
                    bucket,
                    blob_name
                )

                all_fields |= fields

                if count:
                    uploaded_blobs.append(blob_name)
                    total_count += count
                else:
                    logger.warning("No data in DB: %s", db_name)

            # =========================================================
            # NO DATA CASE
            # =========================================================
            if total_count == 0:
                logger.warning("No data found across databases.")

                update_audit_record(
                    bq_client=bq_client,
                    audit_table_id=audit_table_id,
                    audit_id=raw_audit_id,
                    status="no_files",
                    inserted_rows=0,
                    error_message="No JSON records written"
                )
                return

            inserted_rows = total_count

            # =========================================================
            # SYNC SCHEMA (preserve existing columns, append new ones)
            # =========================================================
            schema = bq.sync_schema(
                config.dataset_id,
                config.table_name or config.collection_name,
                all_fields
            )

            # =========================================================
            # LOAD TO BIGQUERY
            # =========================================================
            uri = f"gs://{config.bucket_name}/temp/{run_id}/*.json"

            bq.load_from_gcs(
                config.dataset_id,
                config.table_name or config.collection_name,
                uri,
                schema=schema
            )

            update_audit_record(
                bq_client=bq_client,
                audit_table_id=audit_table_id,
                audit_id=raw_audit_id,
                status="success",
                inserted_rows=inserted_rows,
                error_message=None
            )

    except Exception as e:
        logger.exception("Pipeline failed")

        if raw_audit_id:
            try:
                update_audit_record(
                    bq_client=bq_client,
                    audit_table_id=audit_table_id,
                    audit_id=raw_audit_id,
                    status="fail",
                    inserted_rows=inserted_rows,
                    error_message=str(e)
                )
            except Exception:
                logger.exception("Audit update failed")

        raise JobFailure(str(e)) from e

    finally:
        # =========================================================
        # CLEANUP GCS + LOCAL FILE
        # =========================================================
        logger.info("Cleaning up temporary files...")

        for blob_name in uploaded_blobs:
            try:
                bucket.blob(blob_name).delete()
            except Exception as e:
                logger.warning("Failed to delete %s: %s", blob_name, e)

        try:
            if os.path.exists(local_file):
                os.remove(local_file)
        except Exception as e:
            logger.warning("Failed to delete local file: %s", e)


# =========================================================
# ENTRYPOINT
# =========================================================

if __name__ == "__main__":
    try:
        main()
    except JobFailure as e:
        logger.error("Job failed: %s", e)
        raise