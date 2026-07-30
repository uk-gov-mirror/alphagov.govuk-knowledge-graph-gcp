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
# SHAPE DISCOVERY (pass 1)
# =========================================================

def discover_shapes(db, collection):
    """
    First pass over a collection: determines which field names exist
    (all_fields) and which of those are ever a list on at least one
    document (array_fields). No data is written here — this only
    decides how the export pass below should shape each field so it's
    consistent with a REPEATED vs NULLABLE BigQuery column.
    """
    all_fields = set()
    array_fields = set()

    for doc in db[collection].find():
        clean = sanitize_document(doc)
        for key, value in clean.items():
            key_lower = key.lower()
            all_fields.add(key)
            if isinstance(value, list):
                array_fields.add(key_lower)

    return all_fields, array_fields


# =========================================================
# SCALAR-SAFE COERCION
# =========================================================

def _to_bq_string(value):
    """Coerces any single value to a BigQuery-STRING-safe scalar."""
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return json.dumps(value, default=str)
    if isinstance(value, str):
        return value
    return str(value)


def normalize_for_bq(doc, repeated_fields):
    """
    Shapes a document for export, given the final decision (from
    discover_shapes + sync_schema) about which fields are REPEATED.

    - Fields marked repeated: always emitted as a JSON array, even if
      this particular document only has a scalar for it (wrapped into
      a single-element list) or is missing it (empty list). Elements
      that are themselves lists/dicts are JSON-stringified so every
      element is a STRING, matching a REPEATED STRING column.
    - All other fields: any list/dict is JSON-stringified to a scalar
      string; everything else is cast to a plain string.
    """
    normalized = {}
    for key, value in doc.items():
        key_lower = key.lower()
        if key_lower in repeated_fields:
            if isinstance(value, list):
                items = value
            elif value is None:
                items = []
            else:
                items = [value]
            normalized[key] = [_to_bq_string(v) for v in items]
        else:
            normalized[key] = _to_bq_string(value)
    return normalized


def write_to_gcs(db, collection, bucket, blob_name, repeated_fields):
    """
    Second pass over a collection: writes newline-delimited JSON to
    GCS, shaped according to repeated_fields decided upfront.
    """
    blob = bucket.blob(blob_name)
    count = 0

    with blob.open("w", encoding="utf-8") as f:
        for doc in db[collection].find():
            clean = normalize_for_bq(sanitize_document(doc), repeated_fields)
            f.write(json.dumps(clean) + "\n")
            count += 1

            if count % 1000000 == 0:
                logger.info("Processed %s docs", count)

    return count


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
    array_fields = set()

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
            dbs_with_collection = []

            # =========================================================
            # PASS 1 — DISCOVER FIELD SHAPES ACROSS ALL DBs
            # =========================================================
            for db_name in dbs:
                db = mongo.client[db_name]
                if config.collection_name not in db.list_collection_names():
                    continue

                dbs_with_collection.append(db_name)
                fields, arrays = discover_shapes(db, config.collection_name)
                all_fields |= fields
                array_fields |= arrays

            if not dbs_with_collection:
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

            # =========================================================
            # SYNC SCHEMA (before writing, so we know which fields to
            # wrap into arrays during the write pass)
            # =========================================================
            schema, repeated_fields = bq.sync_schema(
                config.dataset_id,
                config.table_name or config.collection_name,
                all_fields,
                array_fields
            )

            # =========================================================
            # PASS 2 — WRITE TO GCS, SHAPED PER FINAL SCHEMA
            # =========================================================
            for db_name in dbs_with_collection:
                db = mongo.client[db_name]
                blob_name = f"temp/{run_id}/{db_name}_{config.collection_name}.json"

                count = write_to_gcs(
                    db,
                    config.collection_name,
                    bucket,
                    blob_name,
                    repeated_fields
                )

                if count:
                    uploaded_blobs.append(blob_name)
                    total_count += count
                else:
                    logger.warning("No data in DB: %s", db_name)

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