import os
import uuid
from services.loader import load_config
from services.backup import resolve_backup
from services.gcs_writer import write_to_gcs
from services.schema_discovery import discover_shapes
from services.audit import insert_audit_record, update_audit_record
from services.bq_service import BigQueryService
from services.gcs_service import GCSService
from services.mongo_restore import MongoRestoreService
from services.exceptions import JobFailure
from utils.logging import get_logger

logger = get_logger(__name__)

# =========================================================
# INGEST DATA
# =========================================================

def ingest():
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
        # DOWNLOAD BACKUP FILE
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
        # INSERT AUDIT RECORD
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
            # STEP 1 — DISCOVER FIELDS
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
            #  CREATE OR UPDATE THE BIGQUERY SCHEMA
            # =========================================================
            schema, repeated_fields = bq.sync_schema(
                config.dataset_id,
                config.table_name or config.collection_name,
                all_fields,
                array_fields
            )

            # =========================================================
            # STEP 2 — WRITE JSON FILES TO GCS
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
            # LOAD DATA INTO BIGQUERY
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
