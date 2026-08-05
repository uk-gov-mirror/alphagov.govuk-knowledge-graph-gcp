import sys
import os
import logging
from google.cloud import bigquery, storage
from models.config import load_config
from services.ingestion import process_table


logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":

    config = load_config()
    logger.info("Source bucket: %s", config["source_bucket_name"])
    logger.info("Target bucket: %s", config["target_bucket_name"])
    logger.info("Source root: %s", config["source_root"])

    # 1. READ CLOUD RUN TASK ENVIRONMENT VARIABLES
    # Fallback to index 0 and count 1 if running locally
    task_index = int(os.environ.get("CLOUD_RUN_TASK_INDEX", 0))
    task_count = int(os.environ.get("CLOUD_RUN_TASK_COUNT", 1))

    # Read recovery mode from config or environment variable
    recovery_mode = os.environ.get("RECOVERY_MODE", "false").lower() in ("true", "1", "yes")

    all_tables = config.get("tables", [])
    
    # 2. PARTITION WORKLOAD (Modulo partitioning)
    # Each task only takes the tables assigned to its specific index
    assigned_tables = [
        table for idx, table in enumerate(all_tables) 
        if idx % task_count == task_index
    ]

    logger.info(
        "Task %d/%d initialized. Processing %d out of %d total tables. (Recovery Mode: %s)",
        task_index + 1, task_count, len(assigned_tables), len(all_tables), recovery_mode
    )

    if not assigned_tables:
        logger.info("No tables assigned to this task. Exiting.")
        sys.exit(0)

    storage_client = storage.Client(project=config["project_id"])
    bq_client = bigquery.Client(project=config["project_id"])

    failed_tables = []

    # 3. PROCESS ONLY ASSIGNED TABLES
    for table_cfg in assigned_tables:

        logger.info(
            "********* [Task %d] Processing %s.%s *********",
            task_index,
            table_cfg["raw_dataset"],
            table_cfg["raw_table"],
        )

        try:
            process_table(
                storage_client=storage_client, 
                bq_client=bq_client, 
                config=config, 
                table_cfg=table_cfg, 
                recovery_mode=recovery_mode
            )

        except Exception:
            failed_tables.append(
                f"{table_cfg['raw_dataset']}.{table_cfg['raw_table']}"
            )
            logger.exception("Table failed")

    if failed_tables:
        logger.warning("[Task %d] Failed tables:", task_index)
        for t in failed_tables:
            logger.warning(" - %s", t)
        sys.exit(1)

    logger.info("[Task %d] Pipeline completed successfully", task_index)