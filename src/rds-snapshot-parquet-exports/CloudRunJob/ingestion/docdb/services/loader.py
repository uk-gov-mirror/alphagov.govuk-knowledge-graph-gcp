import json
import os
from models.config import AppConfig, BackupConfig

# =========================================================
# LOAD CONFIGURATION
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
        backup=BackupConfig(**backup_cfg) if backup_cfg else None,
    )

    return config, raw
