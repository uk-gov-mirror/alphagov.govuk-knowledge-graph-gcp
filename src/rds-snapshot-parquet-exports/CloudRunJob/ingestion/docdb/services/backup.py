from services.exceptions import JobFailure

# =========================================================
# FIND DB BACKUP FILE
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
