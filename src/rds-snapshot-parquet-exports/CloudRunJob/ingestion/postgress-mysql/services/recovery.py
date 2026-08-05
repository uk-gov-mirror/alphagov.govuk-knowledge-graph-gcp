import logging

logger = logging.getLogger(__name__)


def recover_files(
    storage_client,
    bucket_name,
    error_root,
    source_root,
    path_contains,
    move_path,
):
    """
    Recover failed ingestion files from error location back to landing location.

    Files matching the feed are copied from:
        error_root

    back to:
        source_root

    The original error files are deleted after successful copy.

    Returns:
        List of recovered target bucket Blob objects
    """

    logger.info(
        "Recovery started. Moving files from error '%s' back to landing '%s'",
        error_root,
        source_root,
    )

    bucket = storage_client.bucket(bucket_name)

    # Ensure prefixes have trailing slash
    error_prefix = (
        error_root
        if error_root.endswith("/")
        else f"{error_root}/"
    )

    source_prefix = (
        source_root
        if source_root.endswith("/")
        else f"{source_root}/"
    )

    # Discover files in error location
    error_blobs = list(
        storage_client.list_blobs(
            bucket_name,
            prefix=error_prefix
        )
    )

    logger.info(
        "Total files found in error location: %d",
        len(error_blobs)
    )

    # Filter only files belonging to this feed
    recovery_blobs = [
        blob
        for blob in error_blobs
        if (
            (
                path_contains in blob.name
                and blob.name.endswith(".parquet")
            )
            or move_path in blob.name
            or blob.name.endswith(".json")
        )
    ]

    logger.info(
        "Files eligible for recovery: %d",
        len(recovery_blobs)
    )

    recovered_blobs = []

    for blob in recovery_blobs:

        try:

            relative_path = blob.name[len(error_prefix):]

            new_name = (
                f"{source_prefix}{relative_path}"
            )

            logger.info(
                "Recovering file: %s -> %s",
                blob.name,
                new_name,
            )

            # Copy file back to landing
            new_blob = bucket.copy_blob(
                blob,
                bucket,
                new_name,
            )

            # Delete error copy
            blob.delete()

            recovered_blobs.append(new_blob)

        except Exception as e:

            logger.exception(
                "Failed recovering blob %s",
                blob.name,
            )

            raise Exception(
                f"Recovery failed for {blob.name}: {e}"
            )


    logger.info(
        "Recovery completed. Files recovered: %d",
        len(recovered_blobs)
    )

    return recovered_blobs
