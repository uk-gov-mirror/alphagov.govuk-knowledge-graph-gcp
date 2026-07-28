import logging

logger = logging.getLogger(__name__)

def list_blobs(
    storage_client,
    bucket_name,
    prefix
):

    
    return list(
        storage_client.list_blobs(
            bucket_name,
            prefix=prefix
        )
    )


def derive_move_path(path_contains):

    return "/".join(
        path_contains.strip("/").split("/")[:-1]
    ) + "/"


def copy_blobs_to_bucket(
    storage_client, 
    source_bucket_name, 
    target_bucket_name, 
    blobs
):
    """
    Copies a list of GCS Blobs from a source bucket to a target bucket, 
    preserving their relative paths, and returns the list of new target Blobs.
    """
    logger.info(
        "Copying %d blobs from source bucket '%s' to target bucket '%s'",
        len(blobs), source_bucket_name, target_bucket_name
    )
    
    source_bucket = storage_client.bucket(source_bucket_name)
    target_bucket = storage_client.bucket(target_bucket_name)
    target_blobs = []

    for b in blobs:
        try:
            logger.info("Copying blob: %s", b.name)
            # copy_blob performs an efficient server-side GCS copy and returns the new target Blob reference
            target_blob = source_bucket.copy_blob(b, target_bucket, b.name)
            target_blobs.append(target_blob)
        except Exception as e:
            logger.error("Failed to copy blob %s: %s", b.name, e)
            raise Exception(f"Copy to target bucket failed for {b.name}: {e}")

    return target_blobs
