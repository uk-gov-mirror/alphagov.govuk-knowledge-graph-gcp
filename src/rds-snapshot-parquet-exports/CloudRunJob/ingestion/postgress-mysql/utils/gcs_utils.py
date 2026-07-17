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