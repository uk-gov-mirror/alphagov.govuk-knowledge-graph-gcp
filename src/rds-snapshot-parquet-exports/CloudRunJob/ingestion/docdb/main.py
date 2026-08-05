from services.ingest import ingest
from services.exceptions import JobFailure
from utils.logging import get_logger

logger = get_logger(__name__)


# =========================================================
# START PROGRAM
# =========================================================

if __name__ == "__main__":
    try:
        ingest()
    except JobFailure as e:
        logger.error("Job failed: %s", e)
        raise
