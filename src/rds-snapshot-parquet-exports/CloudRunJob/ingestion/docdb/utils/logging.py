import logging
import sys

# 1. Configure the root logger EXACTLY ONCE at the module level when this file is imported.
# We also use StreamHandler(sys.stdout) to ensure standard application logs go to stdout,
# keeping stderr clean for actual application failures/stack traces.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)


def get_logger(name: str = __name__) -> logging.Logger:
    """
    Returns a configured logger instance with the specified module name context.
    """
    return logging.getLogger(name)