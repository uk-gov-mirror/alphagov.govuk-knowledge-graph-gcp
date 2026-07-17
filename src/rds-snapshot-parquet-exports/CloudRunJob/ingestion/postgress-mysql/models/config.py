import os
import json
import logging

logger = logging.getLogger(__name__)

# Read the path from the environment, defaulting to non-live for safety
CONFIG_FILE = os.getenv("CONFIG_PATH", "config/non-live/config.json")

    
def load_config():
    logger.info("Loading configuration from: %s", CONFIG_FILE)
    try:
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)

    except FileNotFoundError:
        logger.exception("Config file not found: %s", CONFIG_FILE)
        raise RuntimeError(f"Config file not found: {CONFIG_FILE}")

    except json.JSONDecodeError:
        logger.exception("Invalid JSON in config file: %s", CONFIG_FILE)
        raise RuntimeError(f"Invalid JSON in config file: {CONFIG_FILE}")

    except Exception:
        logger.exception("Failed loading config file: %s", CONFIG_FILE)
        raise RuntimeError(f"Failed loading config: {CONFIG_FILE}")