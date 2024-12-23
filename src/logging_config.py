import logging
import sys

def setup_logging():
    logger = logging.getLogger("my_fastapi_app")
    logger.setLevel(logging.INFO)

    # Create console handler
    stream_handler = logging.StreamHandler(sys.stdout)
    log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    stream_handler.setFormatter(log_formatter)

    # Add handler to the logger
    logger.addHandler(stream_handler)

    return logger
