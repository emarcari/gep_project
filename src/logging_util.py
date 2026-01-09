import logging

LOG_LEVEL = logging.INFO
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def setup_logging() -> logging.Logger:
    """
    Configure logging for local environments.

    :return: Configured logger instance.
    """
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
    logger = logging.getLogger("Main")
    logger.info("Configured local logging")

    return logger
