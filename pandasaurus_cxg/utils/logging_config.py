import logging


# Create a filter to exclude ERROR log records
class NoErrorFilter(logging.Filter):
    def filter(self, record):
        return record.levelno != logging.ERROR


def configure_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    # Create a console handler and set the level to INFO
    info = logging.StreamHandler()
    info.setLevel(logging.INFO)
    # Create a console handler and set the level to ERROR
    error = logging.StreamHandler()
    error.setLevel(logging.ERROR)

    # Create a formatter and set the format for log messages
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    info.setFormatter(formatter)
    error.setFormatter(formatter)
    error.addFilter(NoErrorFilter())
    # Add the console handler to the logger
    logger.addHandler(info)
    logger.addHandler(error)

    return logger
