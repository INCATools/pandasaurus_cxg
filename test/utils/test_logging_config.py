import logging

import pytest

from pandasaurus_cxg.utils.logging_config import NoErrorFilter, configure_logger


@pytest.fixture
def logger():
    return configure_logger()


def test_configure_logger(logger):
    # Check if the logger has the expected name
    assert logger.name == "pandasaurus_cxg.utils.logging_config"

    # Check if the logger level is set to INFO
    assert logger.level == logging.INFO

    # Check if there are exactly two handlers (one for INFO and one for ERROR)
    assert len(logger.handlers) == 4

    # Check if the logger's INFO handler has the expected level and filter
    info_handler = logger.handlers[0]
    assert info_handler.level == logging.INFO
    assert len(info_handler.filters) == 0  # No filters for INFO

    # Check if the logger's ERROR handler has the expected level and filter
    error_handler = logger.handlers[1]
    assert error_handler.level == logging.ERROR
    assert len(error_handler.filters) == 1  # One filter for excluding ERROR logs
    assert isinstance(error_handler.filters[0], NoErrorFilter)
