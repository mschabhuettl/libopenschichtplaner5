# Neue Datei: libopenschichtplaner5/src/libopenschichtplaner5/logging_config.py

import logging
import sys


def setup_logging(verbose: bool = False):
    """Configure logging for the library."""
    level = logging.DEBUG if verbose else logging.WARNING

    # Configure root logger
    logging.basicConfig(
        level=level,
        format='%(levelname)s: %(message)s',
        stream=sys.stderr  # Use stderr so it doesn't interfere with stdout
    )

    # Create logger for our library
    logger = logging.getLogger('libopenschichtplaner5')
    return logger


# Get the logger instance
logger = setup_logging()