# libopenschichtplaner5/src/libopenschichtplaner5/utils/logging_config.py
"""
Improved logging and error handling for OpenSchichtplaner5.
"""

import logging
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime


class ColoredFormatter(logging.Formatter):
    """Colored formatter for console output."""

    COLORS = {
        'DEBUG': '\033[36m',  # Cyan
        'INFO': '\033[32m',  # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',  # Red
        'CRITICAL': '\033[35m',  # Magenta
        'RESET': '\033[0m'  # Reset
    }

    def format(self, record):
        log_color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        reset_color = self.COLORS['RESET']

        # Add color to levelname
        record.levelname = f"{log_color}{record.levelname}{reset_color}"

        return super().format(record)


def setup_logging(
        level: str = "INFO",
        log_file: Optional[Path] = None,
        console: bool = True,
        verbose: bool = False
) -> logging.Logger:
    """
    Setup comprehensive logging for the application.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional path to log file
        console: Whether to log to console
        verbose: Whether to use verbose formatting

    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger('openschichtplaner5')
    logger.setLevel(getattr(logging, level.upper()))

    # Clear existing handlers
    logger.handlers.clear()

    # Format strings
    if verbose:
        fmt = '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
        date_fmt = '%Y-%m-%d %H:%M:%S'
    else:
        fmt = '%(levelname)s: %(message)s'
        date_fmt = None

    # Console handler
    if console:
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setLevel(getattr(logging, level.upper()))

        if sys.stderr.isatty():  # Only use colors in terminal
            console_formatter = ColoredFormatter(fmt, date_fmt)
        else:
            console_formatter = logging.Formatter(fmt, date_fmt)

        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    # File handler
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)  # Always debug level for files

        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
            '%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger


class SchichtplanerError(Exception):
    """Base exception for OpenSchichtplaner5."""
    pass


class DataError(SchichtplanerError):
    """Raised when there are data-related errors."""
    pass


class ConfigError(SchichtplanerError):
    """Raised when there are configuration errors."""
    pass


class QueryError(SchichtplanerError):
    """Raised when there are query-related errors."""
    pass


def handle_exception(logger: logging.Logger, exc: Exception, context: str = ""):
    """
    Handle exceptions with proper logging and user-friendly messages.

    Args:
        logger: Logger instance
        exc: Exception to handle
        context: Additional context information
    """
    error_msg = f"{context}: {str(exc)}" if context else str(exc)

    if isinstance(exc, SchichtplanerError):
        logger.error(error_msg)
    elif isinstance(exc, (FileNotFoundError, PermissionError)):
        logger.error(f"File system error - {error_msg}")
    elif isinstance(exc, (ImportError, ModuleNotFoundError)):
        logger.error(f"Import error - {error_msg}")
        logger.info("Try: pip install -r requirements.txt")
    elif isinstance(exc, KeyboardInterrupt):
        logger.info("Operation cancelled by user")
    else:
        logger.error(f"Unexpected error - {error_msg}")
        logger.debug("Full traceback:", exc_info=True)


# Global logger instance
_logger = None


def get_logger() -> logging.Logger:
    """Get the global logger instance."""
    global _logger
    if _logger is None:
        _logger = setup_logging()
    return _logger