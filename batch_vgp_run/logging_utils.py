#!/usr/bin/env python3

"""
Logging utilities for VGP pipeline.

This module provides centralized logging configuration and helper functions
for consistent logging across the pipeline.
"""

import sys
import logging

# Set up module logger
logger = logging.getLogger(__name__)


def setup_logging(quiet=False):
    """
    Configure logging for the VGP pipeline.

    Args:
        quiet (bool): If True, only show warnings and errors
    """
    # Set logging level based on quiet flag
    level = logging.WARNING if quiet else logging.INFO

    # Configure logging format
    log_format = '%(message)s'  # Simple format for user-facing messages

    # Configure root logger
    logging.basicConfig(
        level=level,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Set module logger level
    logger.setLevel(level)


def log_info(message):
    """Log informational message (suppressed in quiet mode)."""
    logger.info(message)


def log_warning(message):
    """Log warning message to both logger and stderr (always shown)."""
    logger.warning(message)
    print(f"Warning: {message}", file=sys.stderr)


def log_error(message):
    """Log error message to both logger and stderr (always shown)."""
    logger.error(message)
    print(f"Error: {message}", file=sys.stderr)
