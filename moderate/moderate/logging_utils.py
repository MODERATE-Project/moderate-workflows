"""Logging utilities for consistent operation tracking and timing.

This module provides context managers and decorators for standardizing
logging patterns across workflow operations, including automatic timing,
structured metadata, and error logging.

Key Features:
- log_operation(): Context manager for operation timing and error tracking
- Automatic duration calculation
- Structured metadata support
- Consistent log format across operations
"""

import time
from contextlib import contextmanager
from typing import Any, Dict

from dagster import get_dagster_logger


@contextmanager
def log_operation(operation: str, **metadata: Any):
    """Context manager that logs operation start, duration, and errors.

    Automatically logs when an operation starts and completes, calculating
    the duration. If an exception occurs, logs the error before re-raising.
    All metadata is included in log messages as structured data.

    Args:
        operation: Human-readable operation description
        **metadata: Additional key-value pairs to include in logs

    Yields:
        DagsterLogManager for additional logging within the operation
    """

    logger = get_dagster_logger()
    start = time.time()

    metadata_str = _format_metadata(metadata)
    logger.info(f"Starting: {operation}{metadata_str}")

    try:
        yield logger
        duration = time.time() - start
        logger.info(
            f"Completed: {operation} ({duration:.2f}s){metadata_str}",
            extra={**metadata, "duration_seconds": duration, "status": "success"},
        )
    except Exception as e:
        duration = time.time() - start
        logger.error(
            f"Failed: {operation} ({duration:.2f}s) - {str(e)}{metadata_str}",
            extra={**metadata, "duration_seconds": duration, "status": "error"},
        )
        raise


@contextmanager
def log_phase(phase: str, **metadata: Any):
    """Context manager for logging workflow phases with nesting support.

    Similar to log_operation but uses different log levels for phase
    boundaries, making it easier to distinguish major workflow phases
    from individual operations.

    Args:
        phase: Phase name (e.g., "Extraction", "Transformation", "Loading")
        **metadata: Additional context to include in logs

    Yields:
        DagsterLogManager for logging within the phase
    """

    logger = get_dagster_logger()
    start = time.time()

    metadata_str = _format_metadata(metadata)
    logger.info(f"═══ Phase Started: {phase}{metadata_str} ═══")

    try:
        yield logger
        duration = time.time() - start
        logger.info(
            f"═══ Phase Completed: {phase} ({duration:.2f}s){metadata_str} ═══",
            extra={**metadata, "duration_seconds": duration, "status": "success"},
        )
    except Exception as e:
        duration = time.time() - start
        logger.error(
            f"═══ Phase Failed: {phase} ({duration:.2f}s) - {str(e)}{metadata_str} ═══",
            extra={**metadata, "duration_seconds": duration, "status": "error"},
        )
        raise


def _format_metadata(metadata: Dict[str, Any]) -> str:
    """Format metadata dictionary as a log-friendly string.

    Args:
        metadata: Dictionary of metadata key-value pairs

    Returns:
        Formatted string like " (key1=value1, key2=value2)" or empty string
    """

    if not metadata:
        return ""

    items = [f"{k}={v}" for k, v in metadata.items()]
    return f" ({', '.join(items)})"
