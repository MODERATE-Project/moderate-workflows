"""Common utility functions used across MODERATE workflows.

This module provides reusable utility functions for common patterns in
workflow operations, including polling, retry logic, and error handling.

Key Functions:
- poll_until(): Generic polling with exponential backoff and jitter
- retry_with_backoff(): Retry failed operations with exponential backoff

These utilities help reduce code duplication and implement best practices
for resilient workflow operations.
"""

import random
import time
from typing import Callable, Optional, TypeVar

from dagster import get_dagster_logger

T = TypeVar("T")


def poll_until(
    check_fn: Callable[[], T],
    condition_fn: Callable[[T], bool],
    timeout_seconds: int = 600,
    initial_interval: float = 1.0,
    max_interval: float = 30.0,
    backoff_factor: float = 1.5,
    jitter: bool = True,
    operation_name: Optional[str] = None,
) -> T:
    """Polls a function until a condition is met with exponential backoff.

    This function repeatedly calls check_fn until condition_fn returns True,
    with progressively longer wait times between attempts. Uses exponential
    backoff with optional jitter to prevent thundering herd problems.

    Args:
        check_fn: Function to poll that returns a value to check
        condition_fn: Function that returns True when polling should stop
        timeout_seconds: Maximum time to wait before raising TimeoutError
        initial_interval: Starting sleep interval in seconds
        max_interval: Maximum sleep interval in seconds
        backoff_factor: Multiplier for interval increase (>1.0)
        jitter: Add randomness (0.5-1.0x) to intervals to prevent thundering herd
        operation_name: Optional name for logging context

    Returns:
        The final result from check_fn when condition is met

    Raises:
        TimeoutError: If condition not met within timeout_seconds
    """

    logger = get_dagster_logger()
    start_time = time.time()
    interval = initial_interval
    attempt = 0

    operation_desc = f" for {operation_name}" if operation_name else ""
    logger.info("Starting polling%s (timeout: %ss)", operation_desc, timeout_seconds)

    while True:
        attempt += 1
        elapsed = time.time() - start_time

        logger.debug(
            "Poll attempt %s%s (elapsed: %.1fs)", attempt, operation_desc, elapsed
        )

        result = check_fn()

        if condition_fn(result):
            logger.info(
                "Polling completed%s after %s attempts (%.1fs)",
                operation_desc,
                attempt,
                elapsed,
            )
            return result

        if elapsed > timeout_seconds:
            raise TimeoutError(
                f"Condition not met within {timeout_seconds}s{operation_desc} "
                f"after {attempt} attempts"
            )

        sleep_time = min(interval, max_interval)
        if jitter:
            sleep_time *= 0.5 + 0.5 * random.random()

        logger.debug("Sleeping %.2fs before next attempt", sleep_time)
        time.sleep(sleep_time)
        interval *= backoff_factor


def retry_with_backoff(
    operation: Callable[[], T],
    max_attempts: int = 3,
    initial_interval: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,),
    operation_name: Optional[str] = None,
) -> T:
    """Retry an operation with exponential backoff on failure.

    Args:
        operation: Function to execute
        max_attempts: Maximum number of attempts
        initial_interval: Starting wait interval in seconds
        backoff_factor: Multiplier for interval increase
        exceptions: Tuple of exception types to catch and retry
        operation_name: Optional name for logging context

    Returns:
        Result from operation on success

    Raises:
        The last exception if all attempts fail
    """

    logger = get_dagster_logger()
    interval = initial_interval
    operation_desc = f" ({operation_name})" if operation_name else ""

    for attempt in range(1, max_attempts + 1):
        try:
            logger.debug("Attempt %s/%s%s", attempt, max_attempts, operation_desc)
            return operation()
        except exceptions as e:
            if attempt == max_attempts:
                logger.error(
                    "All %s attempts failed%s: %s", max_attempts, operation_desc, str(e)
                )
                raise

            logger.warning(
                "Attempt %s/%s failed%s: %s. Retrying in %.1fs...",
                attempt,
                max_attempts,
                operation_desc,
                str(e),
                interval,
            )
            time.sleep(interval)
            interval *= backoff_factor

    raise RuntimeError("Unexpected state in retry_with_backoff")
