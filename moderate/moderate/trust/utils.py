"""Utility classes and functions for trust service integration.

This module provides helper dataclasses and utilities for integrating with
the MODERATE trust services, including DID (Decentralized Identifier) generation
and asset proof creation.

Key Components:
- ResponseDict wrappers: Type-safe access to API responses
- wait_for_task(): Polling utility for async task completion
- add_rounded_datetime(): Timestamp rounding for sensor run keys

The trust services use an async task pattern where operations return a task_id
that must be polled until completion. The wait_for_task() function implements
this pattern with configurable timeout and polling intervals.

Example:
    # Wait for DID generation task to complete
    task_url = platform_api.url_check_did_task(task_id)
    result = wait_for_task(
        task_url=task_url,
        platform_api=platform_api,
        logger=logger,
        timeout_seconds=600
    )

    if result.is_successful:
        did_data = result.result
"""

import pprint
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import requests
from dagster import DagsterLogManager

from moderate.common import poll_until
from moderate.resources import PlatformAPIResource


@dataclass
class ResponseDict:
    """Base class for API response wrappers with safe dictionary access.

    Provides common functionality for wrapping API responses with type-safe
    property accessors. Subclasses can add domain-specific properties.
    """

    the_dict: Dict[str, Any]

    def get(self, key: str, default: Any = None) -> Any:
        """Safe dictionary access with default value."""
        return self.the_dict.get(key, default)

    def require(self, key: str) -> Any:
        """Dictionary access that raises KeyError if key is missing."""
        if key not in self.the_dict:
            raise KeyError(f"Required key '{key}' not found in response")
        return self.the_dict[key]


@dataclass
class KeycloakUserDict(ResponseDict):
    """Wrapper for Keycloak user dictionary responses."""

    @property
    def user_dict(self) -> Dict[str, Any]:
        return self.the_dict

    @property
    def username(self) -> str:
        return self.the_dict["username"]


@dataclass
class DIDResponseDict(ResponseDict):
    """Wrapper for DID (Decentralized Identifier) creation responses."""

    @property
    def task_id(self) -> Optional[int]:
        return self.the_dict.get("task_id")

    @property
    def did_exists_already(self) -> bool:
        """Check if DID was already created (no task needed)."""
        return not self.task_id and self.the_dict.get("user_meta")


@dataclass
class ProofResponseDict(ResponseDict):
    """Wrapper for asset proof creation responses."""

    @property
    def task_id(self) -> Optional[int]:
        return self.the_dict.get("task_id")

    @property
    def proof_exists_already(self) -> bool:
        """Check if proof was already created (no task needed)."""
        return not self.task_id and self.the_dict.get("obj")


@dataclass
class TaskResponseDict(ResponseDict):
    """Wrapper for async task status responses.

    Async operations return a task_id that must be polled. This wrapper
    provides convenient access to task state (pending, successful, error).
    """

    @property
    def is_successful(self) -> bool:
        """Task completed successfully with a result."""
        return self.the_dict.get("result", None) is not None

    @property
    def is_error(self) -> bool:
        """Task completed with an error."""
        return self.the_dict.get("error", None) is not None

    @property
    def is_finished(self) -> bool:
        """Task is in a terminal state (success or error)."""
        return self.is_successful or self.is_error

    @property
    def error_message(self) -> str:
        """Get error message if task failed."""
        return str(self.the_dict.get("error", ""))

    @property
    def result(self) -> Any:
        """Get task result if successful."""
        return self.the_dict.get("result")

    def raise_error(self):
        """Raise exception if task failed."""
        if not self.is_error:
            return

        raise Exception(self.error_message)


def wait_for_task(
    task_url: str,
    platform_api: PlatformAPIResource,
    logger: DagsterLogManager,
    timeout_seconds: int = 600,
    iter_sleep_seconds: float = 5.0,
) -> TaskResponseDict:
    """Wait for an async task to complete by polling the task status endpoint.

    Uses exponential backoff polling to reduce server load while waiting for
    task completion. The task is considered finished when either a result or
    error is present in the response.

    Args:
        task_url: Full URL to the task status endpoint
        platform_api: PlatformAPIResource for authentication
        logger: Dagster logger for operation logging
        timeout_seconds: Maximum time to wait for task completion
        iter_sleep_seconds: Initial polling interval (exponential backoff applied)

    Returns:
        TaskResponseDict containing the final task state

    Raises:
        TimeoutError: If task doesn't complete within timeout_seconds
        requests.HTTPError: If API request fails
    """
    logger.info("Waiting for task to finish: %s", task_url)

    def check_task_status() -> TaskResponseDict:
        """Fetch current task status from API."""
        logger.debug("GET %s", task_url)
        headers = platform_api.get_authorization_header()
        resp = requests.get(task_url, headers=headers)
        resp.raise_for_status()
        resp_json_dict = resp.json()
        logger.debug("Response:\n%s", pprint.pformat(resp_json_dict))
        return TaskResponseDict(the_dict=resp_json_dict)

    task_response = poll_until(
        check_fn=check_task_status,
        condition_fn=lambda task: task.is_finished,
        timeout_seconds=timeout_seconds,
        initial_interval=iter_sleep_seconds,
        operation_name=f"task_{task_url.split('/')[-1]}",
    )

    logger.info(
        "Task finished (%s):\n%s", task_url, pprint.pformat(task_response.the_dict)
    )

    return task_response


def add_rounded_datetime(
    run_key: str, minutes_interval: int, now: Optional[datetime] = None
) -> str:
    now = datetime.utcnow() if not now else now

    rounded = datetime(
        year=now.year,
        month=now.month,
        day=now.day,
        hour=now.hour,
        minute=(now.minute // minutes_interval) * minutes_interval,
    )

    if now.minute % minutes_interval > minutes_interval / 2:
        rounded += timedelta(minutes=minutes_interval)

    rounded_str = rounded.strftime("%Y%m%d%H%M")

    return f"{run_key}_{rounded_str}"
