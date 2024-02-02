import pprint
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import requests
from dagster import DagsterLogManager

from moderate.resources import PlatformAPIResource


@dataclass
class KeycloakUserDict:
    user_dict: Dict[str, Any]

    @property
    def username(self) -> str:
        return self.user_dict["username"]


@dataclass
class DIDResponseDict:
    the_dict: Dict[str, Any]

    @property
    def task_id(self) -> Optional[int]:
        return self.the_dict.get("task_id")

    @property
    def did_exists_already(self) -> bool:
        return not self.task_id and self.the_dict.get("user_meta")


@dataclass
class ProofResponseDict:
    the_dict: Dict[str, Any]

    @property
    def task_id(self) -> Optional[int]:
        return self.the_dict.get("task_id")

    @property
    def proof_exists_already(self) -> bool:
        return not self.task_id and self.the_dict.get("obj")


@dataclass
class TaskResponseDict:
    the_dict: Dict[str, Any]

    @property
    def is_successful(self) -> bool:
        return self.the_dict.get("result", None) is not None

    @property
    def is_error(self) -> bool:
        return self.the_dict.get("error", None) is not None

    @property
    def is_finished(self) -> bool:
        return self.is_successful or self.is_error

    @property
    def error_message(self) -> str:
        return str(self.the_dict.get("error", ""))

    @property
    def result(self) -> Any:
        return self.the_dict.get("result")

    def raise_error(self):
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
    time_start = time.time()

    logger.info("Entering loop to wait for task %s to finish", task_url)

    while True:
        logger.debug("GET %s", task_url)
        headers = platform_api.get_authorization_header()
        resp = requests.get(task_url, headers=headers)
        resp.raise_for_status()
        resp_json_dict = resp.json()
        logger.debug("Response:\n%s", pprint.pformat(resp_json_dict))
        task_response = TaskResponseDict(the_dict=resp_json_dict)

        if task_response.is_finished:
            break

        now = time.time()
        seconds_elapsed = now - time_start
        logger.debug("Seconds elapsed: %s", round(seconds_elapsed, 1))

        if seconds_elapsed > timeout_seconds:
            raise Exception(
                f"""
                Timeout of {timeout_seconds} seconds exceeded 
                while waiting for task {task_url} to finish
                """
            )

        logger.debug("Sleeping for %s seconds", iter_sleep_seconds)
        time.sleep(iter_sleep_seconds)

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
