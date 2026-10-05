"""Run job containers on the Docker daemon.

The runner removes the container after every outcome: success, failure,
timeout and run cancellation. When the container stops or times out, the
runner reads its logs first, since they disappear with the container.
"""

from dataclasses import dataclass
from typing import Dict, Optional

import docker
import requests
from dagster import get_dagster_logger
from docker.errors import DockerException, ImageNotFound, NotFound
from docker.models.containers import Container

from moderate.common import poll_until

# Lines kept from the end of the container output
_LOG_TAIL_LINES = 800

# Longest pause between status checks. Cancelling a run can take this long to
# remove the container.
_POLL_MAX_INTERVAL_SECS = 2.0

_FINISHED_STATUSES = ("exited", "dead")

# docker-py raises requests errors, which are not DockerException subclasses,
# when the connection to the daemon fails
_CLIENT_ERRORS = (DockerException, requests.RequestException)


@dataclass(frozen=True)
class ContainerResult:
    """Outcome of a job container run.

    Attributes:
        success: Whether the container exited with code 0 before the timeout.
        logs: Tail of the container output, if the container ran.
        error_message: Why the run failed, if it did.
    """

    success: bool
    logs: Optional[str] = None
    error_message: Optional[str] = None


def image_reference(repository: str, tag: str) -> str:
    """Join a repository with a tag or a "sha256:" digest."""

    separator = "@" if tag.startswith("sha256:") else ":"
    return f"{repository}{separator}{tag}"


def run_container(
    client: docker.DockerClient,
    name: str,
    image: str,
    environment: Dict[str, str],
    timeout_secs: int,
    pull_policy: str,
    network: Optional[str] = None,
) -> ContainerResult:
    """Run a container until it exits or times out, then remove it.

    Returns Docker errors as a failed result. Interrupts, such as a Dagster
    run cancellation, propagate after the container is removed.

    Args:
        client: Docker client.
        name: Container name, unique on the daemon. The container is removed
            by name, so an interrupt during creation can't leave it behind.
        image: Image reference, by tag or digest.
        environment: Environment variables for the container.
        timeout_secs: Seconds the container may run before the runner kills it.
        pull_policy: "Always", "IfNotPresent" or "Never".
        network: Network to attach the container to. None means Docker's
            default bridge network.

    Returns:
        The outcome of the run, with the container logs when available.

    Raises:
        ValueError: If the pull policy is unknown.
    """

    logger = get_dagster_logger()

    try:
        _ensure_image(client=client, image=image, pull_policy=pull_policy)

        container = client.containers.create(
            image, name=name, environment=environment, network=network
        )

        container.start()
        logger.info("Started container %s (image=%s)", name, image)
        error_message = _wait_for_exit(container=container, timeout_secs=timeout_secs)
        logs = container.logs(tail=_LOG_TAIL_LINES).decode("utf-8", errors="replace")

        return ContainerResult(
            success=error_message is None, logs=logs, error_message=error_message
        )
    except _CLIENT_ERRORS as ex:
        return ContainerResult(success=False, error_message=f"Docker error: {ex}")
    finally:
        _remove_container(client=client, name=name)


def _ensure_image(client: docker.DockerClient, image: str, pull_policy: str) -> None:
    if pull_policy == "Never":
        return

    if pull_policy == "IfNotPresent":
        try:
            client.images.get(image)
            return
        except ImageNotFound:
            pass
    elif pull_policy != "Always":
        raise ValueError(f"Unknown image pull policy: {pull_policy}")

    get_dagster_logger().info("Pulling image %s", image)
    client.images.pull(image)


def _wait_for_exit(container: Container, timeout_secs: int) -> Optional[str]:
    """Wait for the container to stop.

    Returns:
        None if the container exited with code 0, otherwise why it failed.
    """

    def get_status() -> str:
        container.reload()
        return container.status

    try:
        poll_until(
            check_fn=get_status,
            condition_fn=lambda status: status in _FINISHED_STATUSES,
            timeout_seconds=timeout_secs,
            max_interval=_POLL_MAX_INTERVAL_SECS,
            operation_name=f"container {container.name}",
        )
    except TimeoutError:
        return f"Container timed out after {timeout_secs} seconds"

    exit_code = container.attrs["State"]["ExitCode"]

    if exit_code != 0:
        return f"Container exited with code {exit_code}"

    return None


def _remove_container(client: docker.DockerClient, name: str) -> None:
    """Kill and remove the container, logging errors instead of raising them.

    A failed cleanup must not replace an exception or cancellation interrupt
    that is already propagating.
    """

    logger = get_dagster_logger()

    try:
        client.api.remove_container(name, force=True)
        logger.info("Removed container %s", name)
    except NotFound:
        logger.debug("Container %s does not exist, nothing to remove", name)
    except _CLIENT_ERRORS as ex:
        logger.warning("Failed to remove container %s: %s", name, ex)
