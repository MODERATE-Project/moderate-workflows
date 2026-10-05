"""Tests for the job container runner against a real Docker daemon.

They check that the runner keeps the logs for every outcome and leaves no
container behind, including when the run is cancelled. They are skipped when
no Docker daemon is reachable.
"""

import io
import os
import signal
import threading
import time
import uuid

import docker
import pytest
from docker.errors import DockerException

from moderate.matrix_profile.container import run_container

# Prints MESSAGE, sleeps SLEEP seconds and exits with EXIT_CODE
_DOCKERFILE = """
FROM busybox:1.36
CMD echo "$MESSAGE"; sleep "${SLEEP:-0}"; exit "${EXIT_CODE:-0}"
"""

_WAIT_RUNNING_TIMEOUT_SECS = 30


@pytest.fixture(scope="module")
def client():
    try:
        client = docker.from_env()
    except DockerException as ex:
        pytest.skip(f"Docker daemon not reachable: {ex}")

    yield client
    client.close()


@pytest.fixture(scope="module")
def image(client):
    built, _ = client.images.build(fileobj=io.BytesIO(_DOCKERFILE.encode()), rm=True)
    yield built.id
    client.images.remove(built.id, force=True)


def _unique_name() -> str:
    return f"moderate-test-{uuid.uuid4().hex}"


def _container_exists(client, name: str) -> bool:
    # all=True also lists stopped containers that were not removed
    return bool(client.containers.list(all=True, filters={"name": name}))


def _interrupt_when_running(name: str) -> None:
    """Send SIGINT to this process once the container is running, as Dagster
    does when it cancels a run."""

    client = docker.from_env()
    deadline = time.monotonic() + _WAIT_RUNNING_TIMEOUT_SECS

    try:
        while time.monotonic() < deadline:
            if client.containers.list(filters={"name": name, "status": "running"}):
                os.kill(os.getpid(), signal.SIGINT)
                return

            time.sleep(0.2)
    finally:
        client.close()


@pytest.mark.parametrize(
    "environment,timeout_secs,expected_error",
    [
        ({"EXIT_CODE": "0"}, 30, None),
        ({"EXIT_CODE": "3"}, 30, "Container exited with code 3"),
        ({"SLEEP": "60"}, 1, "Container timed out after 1 seconds"),
    ],
    ids=["success", "failure", "timeout"],
)
def test_run_container_outcomes(
    client, image, environment, timeout_secs, expected_error
):
    name = _unique_name()
    message = f"output of {name}"

    result = run_container(
        client=client,
        name=name,
        image=image,
        environment={"MESSAGE": message, **environment},
        timeout_secs=timeout_secs,
        pull_policy="Never",
    )

    assert result.success is (expected_error is None)
    assert result.error_message == expected_error
    assert message in result.logs
    assert not _container_exists(client, name)


def test_run_container_removes_container_on_interrupt(client, image):
    name = _unique_name()
    interrupter = threading.Thread(target=_interrupt_when_running, args=(name,))
    interrupter.start()

    try:
        with pytest.raises(KeyboardInterrupt):
            run_container(
                client=client,
                name=name,
                image=image,
                environment={"SLEEP": "60"},
                timeout_secs=60,
                pull_policy="Never",
            )
    finally:
        interrupter.join()

    assert not _container_exists(client, name)
