"""Utilities for retrieving and processing Kubernetes pod logs.

This module provides functions for:
- Retrieving logs from pods associated with Kubernetes jobs
- Extracting user-friendly error summaries from verbose logs
- Uploading full logs to S3 for reference
- Executing Kubernetes jobs with log retrieval on failure

These utilities are used to improve error reporting for Matrix Profile jobs
by providing actionable information to end users instead of exposing
Kubernetes-internal details.
"""

import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, List, Optional, Tuple

from dagster import OpExecutionContext, get_dagster_logger
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

# Maximum number of lines to retrieve from pod logs
_DEFAULT_TAIL_LINES = 800

# Maximum lines to include in error summary
_DEFAULT_SUMMARY_MAX_LINES = 30

# Maximum characters for error summary to prevent excessively long messages
_MAX_SUMMARY_CHARS = 2000

# Maximum log size to upload to S3 (5 MB)
_MAX_LOG_SIZE_BYTES = 5 * 1024 * 1024


def _load_k8s_config() -> None:
    """Load Kubernetes configuration.

    Attempts to load in-cluster config first (when running inside a pod),
    falls back to kubeconfig file for local development.
    """
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()


def retrieve_pod_logs(
    namespace: str,
    job_name: str,
    tail_lines: int = _DEFAULT_TAIL_LINES,
) -> Tuple[str, Optional[str]]:
    """Retrieve logs from pods associated with a Kubernetes job.

    Args:
        namespace: Kubernetes namespace where the job runs.
        job_name: Name of the Kubernetes job.
        tail_lines: Number of lines to retrieve from the end of logs.

    Returns:
        Tuple of (logs_content, error_message). If successful, logs_content
        contains the pod logs and error_message is None. If failed,
        logs_content is empty and error_message describes the failure.
    """
    logger = get_dagster_logger()

    try:
        _load_k8s_config()
    except Exception as ex:
        logger.warning("Failed to load Kubernetes config: %s", ex)
        return "", f"Failed to load Kubernetes config: {ex}"

    v1 = client.CoreV1Api()

    try:
        # Find pods created by this job using the job-name label
        label_selector = f"job-name={job_name}"
        pods = v1.list_namespaced_pod(
            namespace=namespace, label_selector=label_selector
        )

        if not pods.items:
            logger.warning(
                "No pods found for job %s in namespace %s", job_name, namespace
            )
            return "", f"No pods found for job {job_name}"

        # Collect logs from all pods (usually just one for a simple job)
        all_logs = []
        for pod in pods.items:
            pod_name = pod.metadata.name
            logger.debug("Retrieving logs from pod %s", pod_name)

            try:
                pod_logs = v1.read_namespaced_pod_log(
                    name=pod_name,
                    namespace=namespace,
                    tail_lines=tail_lines,
                )
                all_logs.append(f"=== Pod: {pod_name} ===\n{pod_logs}")
            except ApiException as ex:
                logger.warning("Failed to retrieve logs from pod %s: %s", pod_name, ex)
                all_logs.append(
                    f"=== Pod: {pod_name} ===\n[Failed to retrieve logs: {ex.reason}]"
                )

        combined_logs = "\n\n".join(all_logs)
        return combined_logs, None

    except ApiException as ex:
        logger.warning("Kubernetes API error while retrieving logs: %s", ex)
        return "", f"Kubernetes API error: {ex.reason}"
    except Exception as ex:
        logger.warning("Unexpected error retrieving pod logs: %s", ex)
        return "", f"Unexpected error: {ex}"


def _extract_python_traceback(logs: str) -> Optional[str]:
    """Extract the last Python traceback from logs.

    Args:
        logs: Full log content.

    Returns:
        The last traceback block if found, None otherwise.
    """
    # Pattern to match Python tracebacks
    # Matches from "Traceback (most recent call last):" to the exception message
    traceback_pattern = r"(Traceback \(most recent call last\):.*?)(?=\nTraceback \(most recent call last\):|\Z)"

    matches = re.findall(traceback_pattern, logs, re.DOTALL)
    if matches:
        # Return the last traceback (most recent error)
        return matches[-1].strip()
    return None


def _extract_error_lines(logs: str, max_lines: int = 10) -> Optional[str]:
    """Extract lines containing error-related keywords.

    Args:
        logs: Full log content.
        max_lines: Maximum number of error lines to extract.

    Returns:
        Concatenated error lines if found, None otherwise.
    """
    error_keywords = [
        r"^\s*ERROR[:\s]",
        r"^\s*FATAL[:\s]",
        r"^\s*CRITICAL[:\s]",
        r"\bException\b",
        r"\bError\b",
        r"\bFailed\b",
        r"\bfailed\b",
    ]

    pattern = "|".join(error_keywords)
    lines = logs.split("\n")
    error_lines = []

    for line in lines:
        if re.search(pattern, line, re.IGNORECASE):
            error_lines.append(line.strip())

    if error_lines:
        # Return the last N error lines (most relevant)
        return "\n".join(error_lines[-max_lines:])
    return None


def extract_error_summary(
    logs: str,
    max_lines: int = _DEFAULT_SUMMARY_MAX_LINES,
    max_chars: int = _MAX_SUMMARY_CHARS,
) -> str:
    """Extract an actionable error summary from verbose logs.

    Strategy:
    1. Look for Python traceback (last exception block)
    2. Look for lines containing 'error', 'exception', 'failed'
    3. Fall back to last N lines if no patterns found

    Args:
        logs: Full log content.
        max_lines: Maximum number of lines for fallback extraction.
        max_chars: Maximum characters in the summary.

    Returns:
        A user-friendly error summary.
    """
    if not logs or not logs.strip():
        return "No log output available."

    # Strategy 1: Try to extract Python traceback
    traceback = _extract_python_traceback(logs)
    if traceback:
        summary = traceback
        if len(summary) > max_chars:
            # Truncate but keep the end (contains the actual error)
            summary = "...[truncated]...\n" + summary[-(max_chars - 20) :]
        return summary

    # Strategy 2: Try to extract error-related lines
    error_lines = _extract_error_lines(logs, max_lines=max_lines)
    if error_lines:
        summary = error_lines
        if len(summary) > max_chars:
            summary = summary[:max_chars] + "\n...[truncated]..."
        return summary

    # Strategy 3: Fall back to last N lines
    lines = logs.strip().split("\n")
    last_lines = lines[-max_lines:]
    summary = "\n".join(last_lines)

    if len(summary) > max_chars:
        summary = summary[:max_chars] + "\n...[truncated]..."

    return summary


def upload_logs_to_s3(
    s3_client: Any,
    bucket: str,
    logs: str,
    workflow_job_id: int,
) -> Tuple[str, Optional[str]]:
    """Upload full logs to S3 and return the object key.

    Args:
        s3_client: Boto3 S3 client instance.
        bucket: S3 bucket name.
        logs: Full log content to upload.
        workflow_job_id: ID of the workflow job for naming.

    Returns:
        Tuple of (object_key, error_message). If successful, object_key
        contains the S3 key and error_message is None. If failed,
        object_key is empty and error_message describes the failure.
    """
    logger = get_dagster_logger()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    unique_id = uuid.uuid4().hex[:8]
    object_key = (
        f"logs/matrix-profile-job-{workflow_job_id}-{timestamp}-{unique_id}.log"
    )

    # Truncate logs if too large
    log_bytes = logs.encode("utf-8")
    if len(log_bytes) > _MAX_LOG_SIZE_BYTES:
        logger.warning(
            "Log size (%d bytes) exceeds maximum (%d bytes), truncating",
            len(log_bytes),
            _MAX_LOG_SIZE_BYTES,
        )
        # Truncate from the beginning, keeping the end (most relevant)
        logs = logs[-((_MAX_LOG_SIZE_BYTES // 2)) :]
        logs = "[...log truncated due to size...]\n\n" + logs
        log_bytes = logs.encode("utf-8")

    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=object_key,
            Body=log_bytes,
        )
        logger.info("Uploaded logs to s3://%s/%s", bucket, object_key)
        return object_key, None

    except Exception as ex:
        # Log detailed error information for debugging S3/GCS issues
        logger.error(
            "Failed to upload logs to S3 (bucket=%s, key=%s, size=%d bytes): %s",
            bucket,
            object_key,
            len(log_bytes),
            ex,
        )
        return "", f"Failed to upload logs: {ex}"


def parse_job_name_from_exception(exception: Exception) -> Optional[str]:
    """Attempt to parse the Kubernetes job name from an exception.

    dagster-k8s exceptions typically include the job name in the message.
    Job names are 32-character hex strings (e.g., 5a73cda947b1e33caee479613ec18ebe)
    or dagster-prefixed names (e.g., dagster-run-xxx).

    Args:
        exception: The exception raised by execute_k8s_job.

    Returns:
        The job name if found, None otherwise.
    """
    exception_str = str(exception)

    # Pattern to match job names in dagster-k8s exceptions
    # Order matters: more specific patterns first
    patterns = [
        # Hex job names (32 chars) - most common for execute_k8s_job
        # Matches "for job <hex>" or "job <hex>" patterns
        r"(?:for\s+)?job\s+([a-f0-9]{32})",
        # Dagster run/step job names
        r"(dagster-run-[a-f0-9-]+)",
        r"(dagster-step-[a-f0-9]+)",
        # Generic job name pattern (only as fallback, requires minimum length)
        r"job[:\s]+([a-z0-9][-a-z0-9]{7,}[a-z0-9])",
    ]

    for pattern in patterns:
        match = re.search(pattern, exception_str, re.IGNORECASE)
        if match:
            # Return the captured group
            return match.group(1)

    return None


def get_current_namespace() -> str:
    """Get the current Kubernetes namespace.

    When running in a pod, reads from the service account namespace file.
    Falls back to 'default' for local development.

    Returns:
        The current namespace name.
    """
    try:
        with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return "default"


def delete_k8s_job(job_name: str, namespace: str) -> None:
    """Delete a Kubernetes job.

    Args:
        job_name: Name of the job to delete.
        namespace: Kubernetes namespace where the job runs.
    """
    logger = get_dagster_logger()

    try:
        _load_k8s_config()
    except Exception as ex:
        logger.warning("Failed to load Kubernetes config for job deletion: %s", ex)
        return

    batch_v1 = client.BatchV1Api()

    try:
        batch_v1.delete_namespaced_job(
            name=job_name,
            namespace=namespace,
            body=client.V1DeleteOptions(propagation_policy="Foreground"),
        )
        logger.info("Deleted Kubernetes job %s in namespace %s", job_name, namespace)
    except ApiException as ex:
        if ex.status == 404:
            logger.debug("Job %s already deleted or not found", job_name)
        else:
            logger.warning("Failed to delete job %s: %s", job_name, ex)


@dataclass
class K8sJobResult:
    """Result of a Kubernetes job execution.

    Attributes:
        success: Whether the job completed successfully.
        job_name: Name of the Kubernetes job.
        namespace: Kubernetes namespace where the job ran.
        logs: Pod logs if available (especially on failure).
        error_message: Error description if the job failed.
    """

    success: bool
    job_name: str
    namespace: str
    logs: Optional[str] = None
    error_message: Optional[str] = None


# Default timeout for waiting operations (seconds)
_DEFAULT_WAIT_TIMEOUT = 600

# Poll interval for job status checks (seconds)
_JOB_POLL_INTERVAL = 2


def _create_k8s_job_spec(
    job_name: str,
    image: str,
    env_vars: List[str],
    image_pull_policy: str = "Always",
) -> client.V1Job:
    """Create a Kubernetes Job specification.

    Args:
        job_name: Name for the job.
        image: Container image to run.
        env_vars: List of environment variables in "KEY=VALUE" format.
        image_pull_policy: Image pull policy (Always, IfNotPresent, Never).

    Returns:
        V1Job object ready to be created.
    """
    # Parse environment variables
    env_list = []
    for env_var in env_vars:
        if "=" in env_var:
            key, value = env_var.split("=", 1)
            env_list.append(client.V1EnvVar(name=key, value=value))

    container = client.V1Container(
        name="main",
        image=image,
        image_pull_policy=image_pull_policy,
        env=env_list,
    )

    pod_spec = client.V1PodSpec(
        containers=[container],
        restart_policy="Never",
    )

    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"job-name": job_name}),
        spec=pod_spec,
    )

    job_spec = client.V1JobSpec(
        template=template,
        backoff_limit=0,
        ttl_seconds_after_finished=300,
    )

    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(name=job_name),
        spec=job_spec,
    )

    return job


def _wait_for_job_completion(
    batch_api: client.BatchV1Api,
    job_name: str,
    namespace: str,
    timeout: int,
) -> Tuple[bool, Optional[str]]:
    """Wait for a Kubernetes job to complete.

    Args:
        batch_api: Kubernetes Batch API client.
        job_name: Name of the job to wait for.
        namespace: Kubernetes namespace.
        timeout: Maximum time to wait in seconds.

    Returns:
        Tuple of (success, error_message).
    """
    logger = get_dagster_logger()
    start_time = time.time()

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout:
            return False, f"Job timed out after {timeout} seconds"

        try:
            job = batch_api.read_namespaced_job_status(
                name=job_name, namespace=namespace
            )
        except ApiException as ex:
            return False, f"Failed to read job status: {ex.reason}"

        status = job.status

        # Check for completion
        if status.succeeded is not None and status.succeeded > 0:
            logger.debug("Job %s completed successfully", job_name)
            return True, None

        # Check for failure
        if status.failed is not None and status.failed > 0:
            conditions = status.conditions or []
            failure_reason = "Job failed"
            for condition in conditions:
                if condition.type == "Failed" and condition.status == "True":
                    failure_reason = (
                        condition.message or condition.reason or failure_reason
                    )
            return False, failure_reason

        time.sleep(_JOB_POLL_INTERVAL)


def execute_k8s_job_with_log_capture(
    context: OpExecutionContext,
    image: str,
    env_vars: List[str],
    namespace: Optional[str] = None,
    image_pull_policy: str = "Always",
    timeout: int = _DEFAULT_WAIT_TIMEOUT,
    job_name: Optional[str] = None,
) -> K8sJobResult:
    """Execute a Kubernetes job with log capture on failure.

    This function creates and runs a Kubernetes job, waiting for completion.
    Unlike dagster-k8s's execute_k8s_job, this function retrieves pod logs
    BEFORE deleting a failed job, ensuring error information is preserved.

    Args:
        context: Dagster op execution context.
        image: Container image to run.
        env_vars: List of environment variables in "KEY=VALUE" format.
        namespace: Kubernetes namespace (defaults to current namespace).
        image_pull_policy: Image pull policy.
        timeout: Maximum time to wait for job completion in seconds.
        job_name: Optional job name (auto-generated if not provided).

    Returns:
        K8sJobResult with success status and logs on failure.
    """
    logger = context.log

    # Determine namespace
    if namespace is None:
        namespace = get_current_namespace()

    # Generate job name if not provided
    if job_name is None:
        job_name = uuid.uuid4().hex

    logger.info(
        "Creating Kubernetes job %s in namespace %s (image=%s)...",
        job_name,
        namespace,
        image,
    )

    # Load k8s config
    try:
        _load_k8s_config()
    except Exception as ex:
        return K8sJobResult(
            success=False,
            job_name=job_name,
            namespace=namespace,
            error_message=f"Failed to load Kubernetes config: {ex}",
        )

    batch_api = client.BatchV1Api()

    # Create the job
    job_spec = _create_k8s_job_spec(
        job_name=job_name,
        image=image,
        env_vars=env_vars,
        image_pull_policy=image_pull_policy,
    )

    try:
        batch_api.create_namespaced_job(namespace=namespace, body=job_spec)
    except ApiException as ex:
        return K8sJobResult(
            success=False,
            job_name=job_name,
            namespace=namespace,
            error_message=f"Failed to create job: {ex.reason}",
        )

    logger.info("Waiting for Kubernetes job %s to finish...", job_name)

    # Wait for completion
    success, error_message = _wait_for_job_completion(
        batch_api=batch_api,
        job_name=job_name,
        namespace=namespace,
        timeout=timeout,
    )

    # On failure, retrieve logs BEFORE deleting the job
    logs = None
    if not success:
        logger.info("Job %s failed, retrieving logs before cleanup...", job_name)
        logs, logs_error = retrieve_pod_logs(
            namespace=namespace,
            job_name=job_name,
        )
        if logs_error:
            logger.warning("Failed to retrieve pod logs: %s", logs_error)

    # Clean up the job
    logger.info("Deleting Kubernetes job %s in namespace %s...", job_name, namespace)
    delete_k8s_job(job_name=job_name, namespace=namespace)

    return K8sJobResult(
        success=success,
        job_name=job_name,
        namespace=namespace,
        logs=logs,
        error_message=error_message,
    )
