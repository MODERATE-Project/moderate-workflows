"""Utilities for retrieving and processing Kubernetes pod logs.

This module provides functions for:
- Retrieving logs from pods associated with Kubernetes jobs
- Extracting user-friendly error summaries from verbose logs
- Uploading full logs to S3 for reference

These utilities are used to improve error reporting for Matrix Profile jobs
by providing actionable information to end users instead of exposing
Kubernetes-internal details.
"""

import re
import uuid
from datetime import datetime
from typing import Any, Optional, Tuple

from dagster import get_dagster_logger
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

    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
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
            ContentType="text/plain",
        )
        logger.info("Uploaded logs to s3://%s/%s", bucket, object_key)
        return object_key, None

    except Exception as ex:
        logger.error("Failed to upload logs to S3: %s", ex)
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
