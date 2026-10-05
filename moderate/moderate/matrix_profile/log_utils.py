"""Turn job container logs into error reports for end users.

- extract_error_summary() pulls a short, readable error out of verbose logs.
- upload_logs_to_s3() stores the logs in S3 so users can download them.
"""

import re
import uuid
from datetime import datetime, timezone
from typing import Any, Optional, Tuple

from dagster import get_dagster_logger

# Maximum lines to include in error summary
_DEFAULT_SUMMARY_MAX_LINES = 30

# Maximum characters for error summary to prevent excessively long messages
_MAX_SUMMARY_CHARS = 2000

# Maximum log size to upload to S3 (5 MB)
_MAX_LOG_SIZE_BYTES = 5 * 1024 * 1024


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
