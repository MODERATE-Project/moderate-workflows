import dataclasses
import os
import uuid
from dataclasses import dataclass
from typing import Any, Union

import requests
from botocore.exceptions import ClientError
from dagster import (
    Config,
    OpExecutionContext,
    RunConfig,
    RunRequest,
    SensorEvaluationContext,
    job,
    op,
    sensor,
)
from pydantic import BaseModel, ValidationError

from moderate.enums import Variables
from moderate.matrix_profile.log_utils import (
    execute_k8s_job_with_log_capture,
    extract_error_summary,
    upload_logs_to_s3,
)
from moderate.resources import (
    PlatformAPIResource,
    RabbitResource,
    S3ObjectStorageResource,
)


class MatrixProfileJobConfig(Config):
    image: str = (
        "europe-west1-docker.pkg.dev/moderate-common/moderate-images/moderate-matrix-profile-workflow"
    )

    tag: str = "main"
    timeout_secs: int = 3600
    image_pull_policy: str = "Always"
    output_bucket: str
    file_url: str
    analysis_variable: str
    workflow_job_id: int


def _check_file_exists(s3_client: Any, bucket_name: str, file_key: str):
    try:
        s3_client.head_object(Bucket=bucket_name, Key=file_key)
        return True
    except ClientError as ex:
        if ex.response["Error"]["Code"] == "404":
            return False
        else:
            raise


@dataclass
class MatrixProfileJobResult:
    """Result of a Matrix Profile job execution.

    Attributes:
        workflow_job_id: ID of the workflow job in the Platform API.
        error: User-friendly error summary if the job failed.
        error_logs_key: S3 object key for full error logs (if job failed).
        output_bucket: S3 bucket containing the output report.
        output_key: S3 object key for the output report.
    """

    workflow_job_id: int
    error: Union[str, None] = None
    error_logs_key: Union[str, None] = None
    output_bucket: Union[str, None] = None
    output_key: Union[str, None] = None


def handle_job_failure(
    context: OpExecutionContext,
    config: MatrixProfileJobConfig,
    s3_object_storage: S3ObjectStorageResource,
    logs: Union[str, None],
    error_message: Union[str, None],
) -> MatrixProfileJobResult:
    """Handle a failed Matrix Profile job by processing logs and creating result.

    Args:
        context: Dagster op execution context.
        config: Job configuration.
        s3_object_storage: S3 storage resource for uploading logs.
        logs: Pod logs retrieved before job cleanup (may be None).
        error_message: Error message from job failure.

    Returns:
        MatrixProfileJobResult with error information.
    """
    context.log.error("Matrix Profile job failed: %s", error_message)

    error_summary = None
    error_logs_key = None

    if logs:
        # Extract user-friendly error summary
        error_summary = extract_error_summary(logs)
        context.log.debug("Extracted error summary: %s", error_summary)

        # Upload full logs to S3 for reference
        s3_client = s3_object_storage.get_client()
        error_logs_key, upload_error = upload_logs_to_s3(
            s3_client=s3_client,
            bucket=s3_object_storage.job_outputs_bucket_name,
            logs=logs,
            workflow_job_id=config.workflow_job_id,
        )

        if upload_error:
            context.log.warning("Failed to upload logs to S3: %s", upload_error)
    else:
        context.log.warning("No pod logs available for error analysis")

    # Fall back to generic message if no summary could be extracted
    if not error_summary:
        error_summary = (
            "The analysis job failed. Please check the input data format "
            "and ensure the analysis variable exists in the dataset."
        )

    return MatrixProfileJobResult(
        workflow_job_id=config.workflow_job_id,
        error=error_summary,
        error_logs_key=error_logs_key,
        output_bucket=s3_object_storage.job_outputs_bucket_name,
    )


@op
def run_matrix_profile(
    context: OpExecutionContext,
    config: MatrixProfileJobConfig,
    s3_object_storage: S3ObjectStorageResource,
) -> MatrixProfileJobResult:
    image = "{}:{}".format(config.image, config.tag)
    output_key = "matrixprofile-{}.html".format(uuid.uuid4().hex)

    context.log.info(
        "Running Matrix Profile job (image=%s) (output_key=%s)", image, output_key
    )

    env_vars = [
        "S3_ACCESS_KEY_ID={}".format(s3_object_storage.access_key_id),
        "S3_SECRET_ACCESS_KEY={}".format(s3_object_storage.secret_access_key),
        "OUTPUT_KEY={}".format(output_key),
        "OUTPUT_BUCKET={}".format(config.output_bucket),
        "FILE_URL={}".format(config.file_url),
        "ANALYSIS_VARIABLE={}".format(config.analysis_variable),
    ]

    # Use custom job execution that captures logs before cleanup on failure
    job_result = execute_k8s_job_with_log_capture(
        context=context,
        image=image,
        env_vars=env_vars,
        image_pull_policy=config.image_pull_policy,
        timeout=config.timeout_secs,
    )

    if not job_result.success:
        return handle_job_failure(
            context=context,
            config=config,
            s3_object_storage=s3_object_storage,
            logs=job_result.logs,
            error_message=job_result.error_message,
        )

    context.log.info(
        "Checking if output report exists: s3://%s/%s",
        config.output_bucket,
        output_key,
    )

    output_exists = _check_file_exists(
        s3_client=s3_object_storage.get_client(),
        bucket_name=config.output_bucket,
        file_key=output_key,
    )

    if output_exists:
        context.log.info("Output report found")
    else:
        context.log.error("Output report not found")

    return MatrixProfileJobResult(
        workflow_job_id=config.workflow_job_id,
        output_bucket=config.output_bucket,
        output_key=output_key,
    )


@op
def publish_matrix_profile_result(
    context: OpExecutionContext,
    platform_api: PlatformAPIResource,
    job_result: MatrixProfileJobResult,
):
    context.log.debug("Received Matrix Profile job result: %s", job_result)

    req_patch = requests.patch(
        platform_api.url_patch_job(job_result.workflow_job_id),
        headers=platform_api.get_authorization_header(),
        json={"results": dataclasses.asdict(job_result)},
    )

    req_patch.raise_for_status()
    resp_json = req_patch.json()

    context.log.info("Updated workflow job: %s", resp_json)


@job
def matrix_profile_job():
    publish_matrix_profile_result(run_matrix_profile())


class MatrixProfileMessage(BaseModel):
    workflow_job_id: int
    analysis_variable: str
    bucket: str
    key: str
    image: Union[str, None] = None
    tag: Union[str, None] = None


_DEFAULT_PRESIGNED_URLS_EXPIRATION_SECS = 3600 * 24


@sensor(job=matrix_profile_job)
def matrix_profile_messages_sensor(
    rabbit: RabbitResource,
    context: SensorEvaluationContext,
    s3_object_storage: S3ObjectStorageResource,
):
    s3_client = s3_object_storage.get_client()

    context.log.info("Listening for messages in queue: %s", rabbit.matrix_profile_queue)

    for message in rabbit.consume_queue_json_messages(
        queue=rabbit.matrix_profile_queue
    ):
        try:
            matrix_profile_msg = MatrixProfileMessage(**message)
        except ValidationError as ex:
            context.log.warning("Failed to parse message from RabbitMQ: %s", ex)
            continue

        context.log.info("Received Matrix Profile message: %s", matrix_profile_msg)

        presigned_url = s3_client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": matrix_profile_msg.bucket,
                "Key": matrix_profile_msg.key,
            },
            ExpiresIn=_DEFAULT_PRESIGNED_URLS_EXPIRATION_SECS,
        )

        run_key = uuid.uuid4().hex

        conf_kwargs = {
            "file_url": presigned_url,
            "analysis_variable": matrix_profile_msg.analysis_variable,
            "output_bucket": s3_object_storage.job_outputs_bucket_name,
            "workflow_job_id": matrix_profile_msg.workflow_job_id,
        }

        if matrix_profile_msg.image:
            conf_kwargs["image"] = matrix_profile_msg.image
        elif os.getenv(Variables.MATRIX_PROFILE_JOB_IMAGE.value):
            conf_kwargs["image"] = os.getenv(Variables.MATRIX_PROFILE_JOB_IMAGE.value)

        if matrix_profile_msg.tag:
            conf_kwargs["tag"] = matrix_profile_msg.tag
        elif os.getenv(Variables.MATRIX_PROFILE_JOB_TAG.value):
            conf_kwargs["tag"] = os.getenv(Variables.MATRIX_PROFILE_JOB_TAG.value)

        matrix_profile_job_config = MatrixProfileJobConfig(**conf_kwargs)

        context.log.info(
            "Triggering Matrix Profile job (run_key=%s): %s",
            run_key,
            matrix_profile_job_config,
        )

        yield RunRequest(
            run_key=run_key,
            run_config=RunConfig(ops={"run_matrix_profile": matrix_profile_job_config}),
        )
