import dataclasses
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
from dagster_k8s import execute_k8s_job
from pydantic import BaseModel, ValidationError

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
    workflow_job_id: int
    error: Union[str, None] = None
    output_bucket: Union[str, None] = None
    output_key: Union[str, None] = None


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

    env_vars = {
        "S3_ACCESS_KEY_ID": s3_object_storage.access_key_id,
        "S3_SECRET_ACCESS_KEY": s3_object_storage.secret_access_key,
        "OUTPUT_KEY": output_key,
        "OUTPUT_BUCKET": config.output_bucket,
        "FILE_URL": config.file_url,
        "ANALYSIS_VARIABLE": config.analysis_variable,
    }

    env_vars = ["{}={}".format(k, v) for k, v in env_vars.items()]

    try:
        execute_k8s_job(
            context=context,
            image=image,
            image_pull_policy=config.image_pull_policy,
            timeout=config.timeout_secs,
            env_vars=env_vars,
        )
    except Exception as ex:
        context.log.error("Failed to run Matrix Profile job: %s", ex)

        return MatrixProfileJobResult(
            workflow_job_id=config.workflow_job_id, error=str(ex)
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
