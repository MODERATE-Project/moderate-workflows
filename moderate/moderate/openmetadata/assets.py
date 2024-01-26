import pprint

from dagster import Config, asset, define_asset_job, get_dagster_logger

from moderate.openmetadata import run_metadata_workflow, run_profiler_workflow
from moderate.openmetadata.configs.datalake import (
    build_datalake_s3_metadata_config,
    build_datalake_s3_profiler_config,
)
from moderate.openmetadata.configs.postgres import (
    build_postgres_metadata_config,
    build_postgres_profiler_config,
)
from moderate.resources import (
    OpenMetadataResource,
    PostgresResource,
    S3ObjectStorageResource,
)


class PostgresIngestionConfig(Config):
    source_service_name: str = "platform-postgres"


@asset
def postgres_metadata_ingestion(
    config: PostgresIngestionConfig,
    postgres: PostgresResource,
    open_metadata: OpenMetadataResource,
) -> None:
    """Ingests metadata from Postgres into Open Metadata."""

    logger = get_dagster_logger()

    workflow_config = build_postgres_metadata_config(
        source_service_name=config.source_service_name,
        postgres_user=postgres.username,
        postgres_pass=postgres.password,
        postgres_host=postgres.host,
        postgres_port=postgres.port,
        postgres_db=config.default_dbname,
        open_metadata_host_port=open_metadata.host_port,
        open_metadata_token=open_metadata.token,
    )

    logger.debug(
        "Running metadata ingestion workflow:\n%s", pprint.pformat(workflow_config)
    )

    run_metadata_workflow(workflow_config)


@asset(deps=[postgres_metadata_ingestion])
def postgres_profiler_ingestion(
    config: PostgresIngestionConfig,
    open_metadata: OpenMetadataResource,
) -> None:
    """Ingests profiling metadata from Postgres into Open Metadata."""

    logger = get_dagster_logger()

    workflow_config = build_postgres_profiler_config(
        source_service_name=config.source_service_name,
        open_metadata_host_port=open_metadata.host_port,
        open_metadata_token=open_metadata.token,
    )

    logger.debug(
        "Running profiler ingestion workflow:\n%s", pprint.pformat(workflow_config)
    )

    run_profiler_workflow(workflow_config)


postgres_ingestion_job = define_asset_job(
    name="postgres_ingestion_job",
    selection=[
        postgres_metadata_ingestion,
        postgres_profiler_ingestion,
    ],
)


class DatalakeIngestionConfig(Config):
    source_service_name: str = "platform-datalake"


@asset
def datalake_metadata_ingestion(
    config: DatalakeIngestionConfig,
    s3_object_storage: S3ObjectStorageResource,
    open_metadata: OpenMetadataResource,
) -> None:
    """Ingests metadata from an S3-compatible datalake into Open Metadata."""

    logger = get_dagster_logger()

    workflow_config = build_datalake_s3_metadata_config(
        source_service_name=config.source_service_name,
        open_metadata_host_port=open_metadata.host_port,
        open_metadata_token=open_metadata.token,
        bucket_name=s3_object_storage.bucket_name,
        access_key_id=s3_object_storage.access_key_id,
        secret_access_key=s3_object_storage.secret_access_key,
        region=s3_object_storage.region,
        endpoint_url=s3_object_storage.endpoint_url,
    )

    logger.debug(
        "Running metadata ingestion workflow:\n%s", pprint.pformat(workflow_config)
    )

    run_metadata_workflow(workflow_config)


@asset(deps=[datalake_metadata_ingestion])
def datalake_profiler_ingestion(
    config: DatalakeIngestionConfig,
    s3_object_storage: S3ObjectStorageResource,
    open_metadata: OpenMetadataResource,
) -> None:
    """Ingests profiling metadata from an S3-compatible datalake into Open Metadata."""

    logger = get_dagster_logger()

    workflow_config = build_datalake_s3_profiler_config(
        source_service_name=config.source_service_name,
        open_metadata_host_port=open_metadata.host_port,
        open_metadata_token=open_metadata.token,
        bucket_name=s3_object_storage.bucket_name,
        access_key_id=s3_object_storage.access_key_id,
        secret_access_key=s3_object_storage.secret_access_key,
        region=s3_object_storage.region,
        endpoint_url=s3_object_storage.endpoint_url,
    )

    logger.debug(
        "Running profiler ingestion workflow:\n%s", pprint.pformat(workflow_config)
    )

    run_profiler_workflow(workflow_config)


datalake_ingestion_job = define_asset_job(
    name="datalake_ingestion_job",
    selection=[
        datalake_metadata_ingestion,
        datalake_profiler_ingestion,
    ],
)
