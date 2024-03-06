import pprint
import re
from typing import List

from dagster import (
    Config,
    DynamicOut,
    DynamicOutput,
    asset,
    define_asset_job,
    get_dagster_logger,
    job,
    op,
)
from sqlalchemy import text

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
    ignored_dbnames: List[str] = ["template.*", "postgres"]


@op(out=DynamicOut())
def find_postgres_databases(
    config: PostgresIngestionConfig, postgres: PostgresResource
) -> List[DynamicOutput[str]]:
    """Finds all Postgres databases and filters out ignored ones."""

    logger = get_dagster_logger()

    with postgres.create_engine().connect() as conn:
        result = conn.execute(text("SELECT datname FROM pg_database;"))
        dbnames = [row[0] for row in result]

        logger.debug("Found Postgres databases: %s", dbnames)

        dbnames = [
            dbname
            for dbname in dbnames
            if not any(re.match(pattern, dbname) for pattern in config.ignored_dbnames)
        ]

        logger.debug("Filtered Postgres databases: %s", dbnames)
        return [DynamicOutput(dbname, mapping_key=dbname) for dbname in dbnames]


@op
def postgres_metadata_ingestion(
    config: PostgresIngestionConfig,
    postgres: PostgresResource,
    open_metadata: OpenMetadataResource,
    dbname: str,
) -> str:
    """Ingests metadata from Postgres into Open Metadata."""

    logger = get_dagster_logger()

    workflow_config = build_postgres_metadata_config(
        source_service_name=config.source_service_name,
        postgres_user=postgres.username,
        postgres_pass=postgres.password,
        postgres_host=postgres.host,
        postgres_port=postgres.port,
        postgres_db=dbname,
        open_metadata_host_port=open_metadata.host_port,
        open_metadata_token=open_metadata.token,
        ingest_all_databases=False,
    )

    logger.debug(
        "Running metadata ingestion workflow:\n%s", pprint.pformat(workflow_config)
    )

    run_metadata_workflow(workflow_config)

    return dbname


@op
def postgres_profiler_ingestion(
    config: PostgresIngestionConfig,
    open_metadata: OpenMetadataResource,
    dbname: str,
) -> None:
    """Ingests profiling metadata from Postgres into Open Metadata."""

    logger = get_dagster_logger()

    workflow_config = build_postgres_profiler_config(
        source_service_name=config.source_service_name,
        postgres_db=dbname,
        open_metadata_host_port=open_metadata.host_port,
        open_metadata_token=open_metadata.token,
    )

    logger.debug(
        "Running profiler ingestion workflow:\n%s", pprint.pformat(workflow_config)
    )

    run_profiler_workflow(workflow_config)


@job(
    config={
        "execution": {
            "config": {
                "multiprocess": {"max_concurrent": 1},
            }
        }
    }
)
def postgres_ingestion_job():
    dbnames = find_postgres_databases()
    dbnames.map(postgres_metadata_ingestion).map(postgres_profiler_ingestion).collect()


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

    # We don't have too much control over the data in the datalake, so we don't want to
    # fail the whole workflow if the profiler fails. Instead, we'll just log the error.
    run_profiler_workflow(workflow_config, log_instead_of_raise=True)


datalake_ingestion_job = define_asset_job(
    name="datalake_ingestion_job",
    selection=[
        datalake_metadata_ingestion,
        datalake_profiler_ingestion,
    ],
)
