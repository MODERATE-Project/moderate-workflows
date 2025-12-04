import pprint
import re
import uuid
from typing import List

from dagster import (
    Config,
    DynamicOut,
    DynamicOutput,
    ScheduleDefinition,
    get_dagster_logger,
    job,
    op,
)
from slugify import slugify
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

_MAX_CONCURRENT_POSTGRES = 2
_MAX_CONCURRENT_DATALAKE = 2


def get_mapping_key(val: str) -> str:
    key = uuid.uuid5(uuid.NAMESPACE_URL, val).hex
    key_prefix = slugify(val).replace("-", "_")
    key = f"{key_prefix}_{key}"
    return key


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

        return [
            DynamicOutput(dbname, mapping_key=get_mapping_key(dbname))
            for dbname in dbnames
        ]


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
                "multiprocess": {"max_concurrent": _MAX_CONCURRENT_POSTGRES},
            }
        }
    }
)
def postgres_ingestion_job():
    dbnames = find_postgres_databases()
    dbnames.map(postgres_metadata_ingestion).map(postgres_profiler_ingestion).collect()


postgres_ingestion_schedule = ScheduleDefinition(
    job=postgres_ingestion_job,
    cron_schedule="0 * * * *",
)


class DatalakeIngestionConfig(Config):
    source_service_name: str = "platform-datalake"


@op(out=DynamicOut())
def find_datalake_prefixes(
    s3_object_storage: S3ObjectStorageResource,
) -> List[DynamicOutput[str]]:
    logger = get_dagster_logger()
    s3_client = s3_object_storage.get_client()
    paginator = s3_client.get_paginator("list_objects_v2")
    prefixes = []

    for result in paginator.paginate(
        Bucket=s3_object_storage.bucket_name, Delimiter="/"
    ):
        for prefix in result.get("CommonPrefixes", []):
            val = prefix.get("Prefix")
            prefixes.append(DynamicOutput(value=val, mapping_key=get_mapping_key(val)))

    logger.info(
        "Found %s prefixes in the bucket %s",
        len(prefixes),
        s3_object_storage.bucket_name,
    )

    return prefixes


@op
def datalake_metadata_ingestion(
    config: DatalakeIngestionConfig,
    s3_object_storage: S3ObjectStorageResource,
    open_metadata: OpenMetadataResource,
    prefix: str,
) -> str:
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
        objects_base_prefix=prefix,
    )

    logger.debug(
        "Running metadata ingestion workflow:\n%s", pprint.pformat(workflow_config)
    )

    run_metadata_workflow(workflow_config)

    return prefix


@op
def datalake_profiler_ingestion(
    config: DatalakeIngestionConfig,
    s3_object_storage: S3ObjectStorageResource,
    open_metadata: OpenMetadataResource,
    prefix: str,
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
        objects_base_prefix=prefix,
    )

    logger.debug(
        "Running profiler ingestion workflow:\n%s", pprint.pformat(workflow_config)
    )

    run_profiler_workflow(workflow_config, log_instead_of_raise=False)


@job(
    config={
        "execution": {
            "config": {
                "multiprocess": {"max_concurrent": _MAX_CONCURRENT_DATALAKE},
            }
        }
    }
)
def datalake_ingestion_job():
    prefixes = find_datalake_prefixes()
    prefixes.map(datalake_metadata_ingestion).map(datalake_profiler_ingestion).collect()


datalake_ingestion_schedule = ScheduleDefinition(
    job=datalake_ingestion_job,
    cron_schedule="0 * * * *",
)
