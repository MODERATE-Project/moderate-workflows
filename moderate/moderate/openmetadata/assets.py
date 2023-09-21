import pprint

from dagster import AssetExecutionContext, Config, asset, get_dagster_logger

from moderate.openmetadata import run_profiler_workflow, run_workflow
from moderate.openmetadata.configs.postgres import (
    build_database_metadata_config,
    build_profiler_config,
)
from moderate.resources import OpenMetadataResource, PostgresResource


class PostgresIngestionConfig(Config):
    source_service_name: str = "platform-postgres"
    # ToDo: Fetch this dynamically from the PostgresResource
    default_dbname: str = "building_stock_analysis"


@asset
def postgres_metadata_ingestion(
    context: AssetExecutionContext,
    config: PostgresIngestionConfig,
    postgres: PostgresResource,
    open_metadata: OpenMetadataResource,
) -> None:
    """Ingests metadata from Postgres into Open Metadata."""

    # ToDo: Add some output metadata to this asset
    logger = get_dagster_logger()

    config = build_database_metadata_config(
        source_service_name=config.source_service_name,
        postgres_user=postgres.username,
        postgres_pass=postgres.password,
        postgres_host=postgres.host,
        postgres_port=postgres.port,
        postgres_db=config.default_dbname,
        open_metadata_host_port=open_metadata.host_port,
        open_metadata_token=open_metadata.token,
    )

    logger.debug("Running metadata ingestion workflow:\n%s", pprint.pformat(config))
    run_workflow(config)


@asset(deps=[postgres_metadata_ingestion])
def postgres_profiler_ingestion(
    context: AssetExecutionContext,
    config: PostgresIngestionConfig,
    open_metadata: OpenMetadataResource,
) -> None:
    """Ingests profiling metadata from Postgres into Open Metadata."""

    # ToDo: Add some output metadata to this asset
    logger = get_dagster_logger()

    config = build_profiler_config(
        source_service_name=config.source_service_name,
        open_metadata_host_port=open_metadata.host_port,
        open_metadata_token=open_metadata.token,
    )

    logger.debug("Running profiler ingestion workflow:\n%s", pprint.pformat(config))
    run_profiler_workflow(config)
