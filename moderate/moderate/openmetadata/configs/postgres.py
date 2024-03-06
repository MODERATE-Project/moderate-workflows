"""
https://docs.open-metadata.org/v1.2.x/connectors/database/postgres/yaml
"""

from slugify import slugify

from moderate.openmetadata.configs.common import build_workflow_config


def get_postgres_service_name(base_service_name: str, dbname: str) -> str:
    return slugify(f"{base_service_name}-{dbname}")


def build_postgres_metadata_config(
    source_service_name: str,
    postgres_user: str,
    postgres_pass: str,
    postgres_host: str,
    postgres_port: int,
    postgres_db: str,
    open_metadata_host_port: str,
    open_metadata_token: str,
    ingest_all_databases: bool = True,
    mark_deleted_tables: bool = True,
    include_tables: bool = True,
    include_views: bool = True,
    logger_level: str = "INFO",
    pipeline_name: str = "metadata",
) -> dict:
    service_name = get_postgres_service_name(source_service_name, postgres_db)

    data = {
        "source": {
            "type": "postgres",
            "serviceName": service_name,
            "serviceConnection": {
                "config": {
                    "type": "Postgres",
                    "username": postgres_user,
                    "authType": {"password": postgres_pass},
                    "hostPort": f"{postgres_host}:{postgres_port}",
                    "database": postgres_db,
                    "ingestAllDatabases": ingest_all_databases,
                }
            },
            "sourceConfig": {
                "config": {
                    "type": "DatabaseMetadata",
                    "markDeletedTables": mark_deleted_tables,
                    "includeTables": include_tables,
                    "includeViews": include_views,
                }
            },
        },
        "sink": {"type": "metadata-rest", "config": {}},
        "ingestionPipelineFQN": f"{service_name}.{pipeline_name}",
    }

    workflow_config = build_workflow_config(
        logger_level=logger_level,
        open_metadata_host_port=open_metadata_host_port,
        open_metadata_token=open_metadata_token,
    )

    data.update(workflow_config)

    return data


def build_postgres_profiler_config(
    source_service_name: str,
    postgres_db: str,
    open_metadata_host_port: str,
    open_metadata_token: str,
    logger_level: str = "INFO",
    generate_sample_data: bool = True,
    process_pii_sensitive: bool = False,
    pipeline_name: str = "profiler",
) -> dict:
    service_name = get_postgres_service_name(source_service_name, postgres_db)

    data = {
        "source": {
            "type": "postgres",
            "serviceName": service_name,
            "sourceConfig": {
                "config": {
                    "type": "Profiler",
                    "generateSampleData": generate_sample_data,
                    "processPiiSensitive": process_pii_sensitive,
                }
            },
        },
        "processor": {"type": "orm-profiler", "config": {}},
        "sink": {"type": "metadata-rest", "config": {}},
        "ingestionPipelineFQN": f"{service_name}.{pipeline_name}",
    }

    workflow_config = build_workflow_config(
        logger_level=logger_level,
        open_metadata_host_port=open_metadata_host_port,
        open_metadata_token=open_metadata_token,
    )

    data.update(workflow_config)

    return data
