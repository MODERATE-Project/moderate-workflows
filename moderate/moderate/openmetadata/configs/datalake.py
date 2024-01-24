"""
https://docs.open-metadata.org/v1.2.x/connectors/database/datalake/yaml
"""


from moderate.openmetadata.configs.common import build_workflow_config


def build_datalake_s3_metadata_config(
    source_service_name: str,
    open_metadata_host_port: str,
    open_metadata_token: str,
    bucket_name: str,
    prefix: str,
    mark_deleted_tables: bool = True,
    include_tables: bool = True,
    include_views: bool = True,
    logger_level: str = "INFO",
    pipeline_name: str = "metadata",
) -> dict:
    data = {
        "source": {
            "type": "datalake",
            "serviceName": source_service_name,
            "serviceConnection": {
                "config": {
                    "type": "Datalake",
                    "configSource": {
                        "securityConfig": {
                            "awsAccessKeyId": None,
                            "awsSecretAccessKey": None,
                            "awsRegion": None,
                            "endPointURL": None,
                        }
                    },
                    "bucketName": bucket_name,
                    "prefix": prefix,
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
        "ingestionPipelineFQN": f"{source_service_name}.{pipeline_name}",
    }

    workflow_config = build_workflow_config(
        logger_level=logger_level,
        open_metadata_host_port=open_metadata_host_port,
        open_metadata_token=open_metadata_token,
    )

    data.update(workflow_config)

    return data
