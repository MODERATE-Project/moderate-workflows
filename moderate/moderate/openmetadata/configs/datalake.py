"""
https://docs.open-metadata.org/v1.2.x/connectors/database/datalake/yaml
"""

import hashlib
from typing import Optional

from slugify import slugify

from moderate.openmetadata.configs.common import build_workflow_config


def get_datalake_service_name(base_service_name: str, objects_base_prefix: str) -> str:
    sha256_hash = hashlib.sha256()
    sha256_hash.update(objects_base_prefix.encode("utf-8"))
    unique_key = sha256_hash.hexdigest()
    human_name = slugify(f"{base_service_name}-{objects_base_prefix}")
    return f"{human_name}-{unique_key}"


def _build_service_connection_config(
    bucket_name: str,
    access_key_id: str,
    secret_access_key: str,
    region: str,
    endpoint_url: str,
    prefix: Optional[str] = None,
) -> dict:
    ret = {
        "type": "Datalake",
        "configSource": {
            "securityConfig": {
                "awsAccessKeyId": access_key_id,
                "awsSecretAccessKey": secret_access_key,
                "awsRegion": region,
                "endPointURL": endpoint_url,
            }
        },
        "bucketName": bucket_name,
    }

    if prefix:
        ret.update({"prefix": prefix})

    return ret


def build_datalake_s3_metadata_config(
    source_service_name: str,
    open_metadata_host_port: str,
    open_metadata_token: str,
    bucket_name: str,
    access_key_id: str,
    secret_access_key: str,
    region: str,
    endpoint_url: str,
    objects_base_prefix: str,
    prefix: Optional[str] = None,
    mark_deleted_tables: bool = True,
    include_tables: bool = True,
    include_views: bool = True,
    logger_level: str = "INFO",
    pipeline_name: str = "metadata",
) -> dict:
    service_name = get_datalake_service_name(source_service_name, objects_base_prefix)

    service_connection_config = _build_service_connection_config(
        bucket_name=bucket_name,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        region=region,
        endpoint_url=endpoint_url,
        prefix=prefix,
    )

    objects_base_prefix = objects_base_prefix.rstrip("/")

    data = {
        "source": {
            "type": "datalake",
            "serviceName": service_name,
            "serviceConnection": {"config": service_connection_config},
            "sourceConfig": {
                "config": {
                    "type": "DatabaseMetadata",
                    "markDeletedTables": mark_deleted_tables,
                    "includeTables": include_tables,
                    "includeViews": include_views,
                    "useFqnForFiltering": True,
                    "tableFilterPattern": {
                        "includes": [f".*{objects_base_prefix}/.*$"]
                    },
                }
            },
        },
        "sink": {
            "type": "metadata-rest",
            "config": {},
        },
        "ingestionPipelineFQN": f"{service_name}.{pipeline_name}",
    }

    workflow_config = build_workflow_config(
        logger_level=logger_level,
        open_metadata_host_port=open_metadata_host_port,
        open_metadata_token=open_metadata_token,
    )

    data.update(workflow_config)

    return data


def build_datalake_s3_profiler_config(
    source_service_name: str,
    open_metadata_host_port: str,
    open_metadata_token: str,
    bucket_name: str,
    access_key_id: str,
    secret_access_key: str,
    region: str,
    endpoint_url: str,
    objects_base_prefix: str,
    prefix: Optional[str] = None,
    include_views: bool = True,
    logger_level: str = "INFO",
    pipeline_name: str = "metadata",
    generate_sample_data: bool = True,
    process_pii_sensitive: bool = False,
    profile_sample: int = 5000,
    profile_sample_type: str = "ROWS",
) -> dict:
    service_name = get_datalake_service_name(source_service_name, objects_base_prefix)

    service_connection_config = _build_service_connection_config(
        bucket_name=bucket_name,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        region=region,
        endpoint_url=endpoint_url,
        prefix=prefix,
    )

    data = {
        "source": {
            "type": "datalake",
            "serviceName": service_name,
            "serviceConnection": {"config": service_connection_config},
            "sourceConfig": {
                "config": {
                    "type": "Profiler",
                    "includeViews": include_views,
                    "generateSampleData": generate_sample_data,
                    "processPiiSensitive": process_pii_sensitive,
                    "profileSample": profile_sample,
                    "profileSampleType": profile_sample_type,
                }
            },
        },
        "processor": {
            "type": "orm-profiler",
            "config": {},
        },
        "sink": {
            "type": "metadata-rest",
            "config": {},
        },
        "ingestionPipelineFQN": f"{service_name}.{pipeline_name}",
    }

    workflow_config = build_workflow_config(
        logger_level=logger_level,
        open_metadata_host_port=open_metadata_host_port,
        open_metadata_token=open_metadata_token,
    )

    data.update(workflow_config)

    return data
