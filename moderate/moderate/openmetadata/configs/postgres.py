def build_database_metadata_config(
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
) -> dict:
    data = {
        "source": {
            "type": "postgres",
            "serviceName": source_service_name,
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
        "workflowConfig": {
            "loggerLevel": logger_level,
            "openMetadataServerConfig": {
                "hostPort": open_metadata_host_port,
                "authProvider": "openmetadata",
                "securityConfig": {"jwtToken": open_metadata_token},
            },
        },
    }

    return data


def build_profiler_config(
    source_service_name: str,
    open_metadata_host_port: str,
    open_metadata_token: str,
    logger_level: str = "INFO",
    generate_sample_data: bool = True,
    process_pii_sensitive: bool = False,
) -> dict:
    data = {
        "source": {
            "type": "postgres",
            "serviceName": source_service_name,
            "sourceConfig": {
                "config": {
                    "type": "Profiler",
                    "generateSampleData": generate_sample_data,
                    "processPiiSensitive": process_pii_sensitive,
                    # ToDo: Add optional parameters
                    # "profileSample": 85,
                    # "threadCount": 5,
                    # "confidence": 80,
                    # "timeoutSeconds": 43200,
                    # "databaseFilterPattern": {
                    #     "includes": ["database1", "database2"],
                    #     "excludes": ["database3", "database4"]
                    # },
                    # "schemaFilterPattern": {
                    #     "includes": ["schema1", "schema2"],
                    #     "excludes": ["schema3", "schema4"]
                    # },
                    # "tableFilterPattern": {
                    #     "includes": ["table1", "table2"],
                    #     "excludes": ["table3", "table4"]
                    # }
                }
            },
        },
        "processor": {"type": "orm-profiler", "config": {}},
        "sink": {"type": "metadata-rest", "config": {}},
        "workflowConfig": {
            "loggerLevel": logger_level,
            "openMetadataServerConfig": {
                "hostPort": open_metadata_host_port,
                "authProvider": "openmetadata",
                "securityConfig": {"jwtToken": open_metadata_token},
            },
        },
    }

    return data
