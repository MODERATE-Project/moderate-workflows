def build_workflow_config(
    logger_level: str, open_metadata_host_port: str, open_metadata_token: str
) -> dict:
    return {
        "workflowConfig": {
            "loggerLevel": logger_level,
            "openMetadataServerConfig": {
                "hostPort": open_metadata_host_port,
                "authProvider": "openmetadata",
                "securityConfig": {"jwtToken": open_metadata_token},
            },
        },
    }
