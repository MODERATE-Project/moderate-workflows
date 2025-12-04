"""Configuration enumerations for MODERATE workflows.

This module defines enumerations used throughout the codebase for:
- Resource names for Dagster dependency injection
- Environment variable names for configuration
- Default values for optional configuration
- State namespace identifiers

Using enums provides type safety, autocomplete support, and a single source
of truth for configuration keys across the application.

Example:
    # Access resource by enum key
    @op
    def my_op(context):
        keycloak = context.resources[ResourceNames.KEYCLOAK.value]

    # Reference environment variables safely
    server_url = os.getenv(Variables.KEYCLOAK_SERVER_URL.value)

    # Use defaults when env var not set
    realm = os.getenv(
        Variables.KEYCLOAK_MAIN_REALM_NAME.value,
        VariableDefaults.KEYCLOAK_MAIN_REALM_NAME.value
    )
"""

import enum


class ResourceNames(enum.Enum):
    KEYCLOAK = "keycloak"
    POSTGRES = "postgres"
    OPEN_METADATA = "open_metadata"
    S3_OBJECT_STORAGE = "s3_object_storage"
    PLATFORM_API = "platform_api"
    RABBIT = "rabbit"


class Variables(enum.Enum):
    KEYCLOAK_SERVER_URL = "KEYCLOAK_SERVER_URL"
    KEYCLOAK_ADMIN_USERNAME = "KEYCLOAK_ADMIN_USERNAME"
    KEYCLOAK_ADMIN_PASSWORD = "KEYCLOAK_ADMIN_PASSWORD"
    KEYCLOAK_MAIN_REALM_NAME = "KEYCLOAK_MAIN_REALM_NAME"
    POSTGRES_HOST = "POSTGRES_HOST"
    POSTGRES_PORT = "POSTGRES_PORT"
    POSTGRES_USERNAME = "POSTGRES_USERNAME"
    POSTGRES_PASSWORD = "POSTGRES_PASSWORD"
    OPEN_METADATA_HOST = "OPEN_METADATA_HOST"
    OPEN_METADATA_PORT = "OPEN_METADATA_PORT"
    OPEN_METADATA_TOKEN = "OPEN_METADATA_TOKEN"
    S3_ACCESS_KEY_ID = "S3_ACCESS_KEY_ID"
    S3_SECRET_ACCESS_KEY = "S3_SECRET_ACCESS_KEY"
    S3_REGION = "S3_REGION"
    S3_BUCKET_NAME = "S3_BUCKET_NAME"
    S3_ENDPOINT_URL = "S3_ENDPOINT_URL"
    S3_JOB_OUTPUTS_BUCKET_NAME = "S3_JOB_OUTPUTS_BUCKET_NAME"
    API_BASE_URL = "API_BASE_URL"
    API_USERNAME = "API_USERNAME"
    API_PASSWORD = "API_PASSWORD"
    RABBIT_URL = "RABBIT_URL"
    RABBIT_MATRIX_PROFILE_QUEUE = "MATRIX_PROFILE_QUEUE"
    MATRIX_PROFILE_JOB_IMAGE = "MATRIX_PROFILE_JOB_IMAGE"
    MATRIX_PROFILE_JOB_TAG = "MATRIX_PROFILE_JOB_TAG"


class VariableDefaults(enum.Enum):
    KEYCLOAK_MAIN_REALM_NAME = "moderate"
    S3_ENDPOINT_URL = "https://storage.googleapis.com"
    API_BASE_URL = "https://api.gw.moderate.cloud"
    RABBIT_MATRIX_PROFILE_QUEUE = "matrix_profile"
    S3_JOB_OUTPUTS_BUCKET_NAME = "moderate-output-bucket"


class StateNamespaces(enum.Enum):
    GIT_DATASET_COMMIT = "git_dataset_commit"
    USER_TRUST_DID = "user_trust_did"
