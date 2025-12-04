import os

from dagster import Definitions, EnvVar, load_assets_from_modules

import moderate.assets
import moderate.datasets
import moderate.matrix_profile
import moderate.openmetadata.assets
import moderate.resources
import moderate.trust
from moderate.enums import ResourceNames, VariableDefaults, Variables

# Optional: Validate configuration at module load time
# Uncomment to enable fail-fast validation
# from moderate.validation import validate_resources
# validate_resources(raise_on_error=True)

all_assets = load_assets_from_modules(
    [
        moderate.assets,
        moderate.openmetadata.assets,
        moderate.trust,
        moderate.datasets,
    ]
)

defs = Definitions(
    assets=all_assets,
    resources={
        ResourceNames.KEYCLOAK.value: moderate.resources.KeycloakResource(
            server_url=EnvVar(Variables.KEYCLOAK_SERVER_URL.value),
            admin_username=EnvVar(Variables.KEYCLOAK_ADMIN_USERNAME.value),
            admin_password=EnvVar(Variables.KEYCLOAK_ADMIN_PASSWORD.value),
            main_realm_name=os.getenv(
                Variables.KEYCLOAK_MAIN_REALM_NAME.value,
                VariableDefaults.KEYCLOAK_MAIN_REALM_NAME.value,
            ),
        ),
        ResourceNames.POSTGRES.value: moderate.resources.PostgresResource(
            host=EnvVar(Variables.POSTGRES_HOST.value),
            port=EnvVar.int(Variables.POSTGRES_PORT.value),
            username=EnvVar(Variables.POSTGRES_USERNAME.value),
            password=EnvVar(Variables.POSTGRES_PASSWORD.value),
        ),
        ResourceNames.OPEN_METADATA.value: moderate.resources.OpenMetadataResource(
            host=EnvVar(Variables.OPEN_METADATA_HOST.value),
            port=EnvVar.int(Variables.OPEN_METADATA_PORT.value),
            token=EnvVar(Variables.OPEN_METADATA_TOKEN.value),
        ),
        ResourceNames.S3_OBJECT_STORAGE.value: moderate.resources.S3ObjectStorageResource(
            access_key_id=EnvVar(Variables.S3_ACCESS_KEY_ID.value),
            secret_access_key=EnvVar(Variables.S3_SECRET_ACCESS_KEY.value),
            region=EnvVar(Variables.S3_REGION.value),
            bucket_name=EnvVar(Variables.S3_BUCKET_NAME.value),
            endpoint_url=os.getenv(
                Variables.S3_ENDPOINT_URL.value, VariableDefaults.S3_ENDPOINT_URL.value
            ),
            job_outputs_bucket_name=os.getenv(
                Variables.S3_JOB_OUTPUTS_BUCKET_NAME.value,
                VariableDefaults.S3_JOB_OUTPUTS_BUCKET_NAME.value,
            ),
        ),
        ResourceNames.PLATFORM_API.value: moderate.resources.PlatformAPIResource(
            base_url=EnvVar(Variables.API_BASE_URL.value),
            username=EnvVar(Variables.API_USERNAME.value),
            password=EnvVar(Variables.API_PASSWORD.value),
        ),
        ResourceNames.RABBIT.value: moderate.resources.RabbitResource(
            rabbit_url=EnvVar(Variables.RABBIT_URL.value),
            matrix_profile_queue=os.getenv(
                Variables.RABBIT_MATRIX_PROFILE_QUEUE.value,
                VariableDefaults.RABBIT_MATRIX_PROFILE_QUEUE.value,
            ),
        ),
    },
    jobs=[
        moderate.openmetadata.assets.postgres_ingestion_job,
        moderate.openmetadata.assets.datalake_ingestion_job,
        moderate.trust.propagate_new_user_to_trust_services_job,
        moderate.trust.create_asset_object_proof_job,
        moderate.matrix_profile.matrix_profile_job,
    ],
    sensors=[
        moderate.trust.keycloak_user_sensor,
        moderate.trust.platform_api_asset_object_sensor,
        moderate.matrix_profile.matrix_profile_messages_sensor,
    ],
)
