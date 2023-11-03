import os

from dagster import Definitions, EnvVar, load_assets_from_modules

import moderate.assets
import moderate.openmetadata.assets
import moderate.resources
import moderate.trust
from moderate.enums import ResourceNames, VariableDefaults, Variables

all_assets = load_assets_from_modules(
    [moderate.assets, moderate.openmetadata.assets, moderate.trust]
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
    },
    jobs=[
        moderate.openmetadata.assets.postgres_ingestion_job,
        moderate.trust.call_trust_services_new_user_job,
        moderate.trust.call_trust_services_new_asset_job,
        moderate.trust.verify_asset_trust_services_job,
    ],
    sensors=[moderate.trust.keycloak_user_sensor, moderate.trust.data_asset_sensor],
)
