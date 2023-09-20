import enum

from dagster import Definitions, EnvVar, load_assets_from_modules

import moderate.assets
import moderate.resources


class ResourceNames(enum.Enum):
    KEYCLOAK = "keycloak"
    POSTGRES = "postgres"


class Variables(enum.Enum):
    KEYCLOAK_SERVER_URL = "KEYCLOAK_SERVER_URL"
    KEYCLOAK_ADMIN_USERNAME = "KEYCLOAK_ADMIN_USERNAME"
    KEYCLOAK_ADMIN_PASSWORD = "KEYCLOAK_ADMIN_PASSWORD"
    POSTGRES_HOST = "POSTGRES_HOST"
    POSTGRES_PORT = "POSTGRES_PORT"
    POSTGRES_USERNAME = "POSTGRES_USERNAME"
    POSTGRES_PASSWORD = "POSTGRES_PASSWORD"


all_assets = load_assets_from_modules([moderate.assets])

defs = Definitions(
    assets=all_assets,
    resources={
        ResourceNames.KEYCLOAK.value: moderate.resources.KeycloakResource(
            server_url=EnvVar(Variables.KEYCLOAK_SERVER_URL.value),
            admin_username=EnvVar(Variables.KEYCLOAK_ADMIN_USERNAME.value),
            admin_password=EnvVar(Variables.KEYCLOAK_ADMIN_PASSWORD.value),
        ),
        ResourceNames.POSTGRES.value: moderate.resources.PostgresResource(
            host=EnvVar(Variables.POSTGRES_HOST.value),
            port=EnvVar.int(Variables.POSTGRES_PORT.value),
            username=EnvVar(Variables.POSTGRES_USERNAME.value),
            password=EnvVar(Variables.POSTGRES_PASSWORD.value),
        ),
    },
)
