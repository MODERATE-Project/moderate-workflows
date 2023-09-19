from dagster import ConfigurableResource, get_dagster_logger
from keycloak import KeycloakAdmin, KeycloakOpenIDConnection


class KeycloakResource(ConfigurableResource):
    server_url: str
    admin_username: str
    admin_password: str

    @property
    def keycloak_connection(self) -> KeycloakOpenIDConnection:
        logger = get_dagster_logger()

        keycloak_kwargs = {
            "server_url": self.server_url,
            "username": self.admin_username,
            "password": self.admin_password,
            "verify": True,
        }

        logger.debug(f"Connecting to Keycloak with {keycloak_kwargs}")

        return KeycloakOpenIDConnection(**keycloak_kwargs)

    @property
    def keycloak_admin(self) -> KeycloakAdmin:
        return KeycloakAdmin(connection=self.keycloak_connection)

    def get_users_count(self) -> int:
        return self.keycloak_admin.users_count()
