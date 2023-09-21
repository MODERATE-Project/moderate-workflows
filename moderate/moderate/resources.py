import sqlalchemy.exc
from dagster import ConfigurableResource, get_dagster_logger
from keycloak import KeycloakAdmin, KeycloakOpenIDConnection
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine


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


class PostgresResource(ConfigurableResource):
    host: str
    port: int
    username: str
    password: str

    def postgres_url(self, db_name=None) -> str:
        url = f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}"

        if db_name:
            url += f"/{db_name}"

        return url

    def create_engine(self, db_name=None, **kwargs) -> Engine:
        return create_engine(self.postgres_url(db_name=db_name), **kwargs)

    def create_database(self, name: str):
        logger = get_dagster_logger()

        try:
            with self.create_engine(isolation_level="AUTOCOMMIT").connect() as conn:
                conn.execute(text(f"CREATE DATABASE {name}"))
        except sqlalchemy.exc.ProgrammingError:
            logger.info("Database %s already exists", name)


class OpenMetadataResource(ConfigurableResource):
    host: str
    port: int
    token: str
    use_ssl: bool = False

    @property
    def host_port(self) -> str:
        return "{}://{}:{}/api".format(
            "https" if self.use_ssl else "http",
            self.host,
            self.port,
        )
