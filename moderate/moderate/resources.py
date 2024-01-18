from typing import Union

import sqlalchemy.exc
from dagster import ConfigurableResource, get_dagster_logger
from keycloak import KeycloakAdmin, KeycloakOpenIDConnection
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine

_LOCAL_NAMES = [
    "host.docker.internal",
    "localhost",
]


class KeycloakResource(ConfigurableResource):
    server_url: str
    admin_username: str
    admin_password: str
    main_realm_name: str

    def get_keycloak_connection(self, **kwargs) -> KeycloakOpenIDConnection:
        logger = get_dagster_logger()

        keycloak_kwargs = {
            "server_url": self.server_url,
            "username": self.admin_username,
            "password": self.admin_password,
            "verify": True,
            "realm_name": self.main_realm_name,
            "user_realm_name": "master",
        }

        keycloak_kwargs.update(kwargs)
        logger.debug(f"Connecting to Keycloak with {keycloak_kwargs}")

        return KeycloakOpenIDConnection(**keycloak_kwargs)

    def get_keycloak_admin(self, *args, **kwargs) -> KeycloakAdmin:
        return KeycloakAdmin(connection=self.get_keycloak_connection(*args, **kwargs))

    def get_users_count(self) -> int:
        return self.get_keycloak_admin().users_count()


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

    @property
    def host_port(self, default_https: bool = True) -> str:
        logger = get_dagster_logger()

        if self.host.startswith("http://") or self.host.startswith("https://"):
            scheme_and_host = self.host
        elif self.host in _LOCAL_NAMES:
            logger.info("Host seems to be local: using http:// and ignoring kwarg")
            scheme_and_host = f"http://{self.host}"
        else:
            scheme_and_host = "{}://{}".format(
                "https" if default_https else "http", self.host
            )

        return "{}:{}/api".format(scheme_and_host, self.port)
