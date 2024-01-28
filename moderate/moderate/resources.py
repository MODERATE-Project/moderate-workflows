from typing import List, Optional, Union

import requests
import sqlalchemy.exc
from dagster import ConfigurableResource, get_dagster_logger
from keycloak import KeycloakAdmin, KeycloakOpenIDConnection
from slugify import slugify
from sqlalchemy import Column, Integer, String, create_engine, select, text
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import Session, declarative_base

_LOCAL_NAMES = [
    "host.docker.internal",
    "localhost",
]

_DAGSTER_STATE_DBNAME = "moderate_workflows_dagster_state"
_DAGSTER_STATE_TABLE_KEYVALUES = "moderate_workflows_key_values"


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


Base = declarative_base()


class KeyValue(Base):
    __tablename__ = _DAGSTER_STATE_TABLE_KEYVALUES

    id = Column(Integer, primary_key=True)
    key = Column(String, index=True, unique=True)
    value = Column(String)


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
            logger.info("Created database %s", name)
        except sqlalchemy.exc.ProgrammingError:
            logger.debug("Database %s already exists", name)

    def ensure_dagster_state_db(self):
        self.create_database(name=_DAGSTER_STATE_DBNAME)
        engine = self.create_engine(db_name=_DAGSTER_STATE_DBNAME)
        Base.metadata.create_all(bind=engine, checkfirst=True)

    def build_state_key(self, key: str, namespace: Optional[str] = None) -> str:
        return (
            "{}:{}".format(slugify(namespace), slugify(key))
            if namespace
            else slugify(key)
        )

    def get_state(self, key: str, namespace: Optional[str] = None) -> Union[str, None]:
        logger = get_dagster_logger()
        self.ensure_dagster_state_db()
        key_ns = self.build_state_key(key=key, namespace=namespace)
        logger.debug("Reading state key: %s", key_ns)

        with Session(self.create_engine(db_name=_DAGSTER_STATE_DBNAME)) as session:
            stmt = select(KeyValue).where(KeyValue.key == key_ns)
            kv = session.execute(stmt).scalars().one_or_none()
            return kv.value if kv else None

    def set_state(self, key: str, value: str, namespace: Optional[str] = None):
        logger = get_dagster_logger()
        self.ensure_dagster_state_db()
        key_ns = self.build_state_key(key=key, namespace=namespace)
        logger.debug("Writing state key: %s - %s", key_ns, value)

        with Session(self.create_engine(db_name=_DAGSTER_STATE_DBNAME)) as session:
            stmt = select(KeyValue).where(KeyValue.key == key_ns)
            kv = session.execute(stmt).scalars().one_or_none()

            if kv is None:
                kv = KeyValue(key=key_ns, value=value)
            else:
                kv.value = value

            session.add(kv)
            session.commit()


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


class S3ObjectStorageResource(ConfigurableResource):
    access_key_id: str
    secret_access_key: str
    region: str
    bucket_name: str
    endpoint_url: str


class PlatformAPIResource(ConfigurableResource):
    base_url: str
    username: str
    password: str

    def build_url(self, *parts: List[str]) -> str:
        return self.base_url.strip("/") + "/" + "/".join(parts)

    def url_create_asset(self) -> str:
        return self.build_url("asset")

    def url_find_assets(self) -> str:
        return self.url_create_asset()

    def url_read_asset(self, asset_id: str) -> str:
        return self.build_url("asset", str(asset_id))

    def url_upload_asset_object(self, asset_id: str) -> str:
        return self.build_url("asset", str(asset_id), "object")

    def get_token(self) -> str:
        logger = get_dagster_logger()
        url = self.build_url("api", "token")

        data = {
            "username": self.username,
            "password": self.password,
        }

        logger.debug(
            "Requesting access token from %s (username=%s)", url, self.username
        )

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = requests.request("POST", url, headers=headers, data=data)
        response.raise_for_status()
        logger.debug(f"Received access token from {url}")
        return response.json()["access_token"]

    def get_authorization_header(self) -> dict:
        return {"Authorization": f"Bearer {self.get_token()}"}
