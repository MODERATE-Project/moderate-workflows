import json
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Generator, List, Union

import boto3
import pika
import requests
import sqlalchemy.exc
from dagster import ConfigurableResource, get_dagster_logger
from keycloak import KeycloakAdmin, KeycloakOpenIDConnection
from pika.adapters.blocking_connection import BlockingChannel
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

        safe_log_kwargs = {**keycloak_kwargs}
        safe_log_kwargs.pop("password")
        logger.debug("Creating Keycloak connection with kwargs: %s", safe_log_kwargs)

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
            logger.info("Created database %s", name)
        except sqlalchemy.exc.ProgrammingError:
            logger.debug("Database %s already exists", name)


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
    job_outputs_bucket_name: str

    def get_client(self) -> Any:
        s3_client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            region_name=self.region,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
        )

        return s3_client


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

    def url_find_asset_objects(self) -> str:
        return self.build_url("asset", "object")

    def url_read_asset(self, asset_id: str) -> str:
        return self.build_url("asset", str(asset_id))

    def url_upload_asset_object(self, asset_id: str) -> str:
        return self.build_url("asset", str(asset_id), "object")

    def url_ensure_user_trust_did(self) -> str:
        return self.build_url("user", "did")

    def url_check_did_task(self, task_id: Union[str, int]) -> str:
        return self.build_url("user", "did", "task", str(task_id))

    def url_ensure_trust_proof(self) -> str:
        return self.build_url("asset", "proof")

    def url_check_proof_task(self, task_id: Union[str, int]) -> str:
        return self.build_url("asset", "proof", "task", str(task_id))

    def url_patch_job(self, job_id: int) -> str:
        return self.build_url("job", str(job_id))

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


@dataclass
class Rabbit:
    connection: pika.BlockingConnection
    channel: BlockingChannel


class RabbitResource(ConfigurableResource):
    rabbit_url: str
    matrix_profile_queue: str

    @contextmanager
    def with_rabbit(self) -> Generator[Rabbit, None, None]:
        logger = get_dagster_logger()

        logger.debug("Connecting to RabbitMQ")
        parameters = pika.URLParameters(url=self.rabbit_url)
        connection = pika.BlockingConnection(parameters=parameters)
        channel = connection.channel()

        yield Rabbit(connection=connection, channel=channel)

        logger.debug("Closing RabbitMQ connection")
        channel.cancel()
        channel.close()
        connection.close()

    def consume_queue_json_messages(
        self, queue: str, inactivity_timeout_secs: int = 5, timeout_secs: int = 30
    ) -> Generator[Dict, None, None]:
        logger = get_dagster_logger()

        start_time = time.time()

        with self.with_rabbit() as rabbit:
            logger.debug("Consuming messages from queue: %s", queue)
            rabbit.channel.queue_declare(queue=queue, passive=True)

            for method_frame, properties, body in rabbit.channel.consume(
                queue=queue, auto_ack=True, inactivity_timeout=inactivity_timeout_secs
            ):
                elapsed_time = time.time() - start_time

                if elapsed_time >= timeout_secs:
                    logger.debug("Timeout reached: %s seconds", timeout_secs)
                    break

                if None in [method_frame, properties, body]:
                    continue

                logger.debug(
                    "Received message (method_frame=%s) (properties=%s)",
                    method_frame,
                    properties,
                )

                try:
                    msg_data = json.loads(body.decode())
                except json.JSONDecodeError:
                    logger.warning("Failed to decode message: %s", body)
                    continue

                logger.debug("Decoded message: %s", msg_data)

                yield msg_data
