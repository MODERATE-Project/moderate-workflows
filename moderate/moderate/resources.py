"""Resource definitions for external service integrations.

This module implements Dagster ConfigurableResource classes for integrating with
external services used by the MODERATE workflows:

- KeycloakResource: Identity management and user authentication
- PostgresResource: Database connections and state management
- OpenMetadataResource: Data catalog and metadata ingestion
- S3ObjectStorageResource: S3-compatible object storage client
- PlatformAPIResource: MODERATE platform REST API integration
- RabbitResource: RabbitMQ message queue for async workflows

All resources use environment variable injection via EnvVar for configuration,
ensuring secure credential management and environment-specific settings.

Example:
    Resources are injected into Dagster ops via dependency injection:

    @op
    def my_op(keycloak: KeycloakResource):
        admin = keycloak.get_keycloak_admin()
        users_count = admin.users_count()
"""

import json
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import Lock
from typing import Any, Dict, Generator, List, Optional, Union

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
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30
    pool_recycle: int = 3600

    def postgres_url(self, db_name=None) -> str:
        """Build PostgreSQL connection URL.

        Args:
            db_name: Optional database name to connect to

        Returns:
            PostgreSQL connection URL string
        """
        url = f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}"

        if db_name:
            url += f"/{db_name}"

        return url

    def create_engine(self, db_name=None, **kwargs) -> Engine:
        """Create SQLAlchemy engine with connection pooling configuration.

        Args:
            db_name: Optional database name to connect to
            **kwargs: Additional SQLAlchemy engine arguments (override defaults)

        Returns:
            Configured SQLAlchemy Engine instance

        Note:
            Default pool settings:
            - pool_size: 5 (number of persistent connections)
            - max_overflow: 10 (additional connections when pool exhausted)
            - pool_timeout: 30s (wait time for available connection)
            - pool_recycle: 3600s (recycle connections after 1 hour)
        """
        pool_kwargs = {
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "pool_timeout": self.pool_timeout,
            "pool_recycle": self.pool_recycle,
        }

        pool_kwargs.update(kwargs)

        return create_engine(self.postgres_url(db_name=db_name), **pool_kwargs)

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
    token_ttl_seconds: int = 300

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._cached_token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None
        self._token_lock = Lock()

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
        """Get authentication token with TTL-based caching.

        Returns a cached token if available and not expired, otherwise
        requests a new token from the API. Thread-safe via lock.

        Returns:
            Access token string for API authentication.
        """
        logger = get_dagster_logger()

        with self._token_lock:
            now = datetime.utcnow()

            if self._cached_token and self._token_expiry and self._token_expiry > now:
                logger.debug(
                    "Using cached access token (expires in %s seconds)",
                    (self._token_expiry - now).total_seconds(),
                )
                return self._cached_token

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

            self._cached_token = response.json()["access_token"]
            self._token_expiry = now + timedelta(seconds=self.token_ttl_seconds)

            logger.debug(
                "Received and cached access token from %s (TTL: %s seconds)",
                url,
                self.token_ttl_seconds,
            )

            return self._cached_token

    def get_authorization_header(self) -> dict:
        return {"Authorization": f"Bearer {self.get_token()}"}


@dataclass
class Rabbit:
    connection: pika.BlockingConnection
    channel: BlockingChannel


class RabbitResource(ConfigurableResource):
    rabbit_url: str
    matrix_profile_queue: str
    message_error_strategy: str = "skip"

    @contextmanager
    def with_rabbit(self) -> Generator[Rabbit, None, None]:
        """Context manager for RabbitMQ connection lifecycle.

        Establishes connection and channel, yields them for use, and ensures
        proper cleanup on exit.

        Yields:
            Rabbit dataclass containing connection and channel

        Example:
            with rabbit_resource.with_rabbit() as rabbit:
                rabbit.channel.basic_publish(...)
        """
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
        """Consume JSON messages from a RabbitMQ queue with timeout.

        Connects to the queue and yields decoded JSON messages until either
        the timeout is reached or no messages arrive within inactivity_timeout.

        Args:
            queue: Queue name to consume from
            inactivity_timeout_secs: Max seconds to wait for next message
            timeout_secs: Total max seconds to consume messages

        Yields:
            Dict containing decoded JSON message data

        Note:
            Message error handling strategy (message_error_strategy):
            - "skip" (default): Log warning and continue to next message
            - "raise": Raise exception on decode error
            - "dead_letter": Future support for dead-letter queue

        Example:
            for message in rabbit.consume_queue_json_messages("my_queue"):
                process_message(message)
        """
        logger = get_dagster_logger()

        start_time = time.time()
        messages_processed = 0
        messages_failed = 0

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
                    messages_processed += 1
                    logger.debug("Decoded message: %s", msg_data)
                    yield msg_data

                except json.JSONDecodeError as e:
                    messages_failed += 1
                    logger.warning(
                        "Failed to decode message (attempt %d): %s - Error: %s",
                        messages_failed,
                        body[:100] if body else None,
                        str(e),
                    )

                    if self.message_error_strategy == "raise":
                        raise
                    elif self.message_error_strategy == "dead_letter":
                        logger.warning(
                            "Dead-letter queue support not yet implemented. "
                            "Message will be skipped."
                        )
                    # Default: skip and continue

            elapsed_time = time.time() - start_time
            logger.info(
                "Message consumption complete: %d processed, %d failed, %.1fs elapsed",
                messages_processed,
                messages_failed,
                elapsed_time,
            )
