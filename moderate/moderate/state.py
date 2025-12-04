"""PostgreSQL-backed state management for Dagster workflows.

This module provides persistent key-value storage for workflow state tracking,
enabling idempotent operations and preventing duplicate processing across
sensor runs and job executions.

The PostgresState class creates a dedicated database for state storage with
namespace support, automatic key slugification, and batch operations.

Key Features:
- Persistent state across workflow runs
- Namespace isolation for different workflows
- Automatic database and table creation
- Thread-safe operations via SQLAlchemy sessions
- Batch read operations for efficiency

Example:
    state = PostgresState(postgres_resource)

    # Store state with namespace
    state.set_state("user_123", "processed", namespace="keycloak_users")

    # Retrieve state
    value = state.get_state("user_123", namespace="keycloak_users")

    # Batch retrieval
    values = state.get_batch(["user_1", "user_2"], namespace="keycloak_users")
"""

from typing import Dict, List, Union

from dagster import Optional, get_dagster_logger
from slugify import slugify
from sqlalchemy import Column, Integer, String, select
from sqlalchemy.orm import Session, declarative_base

from moderate.resources import PostgresResource


class StateConfig:
    """Configuration constants for PostgreSQL state storage.

    Centralizes database and table naming configuration for the state
    management system. These values can be customized for different
    environments or testing scenarios.
    """

    DATABASE_NAME = "moderate_workflows_dagster_state"
    TABLE_KEYVALUES = "moderate_workflows_key_values"


_DAGSTER_STATE_DBNAME = StateConfig.DATABASE_NAME
_DAGSTER_STATE_TABLE_KEYVALUES = StateConfig.TABLE_KEYVALUES

Base = declarative_base()


class KeyValue(Base):
    __tablename__ = _DAGSTER_STATE_TABLE_KEYVALUES

    id = Column(Integer, primary_key=True)
    key = Column(String, index=True, unique=True)
    value = Column(String)


class PostgresState:
    def __init__(self, postgres: PostgresResource) -> None:
        self.postgres = postgres
        self.logger = get_dagster_logger()
        self.created_db: bool = False

    @classmethod
    def build_key(
        cls, key: str, namespace: Optional[str] = None, slug_key: bool = True
    ) -> str:
        key_part = slugify(key) if slug_key else key
        return "{}:{}".format(slugify(namespace), key_part) if namespace else key_part

    def ensure_db(self):
        if self.created_db:
            return

        self.postgres.create_database(name=_DAGSTER_STATE_DBNAME)
        engine = self.postgres.create_engine(db_name=_DAGSTER_STATE_DBNAME)
        Base.metadata.create_all(bind=engine, checkfirst=True)
        self.created_db = True

    def get_state(
        self, key: str, namespace: Optional[str] = None, slug_key: bool = True
    ) -> Union[str, None]:
        self.ensure_db()

        key_ns = self.build_key(key=key, namespace=namespace, slug_key=slug_key)
        self.logger.debug("Reading state key: %s", key_ns)

        with Session(
            self.postgres.create_engine(db_name=_DAGSTER_STATE_DBNAME)
        ) as session:
            stmt = select(KeyValue).where(KeyValue.key == key_ns)
            kv = session.execute(stmt).scalars().one_or_none()
            return kv.value if kv else None

    def set_state(
        self,
        key: str,
        value: str,
        namespace: Optional[str] = None,
        slug_key: bool = True,
    ):
        self.ensure_db()

        key_ns = self.build_key(key=key, namespace=namespace, slug_key=slug_key)
        self.logger.debug("Writing state key: %s - %s", key_ns, value)

        with Session(
            self.postgres.create_engine(db_name=_DAGSTER_STATE_DBNAME)
        ) as session:
            stmt = select(KeyValue).where(KeyValue.key == key_ns)
            kv = session.execute(stmt).scalars().one_or_none()

            if kv is None:
                kv = KeyValue(key=key_ns, value=value)
            else:
                kv.value = value

            session.add(kv)
            session.commit()

    def get_batch(
        self, keys: List[str], namespace: Optional[str] = None, slug_key: bool = True
    ) -> Dict[str, str]:
        self.ensure_db()

        keys_ns = {
            self.build_key(
                key=base_key, namespace=namespace, slug_key=slug_key
            ): base_key
            for base_key in keys
        }

        with Session(
            self.postgres.create_engine(db_name=_DAGSTER_STATE_DBNAME)
        ) as session:
            stmt = select(KeyValue).where(KeyValue.key.in_(keys_ns.keys()))
            items: List[KeyValue] = session.execute(stmt).scalars().all()
            return {keys_ns[item.key]: item.value for item in items}
