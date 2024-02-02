from typing import Dict, List, Union

from dagster import Optional, get_dagster_logger
from slugify import slugify
from sqlalchemy import Column, Integer, String, select
from sqlalchemy.orm import Session, declarative_base

from moderate.resources import PostgresResource

_DAGSTER_STATE_DBNAME = "moderate_workflows_dagster_state"
_DAGSTER_STATE_TABLE_KEYVALUES = "moderate_workflows_key_values"

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
