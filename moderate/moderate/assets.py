import enum
import os
import pprint
import shutil
import uuid
from typing import Dict

import pandas as pd
from dagster import (
    AssetExecutionContext,
    Config,
    MetadataValue,
    asset,
    get_dagster_logger,
)
from sh import git

from moderate.resources import KeycloakResource, PostgresResource


@asset
def keycloak_users_count(
    context: AssetExecutionContext, keycloak: KeycloakResource
) -> int:
    """Pings the Keycloak server by counting the users."""

    count = keycloak.get_users_count()
    context.add_output_metadata(metadata={"count": count})
    return count


class BuildingStockDatasetsConfig(Config):
    git_url: str = "https://github.com/MODERATE-Project/building-stock-analysis.git"
    tree_ish: str = "main"


class BuildingStockDatasets(enum.Enum):
    T31_EPC_CLASSIFICATION = "T31_EPC_CLASSIFICATION"
    T31_PV_ANALYSIS = "T31_PV_ANALYSIS"
    T32_BUILDING_STOCK = "T32_BUILDING_STOCK"


@asset
def building_stock_datasets(
    context: AssetExecutionContext, config: BuildingStockDatasetsConfig
) -> Dict[BuildingStockDatasets, pd.DataFrame]:
    """Clones the WP3 building stock analysis repository and returns the datasets."""

    git_dir = str(uuid.uuid4())
    logger = get_dagster_logger()
    logger.info("Cloning repository %s into %s", config.git_url, git_dir)

    try:
        logger.info("Cloning repository: %s", config.git_url)
        git.clone(config.git_url, git_dir)
        logger.info("Checking out tree-ish: %s", config.tree_ish)
        git.reset("--hard", config.tree_ish, _cwd=git_dir)

        csv_paths = {
            BuildingStockDatasets.T31_EPC_CLASSIFICATION: os.path.join(
                git_dir,
                "T3.1-dynamic-analysis",
                "Case-study-I-EPCs-classification",
                "MODERATE-D3.1-Dataset1.csv",
            ),
            BuildingStockDatasets.T31_PV_ANALYSIS: os.path.join(
                git_dir,
                "T3.1-dynamic-analysis",
                "Case-study-II-III-PV-analysis",
                "MODERATE-D3.1-Dataset2_3.csv",
            ),
            BuildingStockDatasets.T32_BUILDING_STOCK: os.path.join(
                git_dir,
                "T3.2-static-analysis",
                "data",
                "HEU MODERATE Building Stock Data_Sources.csv",
            ),
        }

        logger.debug("CSV dataset paths:\n%s", pprint.pformat(csv_paths))

        read_csv_kwargs = {
            BuildingStockDatasets.T31_PV_ANALYSIS: {
                "skiprows": 1,
                "sep": ";",
                "decimal": ",",
            }
        }

        logger.debug("Read CSV kwargs:\n%s", pprint.pformat(read_csv_kwargs))

        dfs = {
            key: pd.read_csv(path, **read_csv_kwargs.get(key, {}))
            for key, path in csv_paths.items()
        }

        context.add_output_metadata(
            metadata={
                f"preview_{key.value}".lower(): MetadataValue.md(
                    df.head().to_markdown()
                )
                for key, df in dfs.items()
            }
        )

        return dfs
    finally:
        try:
            logger.debug("Removing dir: %s", git_dir)
            shutil.rmtree(git_dir)
        except Exception:
            pass


TABLE_BUILDING_STOCK = "building_stock_analysis"


class BuildingStockTablesConfig(Config):
    database: str = TABLE_BUILDING_STOCK


@asset
def building_stock_tables(
    context: AssetExecutionContext,
    config: BuildingStockTablesConfig,
    postgres: PostgresResource,
    building_stock_datasets: Dict[BuildingStockDatasets, pd.DataFrame],
) -> None:
    """Creates the building stock tables in the Postgres database."""

    logger = get_dagster_logger()

    logger.info("Creating database %s", config.database)
    postgres.create_database(config.database)

    metadata = {}

    for key, df in building_stock_datasets.items():
        with postgres.create_engine(db_name=config.database).connect() as conn:
            table = key.value.lower()
            logger.info("Writing dataset %s to table %s", key, table)
            num_rows = df.to_sql(name=table, con=conn, if_exists="replace")
            metadata[f"num_rows_{table}"] = num_rows

    context.add_output_metadata(metadata=metadata)
