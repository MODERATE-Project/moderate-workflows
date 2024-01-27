import os
import pprint
from typing import Dict

import pandas as pd
from dagster import (
    AssetIn,
    AssetOut,
    Config,
    MetadataValue,
    Output,
    asset,
    get_dagster_logger,
    multi_asset,
)

from moderate.datasets.enums import DataFormats, DatasetNames
from moderate.datasets.utils import (
    GitAssetForPlatform,
    GitAssetForPlatformDagsterType,
    GitRepo,
    clone_git_repo,
    upload_git_asset_for_platform,
)
from moderate.resources import PlatformAPIResource, PostgresResource


class BuildingStockDatasetsConfig(Config):
    git_url: str = "https://github.com/MODERATE-Project/building-stock-analysis.git"
    git_treeish: str = "main"

    dataset_paths: Dict[str, str] = {
        DatasetNames.BUILDING_STOCK_T31_EPC_CLASSIFICATION.value: os.path.join(
            "T3.1-dynamic-analysis",
            "Case-study-I-EPCs-classification",
            "MODERATE-D3.1-Dataset1.csv",
        ),
        DatasetNames.BUILDING_STOCK_T31_PV_ANALYSIS.value: os.path.join(
            "T3.1-dynamic-analysis",
            "Case-study-II-III-PV-analysis",
            "MODERATE-D3.1-Dataset2_3.csv",
        ),
        DatasetNames.BUILDING_STOCK_T32_BUILDING_STOCK.value: os.path.join(
            "T3.2-static-analysis",
            "data",
            "HEU MODERATE Building Stock Data_Sources.csv",
        ),
    }


@multi_asset(
    outs={
        DatasetNames.BUILDING_STOCK_T31_EPC_CLASSIFICATION.value: AssetOut(
            dagster_type=GitAssetForPlatform
        ),
        DatasetNames.BUILDING_STOCK_T31_PV_ANALYSIS.value: AssetOut(
            dagster_type=GitAssetForPlatform
        ),
        DatasetNames.BUILDING_STOCK_T32_BUILDING_STOCK.value: AssetOut(
            dagster_type=GitAssetForPlatform
        ),
    }
)
def building_stock(config: BuildingStockDatasetsConfig):
    """Creates the building stock analysis data assets."""

    logger = get_dagster_logger()
    git_repo = GitRepo(repo_url=config.git_url, tree_ish=config.git_treeish)

    with clone_git_repo(config=git_repo) as cloned_repo:
        file_paths = {
            key: os.path.join(cloned_repo.repo_dir, val)
            for key, val in config.dataset_paths.items()
            if os.path.exists(os.path.join(cloned_repo.repo_dir, val))
        }

        read_csv_kwargs = {
            DatasetNames.BUILDING_STOCK_T31_PV_ANALYSIS.value: {
                "skiprows": 1,
                "sep": ";",
                "decimal": ",",
            }
        }

        logger.debug("Read CSV kwargs:\n%s", pprint.pformat(read_csv_kwargs))

        dfs: Dict[DatasetNames, pd.DataFrame] = {
            key: pd.read_csv(path, **read_csv_kwargs.get(key, {}))
            for key, path in file_paths.items()
        }

        parquet_bytes = {key: df.to_parquet(path=None) for key, df in dfs.items()}

        output_metadata = {
            "repo_url": cloned_repo.repo_url,
            "commit_sha": cloned_repo.commit_sha,
        }

        for dataset_name, value in parquet_bytes.items():
            yield Output(
                value=GitAssetForPlatform(
                    data=value,
                    name=dataset_name,
                    format=DataFormats.PARQUET,
                    metadata=output_metadata,
                    series_id=dataset_name,
                ),
                output_name=dataset_name,
                metadata={
                    **output_metadata,
                    **{
                        "preview".lower(): MetadataValue.md(
                            dfs[dataset_name].head().to_markdown()
                        ),
                        "size_MiB": MetadataValue.float(len(value) / (1024.0**2)),
                    },
                },
            )


_ASSET_FOR_PLATFORM = "asset_for_platform"


@asset(
    ins={
        _ASSET_FOR_PLATFORM: AssetIn(
            DatasetNames.BUILDING_STOCK_T31_EPC_CLASSIFICATION.value,
            dagster_type=GitAssetForPlatformDagsterType,
        )
    },
)
def platform_asset_building_stock_epc(
    asset_for_platform: GitAssetForPlatform,
    platform_api: PlatformAPIResource,
    postgres: PostgresResource,
) -> Output[Dict]:
    """Uploads the building stock EPC classification data to the platform."""

    return upload_git_asset_for_platform(
        asset_for_platform=asset_for_platform,
        platform_api=platform_api,
        postgres=postgres,
    )


@asset(
    ins={
        _ASSET_FOR_PLATFORM: AssetIn(
            DatasetNames.BUILDING_STOCK_T31_PV_ANALYSIS.value,
            dagster_type=GitAssetForPlatformDagsterType,
        )
    },
)
def platform_asset_building_stock_pv(
    asset_for_platform: GitAssetForPlatform,
    platform_api: PlatformAPIResource,
    postgres: PostgresResource,
) -> Output[Dict]:
    """Uploads the building stock PV analysis data to the platform."""

    return upload_git_asset_for_platform(
        asset_for_platform=asset_for_platform,
        platform_api=platform_api,
        postgres=postgres,
    )


@asset(
    ins={
        _ASSET_FOR_PLATFORM: AssetIn(
            DatasetNames.BUILDING_STOCK_T32_BUILDING_STOCK.value,
            dagster_type=GitAssetForPlatformDagsterType,
        )
    },
)
def platform_asset_building_stock(
    asset_for_platform: GitAssetForPlatform,
    platform_api: PlatformAPIResource,
    postgres: PostgresResource,
) -> Output[Dict]:
    """Uploads the building stock data to the platform."""

    return upload_git_asset_for_platform(
        asset_for_platform=asset_for_platform,
        platform_api=platform_api,
        postgres=postgres,
    )
