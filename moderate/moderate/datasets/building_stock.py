from typing import Dict, List

from dagster import Config, Output, asset

from moderate.datasets.enums import PlatformBuiltinAssetNames
from moderate.datasets.utils import (
    GitAssetForPlatform,
    ListOfGitAssetForPlatformDagsterType,
    clone_and_parse_datasets,
    upload_list_of_git_assets,
)
from moderate.resources import PlatformAPIResource, PostgresResource


class BuildingStockDatasetsConfig(Config):
    git_url: str = "https://github.com/MODERATE-Project/building-stock-analysis.git"
    git_treeish: str = "main"

    dataset_paths: List[str] = [
        "T3.1-dynamic-analysis/**/*.csv",
        "T3.2-static-analysis/**/*.csv",
    ]


@asset(dagster_type=ListOfGitAssetForPlatformDagsterType)
def building_stock(
    config: BuildingStockDatasetsConfig,
) -> Output[List[GitAssetForPlatform]]:
    """Downloads the building stock datasets from the source Git repository."""

    return clone_and_parse_datasets(
        platform_asset_name=PlatformBuiltinAssetNames.BUILDING_STOCK.value,
        git_url=config.git_url,
        git_treeish=config.git_treeish,
        git_file_globs=config.dataset_paths,
        pd_read_kwargs={
            "MODERATE-D3.1-Dataset2_3.csv": {
                "skiprows": 1,
                "sep": ";",
                "decimal": ",",
            }
        },
    )


@asset
def platform_asset_building_stock(
    building_stock: List[GitAssetForPlatform],
    platform_api: PlatformAPIResource,
    postgres: PostgresResource,
) -> Output[List[Dict]]:
    """Uploads the building stock datasets to the platform."""

    return upload_list_of_git_assets(
        list_of_git_assets=building_stock,
        platform_api=platform_api,
        postgres=postgres,
    )
