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


class GenomeProjectDatasetsConfig(Config):
    git_url: str = "https://github.com/buds-lab/building-data-genome-project-2.git"
    git_treeish: str = "master"
    git_lfs: bool = True

    dataset_paths: List[str] = [
        "data/meters/cleaned/*.csv",
        "data/weather/*.csv",
        "data/metadata/*.csv",
    ]


@asset(dagster_type=ListOfGitAssetForPlatformDagsterType)
def genome_project_datasets(
    config: GenomeProjectDatasetsConfig,
) -> Output[List[GitAssetForPlatform]]:
    """Downloads the GENOME Project datasets from the source Git repository."""

    return clone_and_parse_datasets(
        platform_asset_name=PlatformBuiltinAssetNames.GENOME_PROJECT.value,
        git_url=config.git_url,
        git_treeish=config.git_treeish,
        git_file_globs=config.dataset_paths,
        git_lfs=config.git_lfs,
        git_lfs_globs=config.dataset_paths,
    )


@asset
def platform_asset_genome_project_datasets(
    genome_project_datasets: List[GitAssetForPlatform],
    platform_api: PlatformAPIResource,
    postgres: PostgresResource,
) -> Output[List[Dict]]:
    """Uploads the GENOME Project datasets to the platform."""

    return upload_list_of_git_assets(
        list_of_git_assets=genome_project_datasets,
        platform_api=platform_api,
        postgres=postgres,
    )
