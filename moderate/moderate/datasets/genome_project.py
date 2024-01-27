import glob
import os
import pprint
from typing import List

import pandas as pd
from dagster import Config, MetadataValue, Output, asset, get_dagster_logger

from moderate.datasets.enums import DataFormats
from moderate.datasets.utils import (
    GitAssetForPlatform,
    GitRepo,
    ListOfGitAssetForPlatformDagsterType,
    clone_git_repo,
    dataset_name_from_file_path,
)


class GenomeProjectDatasetsConfig(Config):
    git_url: str = "https://github.com/buds-lab/building-data-genome-project-2.git"
    git_treeish: str = "master"

    dataset_paths: List[str] = [
        "data/meters/cleaned/*.csv",
        "data/weather/*.csv",
        "data/metadata/*.csv",
    ]


@asset(dagster_type=ListOfGitAssetForPlatformDagsterType)
def genome_project_datasets(
    config: GenomeProjectDatasetsConfig,
) -> Output[List[GitAssetForPlatform]]:
    logger = get_dagster_logger()
    git_repo = GitRepo(repo_url=config.git_url, tree_ish=config.git_treeish)

    with clone_git_repo(
        config=git_repo, lfs=True, lfs_globs=config.dataset_paths
    ) as cloned_repo:
        file_paths = [
            os.path.abspath(file_path)
            for dataset_path in config.dataset_paths
            for file_path in glob.glob(os.path.join(cloned_repo.repo_dir, dataset_path))
            if os.path.exists(os.path.abspath(file_path))
        ]

        logger.debug("Found the following files:\n%s", pprint.pformat(file_paths))
        read_csv_kwargs = {}

        dfs = {
            fpath: pd.read_csv(fpath, **read_csv_kwargs.get(fpath, {}))
            for fpath in file_paths
        }

        for fpath, df in dfs.items():
            logger.debug("Sample of DataFrame '%s':\n%s", fpath, df.head())

        parquet_bytes = {fpath: df.to_parquet(path=None) for fpath, df in dfs.items()}

        output_metadata = {
            "repo_url": cloned_repo.repo_url,
            "commit_sha": cloned_repo.commit_sha,
        }

        output_value = []
        output_metadata = {}

        for fpath, data in parquet_bytes.items():
            dataset_name = dataset_name_from_file_path(
                file_path=fpath, sibling_paths=file_paths
            )

            output_value.append(
                GitAssetForPlatform(
                    data=data,
                    name=dataset_name,
                    format=DataFormats.PARQUET,
                    metadata=output_metadata,
                    series_id=dataset_name,
                )
            )

            output_metadata.update(
                {
                    f"{dataset_name}_size_MiB": len(data) / (1024.0**2),
                    f"{dataset_name}_preview": MetadataValue.md(
                        dfs[fpath].head().to_markdown()
                    ),
                }
            )

        return Output(output_value, metadata=output_metadata)
