import glob
import io
import json
import os
import pprint
import shutil
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from typing import IO, Any, ContextManager, Dict, List, Optional, Tuple

import pandas as pd
import requests
from dagster import (
    DagsterType,
    MetadataValue,
    Output,
    get_dagster_logger,
    usable_as_dagster_type,
)
from sh import git
from slugify import slugify

from moderate.datasets.enums import DataFormats
from moderate.enums import StateNamespaces
from moderate.resources import PlatformAPIResource, PostgresResource


@dataclass
class GitRepo:
    repo_url: str
    tree_ish: str


@dataclass
class ClonedRepo:
    repo_url: str
    repo_dir: str
    commit_sha: str


@dataclass
class GitAssetForPlatform:
    data: bytes
    name: str
    format: DataFormats
    metadata: Optional[Dict[str, str]] = None
    series_id: Optional[str] = None
    parent_asset_name: Optional[str] = None

    @property
    def file_name(self) -> str:
        return f"{self.name}.{self.format.value.lower()}"

    @property
    def file_name_slug(self) -> str:
        return slugify(self.file_name)

    @property
    def platform_asset_name(self) -> str:
        return self.parent_asset_name or self.name

    @property
    def state_ns(self) -> str:
        return StateNamespaces.GIT_DATASET_COMMIT.value

    @property
    def state_key(self) -> str:
        return (
            "{}-{}".format(self.parent_asset_name, self.name)
            if self.parent_asset_name
            else self.name
        )

    @property
    def state_val(self) -> Optional[str]:
        return self.metadata.get("commit_sha") if self.metadata else None


GitAssetForPlatformDagsterType = usable_as_dagster_type(GitAssetForPlatform)


def is_list_of_git_asset_for_platform(_, value):
    return isinstance(value, list) and all(
        isinstance(i, GitAssetForPlatform) for i in value
    )


ListOfGitAssetForPlatformDagsterType = DagsterType(
    name="ListOfGitAssetForPlatform",
    type_check_fn=is_list_of_git_asset_for_platform,
    description="A list of GitAssetForPlatform objects",
)


def dataset_name_from_file_path(
    file_path: str, sibling_paths: Optional[List[str]] = None
) -> str:
    common_prefix = os.path.commonprefix(sibling_paths) if sibling_paths else ""
    file_path = file_path.replace(common_prefix, "", 1)
    path_part, _ = os.path.splitext(file_path)
    return slugify(path_part)


@contextmanager
def clone_git_repo(
    config: GitRepo, lfs: bool = False, lfs_globs: Optional[List[str]] = None
) -> ContextManager[ClonedRepo]:
    """Context manager that clones a git repository and yields the temporal directory."""

    git_dir = str(uuid.uuid4())
    logger = get_dagster_logger()
    logger.debug("Cloning repository %s into %s", config.repo_url, git_dir)

    try:
        logger.info("Cloning repository: %s", config.repo_url)
        git.clone(config.repo_url, git_dir)
        logger.info("Checking out tree-ish: %s", config.tree_ish)
        git.reset("--hard", config.tree_ish, _cwd=git_dir)
        commit_sha = git("rev-parse", "HEAD", _cwd=git_dir).strip()

        if lfs and not lfs_globs:
            logger.warning("LFS is enabled but no globs are provided")

        if lfs and lfs_globs:
            git.lfs.install(_cwd=git_dir)
            logger.info("Fetching LFS objects: %s", lfs_globs)
            git.lfs.fetch("-I", ",".join(lfs_globs), _cwd=git_dir)
            logger.info("Checking out LFS objects: %s", lfs_globs)
            git.checkout(config.tree_ish, "--", *lfs_globs, _cwd=git_dir)

        yield ClonedRepo(
            repo_url=config.repo_url, repo_dir=git_dir, commit_sha=commit_sha
        )
    finally:
        try:
            logger.debug("Removing dir: %s", git_dir)
            shutil.rmtree(git_dir)
        except Exception:
            logger.debug("Failed to remove dir: %s", git_dir)


def ensure_asset(
    platform_api: PlatformAPIResource, name: str, **asset_kwargs
) -> Dict[str, Any]:
    """Ensures an asset exists on the platform. If it does not exist, it will be created."""

    logger = get_dagster_logger()
    safe_name = slugify(name)
    uuid_name = platform_api.build_url("asset", safe_name)
    asset_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, uuid_name))
    logger.debug("Ensuring asset (name=%s) (uuid=%s)", safe_name, asset_uuid)

    req_find = requests.get(
        platform_api.url_find_assets(),
        headers=platform_api.get_authorization_header(),
        params={
            "limit": 1,
            "filters": json.dumps([["uuid", "eq", asset_uuid]]),
        },
    )

    req_find.raise_for_status()
    found = req_find.json()

    if found and len(found) == 1:
        logger.debug("Found existing asset (name=%s) (uuid=%s)", safe_name, asset_uuid)
        return found[0]
    elif found and len(found) > 1:
        logger.warning("Found multiple assets with same UUID: %s", asset_uuid)

    logger.debug("Creating new asset (name=%s) (uuid=%s)", safe_name, asset_uuid)

    data = {
        "name": safe_name,
        "is_public_ownerless": True,
        "uuid": asset_uuid,
    }

    data.update(asset_kwargs)

    req_create = requests.post(
        platform_api.url_create_asset(),
        headers=platform_api.get_authorization_header(),
        json=data,
    )

    req_create.raise_for_status()
    asset_created = req_create.json()
    logger.debug("Created new asset: %s", asset_created)
    return asset_created


def upload_asset_object(
    platform_api: PlatformAPIResource,
    asset_id: int,
    data_file: IO[bytes],
    data_file_name: str,
    tags: Optional[Dict[str, str]] = None,
    series_id: Optional[str] = None,
) -> Dict[str, Any]:
    logger = get_dagster_logger()
    payload = {}

    if tags:
        payload.update({"tags": json.dumps(tags)})

    if series_id:
        payload.update({"series_id": series_id})
    else:
        logger.warning("No series_id provided: asset will not be versioned")

    files = [("obj", (data_file_name, data_file, "application/octet-stream"))]

    logger.info(
        "Uploading asset object (asset_id=%s):\n%s\n%s",
        asset_id,
        pprint.pformat(payload),
        pprint.pformat(files),
    )

    req_upload = requests.post(
        platform_api.url_upload_asset_object(asset_id),
        headers=platform_api.get_authorization_header(),
        data=payload,
        files=files,
    )

    req_upload.raise_for_status()
    return req_upload.json()


def upload_git_asset_for_platform(
    asset_for_platform: GitAssetForPlatform,
    platform_api: PlatformAPIResource,
    postgres: PostgresResource,
) -> Tuple[Optional[Dict], Dict]:
    logger = get_dagster_logger()
    logger.info("Uploading asset %s to platform", asset_for_platform.name)

    the_asset = ensure_asset(
        platform_api=platform_api, name=asset_for_platform.platform_asset_name
    )

    logger.info("Asset: %s", the_asset)
    this_commit = asset_for_platform.state_val
    logger.info("Commit from input: %s", this_commit)

    db_commit = postgres.get_state(
        key=asset_for_platform.state_key,
        namespace=asset_for_platform.state_ns,
    )

    logger.info("Commit found in state DB: %s", db_commit)

    if this_commit and db_commit and this_commit == db_commit:
        logger.info("Commit %s already uploaded: skipping", db_commit)
        return None, {"commit_sha": db_commit, "skipped": True}

    meta = asset_for_platform.metadata or {}

    uploaded_object = upload_asset_object(
        platform_api=platform_api,
        asset_id=the_asset["id"],
        data_file=io.BytesIO(asset_for_platform.data),
        data_file_name=asset_for_platform.file_name,
        tags=meta,
        series_id=asset_for_platform.file_name_slug,
    )

    postgres.set_state(
        key=asset_for_platform.state_key,
        value=this_commit,
        namespace=asset_for_platform.state_ns,
    )

    return uploaded_object, {
        key: val
        for key, val in uploaded_object.items()
        if isinstance(val, (str, int, float))
    }


def clone_and_parse_datasets(
    git_url: str,
    git_treeish: str,
    git_file_globs: List[str],
    git_lfs: bool = False,
    git_lfs_globs: Optional[List[str]] = None,
    pd_read_kwargs: Optional[Dict[str, Dict]] = None,
    platform_asset_name: Optional[str] = None,
) -> Output[List[GitAssetForPlatform]]:
    logger = get_dagster_logger()
    git_repo = GitRepo(repo_url=git_url, tree_ish=git_treeish)

    with clone_git_repo(
        config=git_repo, lfs=git_lfs, lfs_globs=git_lfs_globs
    ) as cloned_repo:
        file_paths = [
            os.path.abspath(file_path)
            for dataset_path in git_file_globs
            for file_path in glob.glob(os.path.join(cloned_repo.repo_dir, dataset_path))
            if os.path.exists(os.path.abspath(file_path))
        ]

        logger.debug("Found the following files:\n%s", pprint.pformat(file_paths))

        dfs = {}

        for fpath in file_paths:
            read_kwargs_candidates = (
                [key for key in pd_read_kwargs.keys() if fpath.endswith(key)]
                if pd_read_kwargs
                else []
            )

            read_kwargs_key = (
                sorted(read_kwargs_candidates, key=lambda x: len(x), reverse=True)[0]
                if len(read_kwargs_candidates)
                else None
            )

            read_kwargs = (
                pd_read_kwargs.get(read_kwargs_key, {}) if pd_read_kwargs else {}
            )

            logger.debug("Read kwargs for '%s': %s", fpath, read_kwargs)
            dfs[fpath] = pd.read_csv(fpath, **read_kwargs)

        for fpath, df in dfs.items():
            logger.debug("Sample of DataFrame '%s':\n%s", fpath, df.head())

        parquet_bytes = {fpath: df.to_parquet(path=None) for fpath, df in dfs.items()}

        asset_metadata = {
            "repo_url": cloned_repo.repo_url,
            "commit_sha": cloned_repo.commit_sha,
        }

        output_value = []
        output_metadata = {**asset_metadata}

        for fpath, data in parquet_bytes.items():
            dataset_name = dataset_name_from_file_path(
                file_path=fpath, sibling_paths=file_paths
            )

            output_value.append(
                GitAssetForPlatform(
                    data=data,
                    name=dataset_name,
                    format=DataFormats.PARQUET,
                    metadata=asset_metadata,
                    series_id=dataset_name,
                    parent_asset_name=platform_asset_name,
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


def upload_list_of_git_assets(
    list_of_git_assets: List[GitAssetForPlatform],
    platform_api: PlatformAPIResource,
    postgres: PostgresResource,
) -> Output[List[Dict]]:
    logger = get_dagster_logger()
    output_value = []
    output_metadata = {}

    for asset_for_platform in list_of_git_assets:
        logger.info("Uploading asset for platform: %s", asset_for_platform.name)

        uploaded_obj, obj_meta = upload_git_asset_for_platform(
            asset_for_platform=asset_for_platform,
            platform_api=platform_api,
            postgres=postgres,
        )

        output_metadata.update(
            {f"{asset_for_platform.name}_{key}": val for key, val in obj_meta.items()}
        )

        logger.debug("Uploaded object:\n%s", pprint.pformat(uploaded_obj))
        logger.debug("Current metadata:\n%s", pprint.pformat(output_metadata))

    return Output(value=output_value, metadata=output_metadata)
