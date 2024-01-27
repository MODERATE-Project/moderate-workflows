import io
import json
import pprint
import shutil
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from typing import IO, Any, ContextManager, Dict, Optional

import requests
from dagster import Output, get_dagster_logger, usable_as_dagster_type
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

    @property
    def file_name(self) -> str:
        return f"{self.name}.{self.format.value.lower()}"

    @property
    def file_name_slug(self) -> str:
        return slugify(self.file_name)


GitAssetForPlatformDagsterType = usable_as_dagster_type(GitAssetForPlatform)


@contextmanager
def clone_git_repo(config: GitRepo) -> ContextManager[ClonedRepo]:
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
) -> Output[Dict]:
    logger = get_dagster_logger()
    logger.info("Uploading asset %s to platform", asset_for_platform.name)
    the_asset = ensure_asset(platform_api=platform_api, name=asset_for_platform.name)
    logger.info("Asset: %s", the_asset)
    meta = asset_for_platform.metadata or {}
    this_commit = meta.get("commit_sha")
    logger.info("Commit from input: %s", this_commit)

    state_commit = postgres.get_state(
        key=asset_for_platform.name,
        namespace=StateNamespaces.GIT_DATASET_COMMIT.value,
    )

    logger.info("Commit found in state DB: %s", state_commit)

    if this_commit and state_commit and this_commit == state_commit:
        logger.info("Commit %s already uploaded: skipping", state_commit)
        return Output({}, metadata={"commit_sha": state_commit, "skipped": True})

    uploaded_object = upload_asset_object(
        platform_api=platform_api,
        asset_id=the_asset["id"],
        data_file=io.BytesIO(asset_for_platform.data),
        data_file_name=asset_for_platform.file_name,
        tags=meta,
        series_id=asset_for_platform.file_name_slug,
    )

    postgres.set_state(
        key=asset_for_platform.name,
        value=this_commit,
        namespace=StateNamespaces.GIT_DATASET_COMMIT.value,
    )

    return Output(
        uploaded_object,
        metadata={
            key: val
            for key, val in uploaded_object.items()
            if isinstance(val, (str, int, float))
        },
    )