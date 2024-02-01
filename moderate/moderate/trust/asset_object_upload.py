import json
import pprint
from datetime import datetime

import requests
from dagster import (
    Config,
    OpExecutionContext,
    RunConfig,
    RunRequest,
    SensorEvaluationContext,
    job,
    op,
    sensor,
)

from moderate.resources import PlatformAPIResource
from moderate.trust.utils import ProofResponseDict, wait_for_task

_SENSOR_MIN_INTERVAL_SECONDS = 300
_GET_REQUEST_LIMIT = 10


class ProofCreationTrustConfig(Config):
    asset_obj_key: str
    task_wait_seconds: int = 600


@op
def create_asset_object_proof(
    context: OpExecutionContext,
    config: ProofCreationTrustConfig,
    platform_api: PlatformAPIResource,
) -> None:
    """Creates an integrity proof for an asset object."""

    context.log.info(
        (
            "Sending request to Trust API to "
            "generate the integrity proof for an asset object: %s"
        ),
        config.asset_obj_key,
    )

    payload = {"object_key_or_id": config.asset_obj_key}
    proof_url = platform_api.url_ensure_trust_proof()
    context.log.debug("POST %s: %s", proof_url, payload)

    req_proof = requests.post(
        proof_url, headers=platform_api.get_authorization_header(), json=payload
    )

    req_proof.raise_for_status()
    req_did_json = req_proof.json()
    context.log.debug("Response:\n%s", pprint.pformat(req_did_json))
    proof_response = ProofResponseDict(the_dict=req_did_json)

    if proof_response.proof_exists_already:
        context.log.info("Proof already exists for object: %s", config.asset_obj_key)
        return

    proof_task_url = platform_api.url_check_proof_task(task_id=proof_response.task_id)

    task_response = wait_for_task(
        task_url=proof_task_url,
        platform_api=platform_api,
        logger=context.log,
        timeout_seconds=config.task_wait_seconds,
    )

    task_response.raise_error()


@job
def create_asset_object_proof_job():
    """Job that creates integrity proofs for asset objects."""

    create_asset_object_proof()


@sensor(
    job=create_asset_object_proof_job,
    minimum_interval_seconds=_SENSOR_MIN_INTERVAL_SECONDS,
)
def platform_api_asset_object_sensor(
    context: SensorEvaluationContext,
    platform_api: PlatformAPIResource,
):
    """Sensor that monitors the platform API for new asset objects."""

    assets_url = platform_api.url_find_asset_objects()
    context.log.debug("GET %s", assets_url)
    headers = platform_api.get_authorization_header()

    resp = requests.get(
        assets_url,
        headers=headers,
        params={
            "filters": json.dumps([["proof_id", "eq", None]]),
            "limit": _GET_REQUEST_LIMIT,
        },
    )

    resp.raise_for_status()
    resp_json = resp.json()
    context.log.debug("Response:\n%s", pprint.pformat(resp_json))

    for asset_object in resp_json:
        asset_obj_key = asset_object.get("key")

        if asset_obj_key is None:
            context.log.warning("Asset object has no key: %s", asset_object)
            continue

        utcnow = datetime.utcnow()

        # At most one run per hour per asset object
        run_key = "{}_{}_{}_{}_{}".format(
            asset_obj_key, utcnow.year, utcnow.month, utcnow.day, utcnow.hour
        )

        yield RunRequest(
            run_key=run_key,
            run_config=RunConfig(
                ops={
                    "create_asset_object_proof": ProofCreationTrustConfig(
                        asset_obj_key=asset_obj_key,
                    )
                }
            ),
        )
