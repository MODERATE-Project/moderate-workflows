import pprint
from datetime import datetime
from typing import Any, Dict

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

from moderate.enums import StateNamespaces
from moderate.resources import KeycloakResource, PlatformAPIResource, PostgresResource
from moderate.trust.utils import DIDResponseDict, KeycloakUserDict, wait_for_task

_SENSOR_MIN_INTERVAL_SECONDS = 600
_LIMIT_PER_RUN = 10


class UserRegistrationTrustConfig(Config):
    keycloak_user_dict: Dict[str, Any]
    task_wait_seconds: int = 600


@op
def propagate_new_user_to_trust_services(
    context: OpExecutionContext,
    config: UserRegistrationTrustConfig,
    platform_api: PlatformAPIResource,
    postgres: PostgresResource,
) -> None:
    """Propagates a new user from Keycloak to the Trust API."""

    context.log.info(
        (
            "Sending request to Trust API to "
            "generate the DID for a new user "
            "observed in Keycloak:\n%s"
        ),
        pprint.pformat(config.keycloak_user_dict),
    )

    kc_user = KeycloakUserDict(user_dict=config.keycloak_user_dict)
    payload = {"username": kc_user.username}
    did_url = platform_api.url_ensure_user_trust_did()
    context.log.debug("POST %s: %s", did_url, payload)

    req_did = requests.post(
        did_url, headers=platform_api.get_authorization_header(), json=payload
    )

    req_did.raise_for_status()
    req_did_json = req_did.json()
    context.log.debug("Response:\n%s", pprint.pformat(req_did_json))
    did_response = DIDResponseDict(the_dict=req_did_json)

    if did_response.did_exists_already:
        context.log.info("The DID for user %s already exists", kc_user.username)

        postgres.set_state(
            key=kc_user.username,
            namespace=StateNamespaces.USER_TRUST_DID.value,
            value=datetime.utcnow().isoformat(),
            slug_key=False,
        )

        return

    did_task_url = platform_api.url_check_did_task(task_id=did_response.task_id)

    task_response = wait_for_task(
        task_url=did_task_url,
        platform_api=platform_api,
        logger=context.log,
        timeout_seconds=config.task_wait_seconds,
    )

    task_response.raise_error()

    postgres.set_state(
        key=kc_user.username,
        namespace=StateNamespaces.USER_TRUST_DID.value,
        value=datetime.utcnow().isoformat(),
        slug_key=False,
    )


@job
def propagate_new_user_to_trust_services_job():
    """Job that propagates a new user from Keycloak to the Trust API."""

    propagate_new_user_to_trust_services()


@sensor(
    job=propagate_new_user_to_trust_services_job,
    minimum_interval_seconds=_SENSOR_MIN_INTERVAL_SECONDS,
)
def keycloak_user_sensor(
    context: SensorEvaluationContext,
    keycloak: KeycloakResource,
    postgres: PostgresResource,
):
    """Sensor that detects new users in Keycloak."""

    # ToDo:
    # Iterating over the entire set of users in Keycloak is not the most optimal solution.
    # We could optimize this sensor by using a cursor if the need arises:
    # https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors#sensor-optimizations-using-cursors

    user_dicts = keycloak.get_keycloak_admin().get_users({})
    run_counter = 0

    for user_dict in user_dicts:
        kc_user = KeycloakUserDict(user_dict=user_dict)

        did_state_value = postgres.get_state(
            key=kc_user.username,
            namespace=StateNamespaces.USER_TRUST_DID.value,
            slug_key=False,
        )

        if did_state_value:
            continue

        utcnow = datetime.utcnow()

        # At most one run per hour per user
        run_key = "{}_{}_{}_{}_{}".format(
            kc_user.username, utcnow.year, utcnow.month, utcnow.day, utcnow.hour
        )

        yield RunRequest(
            run_key=run_key,
            run_config=RunConfig(
                ops={
                    "propagate_new_user_to_trust_services": UserRegistrationTrustConfig(
                        keycloak_user_dict=user_dict,
                    )
                }
            ),
        )

        run_counter += 1

        if run_counter >= _LIMIT_PER_RUN:
            break
