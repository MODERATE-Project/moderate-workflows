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
from moderate.state import PostgresState
from moderate.trust.utils import (
    DIDResponseDict,
    KeycloakUserDict,
    add_rounded_datetime,
    wait_for_task,
)

_SENSOR_MIN_INTERVAL_SECONDS: int = 300
_LIMIT_PER_RUN: int = 5
_MAX_FREQ_MINUTES: int = 30


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

    kc_user = KeycloakUserDict(the_dict=config.keycloak_user_dict)
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

    pgstate = PostgresState(postgres=postgres)

    if did_response.did_exists_already:
        context.log.info("The DID for user %s already exists", kc_user.username)

        pgstate.set_state(
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

    pgstate.set_state(
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
    context.log.info("Found %s users in Keycloak", len(user_dicts))

    kc_users = [KeycloakUserDict(the_dict=user_dict) for user_dict in user_dicts]
    kc_users = {kc_user.username: kc_user for kc_user in kc_users}
    usernames_all = list(kc_users.keys())

    pgstate = PostgresState(postgres=postgres)

    user_did_states = pgstate.get_batch(
        keys=usernames_all,
        namespace=StateNamespaces.USER_TRUST_DID.value,
        slug_key=False,
    )

    usernames_with_did = list(user_did_states.keys())
    context.log.info("%s users already have a DID", len(usernames_with_did))

    usernames_pending = list(set(usernames_all).difference(set(usernames_with_did)))
    context.log.info("%s users pending a DID", len(usernames_pending))

    kc_users = [
        kc_user
        for username, kc_user in kc_users.items()
        if username in usernames_pending
    ]

    context.log.debug("Limit per run: %s", _LIMIT_PER_RUN)
    kc_users = kc_users[:_LIMIT_PER_RUN]

    context.log.debug(
        "Requesting DID creation for usernames:\n%s",
        pprint.pformat([kc_user.username for kc_user in kc_users]),
    )

    for kc_user in kc_users:
        run_key = add_rounded_datetime(
            run_key=kc_user.username, minutes_interval=_MAX_FREQ_MINUTES
        )

        yield RunRequest(
            run_key=run_key,
            run_config=RunConfig(
                ops={
                    "propagate_new_user_to_trust_services": UserRegistrationTrustConfig(
                        keycloak_user_dict=kc_user.user_dict,
                    )
                }
            ),
        )
