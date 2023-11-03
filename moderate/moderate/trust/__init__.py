import pprint
from typing import Any, Dict

from dagster import (
    Config,
    EnvVar,
    OpExecutionContext,
    RunConfig,
    RunRequest,
    SensorEvaluationContext,
    job,
    op,
    sensor,
)

from moderate.enums import Variables
from moderate.resources import KeycloakResource


class BaseTrustServicesConfig(Config):
    url: str


class NewUserTrustServicesConfig(BaseTrustServicesConfig):
    keycloak_user: Dict[str, Any]


@op
def call_trust_services_new_user(
    context: OpExecutionContext, config: NewUserTrustServicesConfig
):
    context.log.debug(
        (
            "Sending request to Trust Services API to "
            "generate the keypair and DID for a new user "
            "observed in Keycloak (i.e. the OIDC server)"
        )
    )

    context.log.debug("Keycloak user:\n%s", pprint.pformat(config.keycloak_user))


@job
def call_trust_services_new_user_job():
    call_trust_services_new_user()


@sensor(job=call_trust_services_new_user_job)
def keycloak_user_sensor(context: SensorEvaluationContext, keycloak: KeycloakResource):
    """Sensor that detects new users in Keycloak and calls the Trust Services API."""

    # ToDo:
    # Iterating over the entire set of users in Keycloak is not the most optimal solution.
    # We could optimize this sensor by using a cursor if the need arises:
    # https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors#sensor-optimizations-using-cursors

    users = keycloak.get_keycloak_admin().get_users({})

    for user in users:
        yield RunRequest(
            run_key=user["username"],
            run_config=RunConfig(
                ops={
                    "call_trust_services_new_user": NewUserTrustServicesConfig(
                        url=EnvVar(Variables.TRUST_SERVICES_API_URL.value),
                        keycloak_user=user,
                    )
                }
            ),
        )


def get_asset_hash() -> bytes:
    raise NotImplementedError


class NewAssetTrustServicesConfig(BaseTrustServicesConfig):
    asset_id: str


@op
def call_trust_services_new_asset(
    context: OpExecutionContext, config: NewAssetTrustServicesConfig
):
    context.log.debug(
        (
            "Sending request to Trust Services API to "
            "create the integrity proof for a new asset"
        )
    )


@job
def call_trust_services_new_asset_job():
    call_trust_services_new_asset()


@sensor(job=call_trust_services_new_asset_job)
def data_asset_sensor(context: SensorEvaluationContext):
    """Sensor that detects new data assets and calls the Trust Services API."""

    pass


class VerifyAssetTrustServicesConfig(BaseTrustServicesConfig):
    asset_id: str


@op
def verify_asset_trust_services(
    context: OpExecutionContext, config: VerifyAssetTrustServicesConfig
):
    context.log.debug("Verifying the integrity of an asset")


@job
def verify_asset_trust_services_job():
    verify_asset_trust_services()
