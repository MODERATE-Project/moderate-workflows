from dagster import Config, OpExecutionContext, SensorEvaluationContext, job, op, sensor

from moderate.moderate.resources import KeycloakResource


class TrustServicesConfig(Config):
    url: str


@op
def call_trust_services_new_user(
    context: OpExecutionContext, config: TrustServicesConfig
):
    context.log.debug(
        (
            "Sending request to Trust Services API to "
            "generate the keypair and DID for a new user "
            "observed in Keycloak (OIDC server)."
        )
    )


@job
def call_trust_services_new_user_job():
    call_trust_services_new_user()


@sensor(job=call_trust_services_new_user_job)
def keycloak_user_sensor(context: SensorEvaluationContext, keycloak: KeycloakResource):
    """Sensor that detects new users in Keycloak and calls the Trust Services API.
    Interating over the entire set of users in Keycloak is not the most optimal solution.
    We could optimize this sensor by using a cursor if the need arises:
    https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors#sensor-optimizations-using-cursors
    """

    pass
