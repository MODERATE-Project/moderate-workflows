from dagster import AssetExecutionContext, asset

from moderate.resources import KeycloakResource


@asset
def keycloak_users_count(
    context: AssetExecutionContext, keycloak: KeycloakResource
) -> int:
    """Pings the Keycloak server by counting the users."""

    count = keycloak.get_users_count()
    context.add_output_metadata(metadata={"count": count})
    return count
