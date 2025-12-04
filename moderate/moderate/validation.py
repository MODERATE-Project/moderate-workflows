"""Configuration validation for MODERATE workflows.

This module provides utilities for validating environment configuration
at startup, ensuring all required variables are set before workflow
execution begins. Early validation prevents runtime errors and provides
clear error messages for misconfiguration.

Key Functions:
- validate_required_env_vars(): Check required environment variables
- validate_resources(): Validate resource configuration
- get_missing_required_vars(): Get list of missing required variables

Example:
    from moderate.validation import validate_resources

    # In __init__.py or before Definitions
    validation_errors = validate_resources()
    if validation_errors:
        print("Configuration errors:", validation_errors)
"""

import os
from typing import Dict, List, Optional

from dagster import get_dagster_logger

from moderate.enums import Variables, VariableDefaults


def get_missing_required_vars() -> List[str]:
    """Get list of required environment variables that are not set.

    Checks all environment variables defined in the Variables enum that
    don't have corresponding defaults in VariableDefaults.

    Returns:
        List of missing required variable names (empty if all present)
    """
    required_vars = [
        Variables.KEYCLOAK_SERVER_URL,
        Variables.KEYCLOAK_ADMIN_USERNAME,
        Variables.KEYCLOAK_ADMIN_PASSWORD,
        Variables.POSTGRES_HOST,
        Variables.POSTGRES_PORT,
        Variables.POSTGRES_USERNAME,
        Variables.POSTGRES_PASSWORD,
        Variables.OPEN_METADATA_HOST,
        Variables.OPEN_METADATA_PORT,
        Variables.OPEN_METADATA_TOKEN,
        Variables.S3_ACCESS_KEY_ID,
        Variables.S3_SECRET_ACCESS_KEY,
        Variables.S3_REGION,
        Variables.S3_BUCKET_NAME,
        Variables.API_BASE_URL,
        Variables.API_USERNAME,
        Variables.API_PASSWORD,
        Variables.RABBIT_URL,
    ]

    missing = []
    for var in required_vars:
        if not os.getenv(var.value):
            missing.append(var.value)

    return missing


def get_optional_vars_with_defaults() -> Dict[str, str]:
    """Get dictionary of optional variables and their default values.

    Returns:
        Dict mapping variable names to their default values
    """
    return {
        Variables.KEYCLOAK_MAIN_REALM_NAME.value: VariableDefaults.KEYCLOAK_MAIN_REALM_NAME.value,
        Variables.S3_ENDPOINT_URL.value: VariableDefaults.S3_ENDPOINT_URL.value,
        Variables.RABBIT_MATRIX_PROFILE_QUEUE.value: VariableDefaults.RABBIT_MATRIX_PROFILE_QUEUE.value,
        Variables.S3_JOB_OUTPUTS_BUCKET_NAME.value: VariableDefaults.S3_JOB_OUTPUTS_BUCKET_NAME.value,
    }


def validate_resources(raise_on_error: bool = False) -> Optional[List[str]]:
    """Validate all required resource environment variables are set.

    Checks that all required environment variables for external service
    integrations are configured. Optionally raises an exception on
    validation failure.

    Args:
        raise_on_error: If True, raises EnvironmentError on validation failure

    Returns:
        List of missing variables (None if all present), or raises if
        raise_on_error=True

    Raises:
        EnvironmentError: If raise_on_error=True and validation fails

    Example:
        # Check without raising
        errors = validate_resources()
        if errors:
            print(f"Missing: {errors}")

        # Fail fast
        validate_resources(raise_on_error=True)
    """
    logger = get_dagster_logger()
    missing = get_missing_required_vars()

    if not missing:
        logger.info("All required environment variables validated successfully")

        optional_vars = get_optional_vars_with_defaults()
        for var_name, default_value in optional_vars.items():
            actual_value = os.getenv(var_name, default_value)
            if actual_value == default_value:
                logger.debug("Using default for %s: %s", var_name, default_value)

        return None

    error_msg = (
        f"Missing {len(missing)} required environment variable(s):\n  "
        + "\n  ".join(f"- {var}" for var in missing)
        + "\n\nPlease set these in your .env file or environment.\n"
        + "See CLAUDE.md for configuration details."
    )

    if raise_on_error:
        raise EnvironmentError(error_msg)

    logger.warning(error_msg)
    return missing


def validate_numeric_vars() -> Dict[str, str]:
    """Validate that numeric environment variables contain valid integers.

    Returns:
        Dict mapping variable names to error messages (empty if all valid)
    """
    numeric_vars = {
        Variables.POSTGRES_PORT.value: os.getenv(Variables.POSTGRES_PORT.value),
        Variables.OPEN_METADATA_PORT.value: os.getenv(
            Variables.OPEN_METADATA_PORT.value
        ),
    }

    errors = {}
    for var_name, var_value in numeric_vars.items():
        if var_value is None:
            continue

        try:
            int(var_value)
        except ValueError:
            errors[var_name] = f"Must be a valid integer, got: {var_value}"

    return errors


def validate_url_vars() -> Dict[str, str]:
    """Validate that URL environment variables are properly formatted.

    Returns:
        Dict mapping variable names to error messages (empty if all valid)
    """
    url_vars = {
        Variables.KEYCLOAK_SERVER_URL.value: os.getenv(
            Variables.KEYCLOAK_SERVER_URL.value
        ),
        Variables.S3_ENDPOINT_URL.value: os.getenv(
            Variables.S3_ENDPOINT_URL.value, VariableDefaults.S3_ENDPOINT_URL.value
        ),
        Variables.API_BASE_URL.value: os.getenv(
            Variables.API_BASE_URL.value, VariableDefaults.API_BASE_URL.value
        ),
    }

    errors = {}
    for var_name, var_value in url_vars.items():
        if var_value is None:
            continue

        if not var_value.startswith(("http://", "https://")):
            errors[var_name] = f"Must start with http:// or https://, got: {var_value}"

    return errors


def validate_all(raise_on_error: bool = False) -> Dict[str, List[str]]:
    """Run all validation checks and return comprehensive results.

    Args:
        raise_on_error: If True, raises EnvironmentError on any validation failure

    Returns:
        Dict with validation results:
        - 'missing': List of missing required variables
        - 'numeric_errors': Dict of numeric validation errors
        - 'url_errors': Dict of URL validation errors

    Raises:
        EnvironmentError: If raise_on_error=True and any validation fails
    """
    logger = get_dagster_logger()

    results = {
        "missing": get_missing_required_vars(),
        "numeric_errors": validate_numeric_vars(),
        "url_errors": validate_url_vars(),
    }

    has_errors = any(
        [
            results["missing"],
            results["numeric_errors"],
            results["url_errors"],
        ]
    )

    if has_errors:
        error_parts = []

        if results["missing"]:
            error_parts.append(
                "Missing required variables:\n  "
                + "\n  ".join(f"- {v}" for v in results["missing"])
            )

        if results["numeric_errors"]:
            error_parts.append(
                "Invalid numeric values:\n  "
                + "\n  ".join(
                    f"- {k}: {v}" for k, v in results["numeric_errors"].items()
                )
            )

        if results["url_errors"]:
            error_parts.append(
                "Invalid URL values:\n  "
                + "\n  ".join(f"- {k}: {v}" for k, v in results["url_errors"].items())
            )

        error_msg = "\n\n".join(error_parts)

        if raise_on_error:
            raise EnvironmentError(f"Configuration validation failed:\n\n{error_msg}")

        logger.warning("Configuration validation failed:\n%s", error_msg)
    else:
        logger.info("All configuration validations passed")

    return results
