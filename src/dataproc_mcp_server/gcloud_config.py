"""Utility functions to read gcloud configuration values."""

import subprocess

import structlog

logger = structlog.get_logger(__name__)


def get_default_project() -> str | None:
    """Get the default project ID from gcloud config.

    Returns:
        The default project ID if configured, None otherwise.
    """
    try:
        result = subprocess.run(
            ["gcloud", "config", "get-value", "core/project"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if result.returncode == 0 and result.stdout.strip():
            project_id = result.stdout.strip()
            logger.debug(
                "Retrieved default project from gcloud config", project_id=project_id
            )
            return project_id
        else:
            logger.debug("No default project configured in gcloud")
            return None

    except subprocess.TimeoutExpired:
        logger.warning("Timeout while reading gcloud config for project")
        return None
    except FileNotFoundError:
        logger.debug(
            "gcloud CLI not found - install Google Cloud SDK to use default project"
        )
        return None
    except Exception as e:
        logger.warning("Error reading default project from gcloud config", error=str(e))
        return None


def get_default_region() -> str | None:
    """Get the default region from gcloud config.

    Returns:
        The default compute region if configured, None otherwise.
    """
    try:
        result = subprocess.run(
            ["gcloud", "config", "get-value", "compute/region"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if result.returncode == 0 and result.stdout.strip():
            region = result.stdout.strip()
            logger.debug("Retrieved default region from gcloud config", region=region)
            return region
        else:
            logger.debug("No default region configured in gcloud")
            return None

    except subprocess.TimeoutExpired:
        logger.warning("Timeout while reading gcloud config for region")
        return None
    except FileNotFoundError:
        logger.debug(
            "gcloud CLI not found - install Google Cloud SDK to use default region"
        )
        return None
    except Exception as e:
        logger.warning("Error reading default region from gcloud config", error=str(e))
        return None


def get_default_zone() -> str | None:
    """Get the default zone from gcloud config.

    Returns:
        The default compute zone if configured, None otherwise.
    """
    try:
        result = subprocess.run(
            ["gcloud", "config", "get-value", "compute/zone"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if result.returncode == 0 and result.stdout.strip():
            zone = result.stdout.strip()
            logger.debug("Retrieved default zone from gcloud config", zone=zone)
            return zone
        else:
            logger.debug("No default zone configured in gcloud")
            return None

    except subprocess.TimeoutExpired:
        logger.warning("Timeout while reading gcloud config for zone")
        return None
    except FileNotFoundError:
        logger.debug(
            "gcloud CLI not found - install Google Cloud SDK to use default zone"
        )
        return None
    except Exception as e:
        logger.warning("Error reading default zone from gcloud config", error=str(e))
        return None


def validate_gcloud_config() -> dict[str, str]:
    """Validate gcloud configuration and return available defaults.

    Returns:
        Dictionary with available configuration values.
    """
    config = {}

    project = get_default_project()
    if project:
        config["project"] = project

    region = get_default_region()
    if region:
        config["region"] = region

    zone = get_default_zone()
    if zone:
        config["zone"] = zone

    return config
