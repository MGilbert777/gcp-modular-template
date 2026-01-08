from google.cloud import secretmanager
from .logger_factory import setup_logging

logger = setup_logging("api")

def fetch_secret(secret_id: str, project_id: str = None) -> str:
    """
    Retrieves a secret version from GCP Secret Manager.

    Args:
        secret_id: The ID of the secret to fetch.
        project_id: The GCP project ID.

    Returns:
        The secret string payload.
    """
    project = project_id or os.getenv("GCP_PROJECT_ID")
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project}/secrets/{secret_id}/versions/latest"
    
    try:
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logger.error(f"Failed to fetch secret {secret_id}: {e}")
        raise
