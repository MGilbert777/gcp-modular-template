from google.cloud import storage, bigquery, pubsub_v1, secretmanager
import os

class GCPClientFactory:
    """
    Factory to provide GCP clients using ADC.
    Supports modularity by allowing project_id overrides per customer.
    """
    def __init__(self, project_id: str = None):
        self.project_id = project_id or os.getenv("GCP_PROJECT_ID")

    def get_storage_client(self) -> storage.Client:
        return storage.Client(project=self.project_id)

    def get_bq_client(self) -> bigquery.Client:
        return bigquery.Client(project=self.project_id)

    def get_secret_client(self) -> secretmanager.SecretManagerServiceClient:
        return secretmanager.SecretManagerServiceClient()
