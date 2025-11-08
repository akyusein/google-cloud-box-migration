from google_storage_ops import GoogleCloudStorage
from utils import get_client

try:
    operation = GoogleCloudStorage('bucket', "prefix", get_client(), "collected_blobs.csv")

    operation.collect_blobs()

    operation.format_prefix()

    print("Operations completed successfully!")

except Exception as e:
    print(f"Error: {e}")