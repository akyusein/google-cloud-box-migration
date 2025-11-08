import pandas as pd
from google.cloud import storage
from google_storage_ops import GoogleCloudStorage
import os

UNITS = ['B', 'KiB', 'MiB', 'GiB', 'TiB']

def get_client():
    """Authenticates to Google Cloud Storage and returns a storage client object."""
    credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_path:
        raise ValueError(
            "Please set the GOOGLE_APPLICATION_CREDENTIALS environment variable to your service account key path.")
    else:
        return storage.Client()

def format_bytes(size_bytes):
    """Convert bytes to human-readable format."""
    for unit in UNITS:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} {UNITS[-1]}"

operation = GoogleCloudStorage('bucket_name', "prefix", get_client())
operation.collect_blobs("collected_blobs.csv")

df = pd.read_csv("collected_blobs.csv")

df['root_prefix'] = df['blob_name'].str.split('/').str[1]
root_prefix_sizes = df.groupby('root_prefix')['blob_size'].sum().reset_index()
root_prefix_sizes.columns = ['root_prefix', 'total_size_bytes']
root_prefix_sizes.to_csv("b_backup_top_level.csv", index=False)