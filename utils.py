import os
from google.cloud import storage

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
    """Converts bytes to human-readable format."""
    for unit in UNITS:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} {UNITS[-1]}"