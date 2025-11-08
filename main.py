from google_storage_ops import GoogleCloudStorage
from utils import get_client

try:
    #Initializes the Google Cloud Storage object
    operation = GoogleCloudStorage('bucket', "prefix", get_client(), "collected_blobs.csv")

    #Collects the blobs by performing API calls and storing the data in a cache.
    operation.collect_blobs()

    #The method returns the sum of the root prefixes from the cached data.
    operation.format_prefix()

    print("Operations completed successfully!")

except Exception as e:
    print(f"Error: {e}")