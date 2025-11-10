from google_storage_ops import GoogleCloudStorage

try:
    #Initializes the Google Cloud Storage object
    operation = GoogleCloudStorage("b_backup_aksel", "B:", "collected_blobs.csv")

    #The method returns the sum of the root prefixes from the cached data.
    operation.format_prefix()

    print("Operations completed successfully!")

except Exception as e:
    print(f"Error: {e}")