from google_storage_ops import GoogleCloudStorage
import pymongo

my_client = pymongo.MongoClient("mongodb://localhost:27017/")
gcs_db = my_client["prefix"]
gcs_col = gcs_db["size"]

try:
    #Initializes the Google Cloud Storage object.
    operation = GoogleCloudStorage("bucket", "prefix", "collected_blobs.csv")

    #Add the entries of the prefix name and size to the NoSQL database.
    db_entries = operation.obtain_blobs_for_db()
    params = gcs_col.insert_many(db_entries)
    print(params.inserted_ids)

    #The method returns the sum of the root prefixes from the cached data.
    operation.format_prefix()

    print("Operations completed successfully!")

except Exception as e:
    print(f"Error: {e}")