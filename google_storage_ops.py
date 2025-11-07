import pandas as pd

class GoogleCloudStorage:
    def __init__(self, bucket_name, prefix, client):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.client = client

    def auth(self):
        """Performs the authentication process with GCS and returns the HTTPIterator"""
        bucket = self.client.get_bucket(self.bucket_name)
        iterator = bucket.list_blobs(prefix=self.prefix)
        return iterator

    def collect_blobs(self, csv_file):
        """Collects all the objects from the specified bucket and prefix into two
         different lists for the name and the size. At the end it saves the information
         to a .csv file for further analysis"""
        main_obj = self.auth()
        blob_names = []
        blob_sizes = []

        for blob in main_obj:
            if blob.size != 0:
                blob_names.append(blob.name)
                blob_sizes.append(blob.size)

        recorded = {
            "blob_name": blob_names,
            "blob_size": blob_sizes
        }
        df = pd.DataFrame(recorded)
        df.to_csv(csv_file, index=False)

    def get_root_prefix(self):
        """Iterates through the objects, using the split function to convert the strings
        into a list and selecting the second index to get the root prefixes.
        Saves the information in a set to remove duplicate entries, which
        is converted at the end into a list"""
        main_obj = self.auth()
        blob_list = {blob.name.split("/")[1] for blob in main_obj if blob.size != 0}
        return list(blob_list)