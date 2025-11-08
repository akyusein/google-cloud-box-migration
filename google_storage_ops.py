import pandas as pd
from utils import format_bytes

class GoogleCloudStorage:
    """Constructor method for the class."""
    def __init__(self, bucket_name, prefix, client, csv_file):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.client = client
        self.csv_file = csv_file

    def auth(self):
        """Performs the authentication process with GCS and returns the HTTPIterator."""
        try:
            bucket = self.client.get_bucket(self.bucket_name)
            iterator = bucket.list_blobs(prefix=self.prefix)
            return iterator
        except Exception as e:
            raise RuntimeError(f"Failed to authenticate or access bucket: {e}")

    def collect_blobs(self):
        """Collects all the objects from the specified bucket and prefix into two
         different lists for the name and the size. At the end it saves the information
         to a .csv file for further analysis."""
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
        df.to_csv(self.csv_file, index=False)
        return df

    def collect_prefix(self):
        """Groups the entries with the same top-level prefix and uses
        the sum function to add up the total size for that prefix.
        Returns a new dataframe saved to a .csv file"""
        df = self.collect_blobs()
        df['root_prefix'] = df['blob_name'].str.split('/').str[1]
        root_prefix_sizes = df.groupby('root_prefix')['blob_size'].sum().reset_index()
        root_prefix_sizes.columns = ['root_prefix', 'total_size_bytes']
        root_prefix_sizes.to_csv("top_level.csv", index=False)
        return root_prefix_sizes

    def format_prefix(self):
        """Converts the raw bytes in the dataframe into a human-readable format."""
        df = self.collect_prefix()
        root_prefix_list = []
        total_size_list = []
        for index, row in df.iterrows():
            total_size_list.append(format_bytes(row.total_size_bytes))
            root_prefix_list.append(row.root_prefix)
        formatted = {
                "root_prefix": root_prefix_list,
                "total_size": total_size_list
            }
        new_df = pd.DataFrame(formatted)
        new_df.to_csv("finalised_prefix.csv", index=False)