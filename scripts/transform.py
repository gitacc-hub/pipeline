import pandas as pd
import sys
from azure.storage.blob import BlobServiceClient
import os

connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

blob_service_client = BlobServiceClient.from_connection_string(connection_string)

raw_container = "raw"
clean_container = "clean"
input_blob_name = sys.argv[1]
output_blob_name = "cleaned-" + input_blob_name

# Step 1: Download file from raw container
raw_blob = blob_service_client.get_blob_client(container=raw_container, blob=input_blob_name)
downloaded = raw_blob.download_blob().readall()

with open("temp.csv", "wb") as f:
    f.write(downloaded)

# Step 2: Transform
df = pd.read_csv("temp.csv")
df = df.dropna()
df.to_csv("temp_clean.csv", index=False)

# Step 3: Upload to clean container
clean_blob = blob_service_client.get_blob_client(container=clean_container, blob=output_blob_name)

with open("temp_clean.csv", "rb") as clean_file:
    clean_blob.upload_blob(clean_file, overwrite=True)

print("Pipeline completed successfully!")
