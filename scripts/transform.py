import os
import pandas as pd
from azure.storage.blob import BlobServiceClient

# Read connection string from GitHub Actions environment
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# Connect to Azure Storage
blob_service = BlobServiceClient.from_connection_string(connection_string)

raw_container = "raw"
clean_container = "clean"

raw_client = blob_service.get_container_client(raw_container)
clean_client = blob_service.get_container_client(clean_container)

print("Listing files in raw container...")

# Loop through all files in raw/
for blob in raw_client.list_blobs():
    blob_name = blob.name
    print(f"Processing {blob_name} ...")

    # Download file
    raw_blob = raw_client.get_blob_client(blob_name)
    data = raw_blob.download_blob().content_as_text()

    # Use pandas to transform
    df = pd.read_csv(pd.compat.StringIO(data))
    df = df.dropna()

    # Convert cleaned data to CSV (in memory)
    output = df.to_csv(index=False)

    # Upload cleaned version: cleaned-input.csv
    clean_name = f"cleaned-{blob_name}"
    clean_blob = clean_client.get_blob_client(clean_name)

    clean_blob.upload_blob(output, overwrite=True)

    print(f"Uploaded cleaned file -> {clean_name}")

print("All files processed.")
