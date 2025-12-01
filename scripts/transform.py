from azure.storage.blob import BlobServiceClient
import pandas as pd
import os

connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

raw_container = "raw"
clean_container = "clean"

# List all blobs in raw container
raw_blobs = blob_service_client.get_container_client(raw_container).list_blobs()

for blob in raw_blobs:
    blob_name = blob.name
    print(f"Processing {blob_name}...")

    # Download
    raw_blob = blob_service_client.get_blob_client(container=raw_container, blob=blob_name)
    downloaded = raw_blob.download_blob().readall()
    with open("temp.csv", "wb") as f:
        f.write(downloaded)

    # Transform
    df = pd.read_csv("temp.csv")
    df = df.dropna()
    df.to_csv("temp_clean.csv", index=False)

    # Upload to clean container
    clean_blob = blob_service_client.get_blob_client(container=clean_container, blob=f"cleaned-{blob_name}")
    with open("temp_clean.csv", "rb") as f:
        clean_blob.upload_blob(f, overwrite=True)

    print(f"{blob_name} processed and uploaded.")
