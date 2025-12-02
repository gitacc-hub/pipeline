import os #Read env files (azure connection string)
import pandas as pd #Used to transform csv files
from io import StringIO #Turn text in file like object
from azure.storage.blob import BlobServiceClient #Allows python to connect to Azure Blob Storage

# Read connection string from GitHub Actions environment
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING") #Pulls Secret codes stored in Github Secrets

# Connect to Azure Storage
blob_service = BlobServiceClient.from_connection_string(connection_string)
# Creates a connection object .Your pipeline uses this to read and write files from Azure

raw_container = "raw" # Local of inout files
clean_container = "clean" #Location where clean files go

raw_client = blob_service.get_container_client(raw_container)
clean_client = blob_service.get_container_client(clean_container)

print("Listing files in raw container...")

# Loop through all files in raw/
for blob in raw_client.list_blobs():
    blob_name = blob.name
    print(f"Processing {blob_name} ...")

    # Download the blob as text
    raw_blob = raw_client.get_blob_client(blob_name)
    data = raw_blob.download_blob().content_as_text()

    # Convert text into pandas DataFrame
    df = pd.read_csv(StringIO(data))

    # Simple transformation
    df = df.dropna()

    # Convert cleaned df to CSV (string)
    output = df.to_csv(index=False)

    # Upload cleaned file to clean container
    clean_name = f"cleaned-{blob_name}"
    clean_blob = clean_client.get_blob_client(clean_name)
    clean_blob.upload_blob(output, overwrite=True)

    print(f"Uploaded cleaned file -> {clean_name}")

print("All files processed successfully.")
