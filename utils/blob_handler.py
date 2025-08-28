from azure.storage.blob import BlobServiceClient
import datetime, os
from dotenv import load_dotenv

load_dotenv() 

BLOB_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER = "crypto-raw"

def connect_blob():
    return BlobServiceClient.from_connection_string(BLOB_CONN_STR)

def save_raw_to_blob(blob_client, symbol, df, folder="incremental"):
    raw_json = df.to_json(orient="records", date_format="iso")
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    blob_name = f"{folder}/{symbol}_{ts}.json"
    container_client = blob_client.get_container_client(CONTAINER)
    container_client.upload_blob(name=blob_name, data=raw_json, overwrite=True)
    print(f"☁️ Saved raw {symbol} to Blob: {blob_name}")
