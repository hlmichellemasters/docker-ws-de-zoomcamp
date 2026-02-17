import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time

# ====== CONFIG ======
BUCKET_NAME = "michelle-masters-nytaxi-2019-2020"
CREDENTIALS_FILE = "secrets/gcs.json"

# DataTalksClub releases (csv.gz) - NOT NYC TLC
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

TAXI_TYPES = ["yellow", "green"]
YEARS = ["2019", "2020"]
MONTHS = [f"{i:02d}" for i in range(1, 13)]

DOWNLOAD_DIR = "./data"  # keep workspace tidy
CHUNK_SIZE = 8 * 1024 * 1024
MAX_WORKERS = 6
RETRIES = 3
# ====================

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
bucket = client.bucket(BUCKET_NAME)


def create_bucket(bucket_name: str):
    try:
        client.get_bucket(bucket_name)
        project_bucket_ids = [bckt.id for bckt in client.list_buckets()]
        if bucket_name not in project_bucket_ids:
            print(f"Bucket '{bucket_name}' exists but is NOT in your project. Pick a new name.")
            sys.exit(1)
        print(f"Bucket '{bucket_name}' exists. Proceeding...")
    except NotFound:
        b = storage.Bucket(client, name=bucket_name)
        b.location = "US-CENTRAL1"  # match your BigQuery dataset location if using regional datasets
        client.create_bucket(b)
        print(f"Created bucket '{bucket_name}' in {b.location}")
    except Forbidden:
        print(f"Bucket '{bucket_name}' exists but is not accessible (name taken). Choose a different bucket name.")
        sys.exit(1)


def download_file(taxi_type: str, year: str, month: str):
    # e.g. .../yellow/yellow_tripdata_2019-01.csv.gz
    fname = f"{taxi_type}_tripdata_{year}-{month}.csv.gz"
    url = f"{BASE_URL}/{taxi_type}/{fname}"
    file_path = os.path.join(DOWNLOAD_DIR, fname)

    try:
        print(f"Downloading {url} ...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"FAILED download {url}: {e}")
        return None


def verify_gcs_upload(blob_name: str) -> bool:
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path: str, max_retries: int = RETRIES):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(1, max_retries + 1):
        try:
            print(f"Uploading {file_path} to gs://{BUCKET_NAME}/{blob_name} (attempt {attempt}) ...")
            blob.upload_from_filename(file_path)
            if verify_gcs_upload(blob_name):
                print(f"Verified upload: {blob_name}")
                return True
            print(f"Verification failed: {blob_name}")
        except Exception as e:
            print(f"FAILED upload {blob_name}: {e}")
        time.sleep(3)

    print(f"Gave up on {blob_name}")
    return False


if __name__ == "__main__":
    create_bucket(BUCKET_NAME)

    tasks = [(t, y, m) for t in TAXI_TYPES for y in YEARS for m in MONTHS]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        file_paths = list(ex.map(lambda x: download_file(*x), tasks))

    file_paths = [p for p in file_paths if p is not None]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        list(ex.map(upload_to_gcs, file_paths))

    print("Done: all files processed.")
