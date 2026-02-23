"""
Check for lease data updates from the GOV.UK Land Property Data API.
Downloads and processes change files if new versions are available.
"""

import os
import re
import zipfile
from pathlib import Path

import requests
from dotenv import load_dotenv

from src.utils.mongo_client import MongoDBClient


# Load environment variables
load_dotenv()

# Config
API_BASE = "https://use-land-property-data.service.gov.uk/api/v1/datasets/leases"
DATA_DIR = Path(__file__).parent.parent.parent / "lease_data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def extract_version_from_filename(filename: str) -> str | None:
    """Extract YYYY-MM version from filename."""
    match = re.search(r"_(\d{4})_(\d{2})\.zip$", filename)
    return f"{match.group(1)}-{match.group(2)}" if match else None


def download_file_with_auth(url: str, file_path: Path) -> None:
    """Download file from pre-signed URL."""
    response = requests.get(url, stream=True)

    if not response.ok:
        error_body = response.text
        print(f"Download failed with status {response.status_code}")
        print(f"Response body: {error_body}")
        raise Exception(f"Failed to download file: {response.status_code}")

    with open(file_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)


def clean_csv_trailing_row_count(csv_path: Path) -> None:
    """Remove trailing Row Count line from CSV file."""
    with open(csv_path, "r", encoding="latin-1") as f:
        lines = f.read().split("\n")

    # Remove blank lines at end
    while lines and lines[-1].strip() == "":
        lines.pop()

    # Find the Row Count line
    row_count_pattern = re.compile(r'^"Row Count:"\s*,\s*"\d+"\s*$')
    row_count_index = None

    for i, line in enumerate(lines):
        if row_count_pattern.match(line.strip()):
            row_count_index = i
            break

    if row_count_index is not None:
        lines = lines[:row_count_index]
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        print("Removed Row Count line and any trailing content")
    else:
        # Still save as UTF-8 for consistency
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        print("No Row Count line found at end of CSV")


def unzip_and_clean_csv(zip_path: Path) -> None:
    """Unzip the file and process the CSV."""
    print(f"Extracting ZIP: {zip_path}")

    with zipfile.ZipFile(zip_path, "r") as z:
        csv_files = [f for f in z.namelist() if f.lower().endswith(".csv")]

        if not csv_files:
            raise Exception("No CSV file found in ZIP")

        csv_entry = csv_files[0]
        csv_path = DATA_DIR / csv_entry

        z.extract(csv_entry, DATA_DIR)

        print(f"Extracted CSV to {csv_path}")

    clean_csv_trailing_row_count(csv_path)


def check_for_update() -> None:
    """
    Check for and download lease data updates.
    - Fetch dataset metadata from GOV.UK API
    - Extract version from filename
    - Check MongoDB if version already applied
    - If new, get signed download URL, download, unzip, and clean CSV
    """
    # Get environment variables
    mongo_uri = os.getenv("MONGO_URI")
    mongo_database = os.getenv("MONGO_DATABASE")
    govuk_api_key = os.getenv("GOVUK_API_KEY")

    if not govuk_api_key:
        raise Exception("GOVUK_API_KEY environment variable is required")

    # Connect to MongoDB
    mongo_client = MongoDBClient(connection_string=mongo_uri, database_name=mongo_database)
    mongo_client.connect()
    print("Connected to MongoDB")

    headers = {"Authorization": govuk_api_key}

    try:
        # Step 1: Fetch dataset metadata
        response = requests.get(API_BASE, headers=headers)
        if not response.ok:
            raise Exception(f"Failed to fetch dataset metadata: {response.status_code}")

        result = response.json().get("result", {})
        resources = result.get("resources", [])

        change_file = next(
            (r for r in resources if r.get("name") == "Change Only File"),
            None
        )

        if not change_file:
            raise Exception("Change Only File not found in dataset")

        version = extract_version_from_filename(change_file.get("file_name", ""))
        if not version:
            raise Exception("Could not extract version from filename")

        # Check if version already applied
        collection = mongo_client.get_collection("lease_update_logs")
        already_applied = collection.find_one({"version": version})

        if already_applied:
            print(f"Version {version} already applied. Exiting.")
            return

        print(f"New version found: {version}")

        # Step 2: Get signed download URL
        file_meta_response = requests.get(
            f"{API_BASE}/{change_file['file_name']}",
            headers=headers
        )

        if not file_meta_response.ok:
            raise Exception(f"Failed to fetch file metadata: {file_meta_response.status_code}")

        file_meta = file_meta_response.json().get("result", {})
        file_url = file_meta.get("download_url")

        file_path = DATA_DIR / change_file["file_name"]

        print(f"Downloading file to {file_path}...")
        download_file_with_auth(file_url, file_path)

        print(f"File downloaded successfully: {file_path}")

        unzip_and_clean_csv(file_path)

    finally:
        mongo_client.close()


if __name__ == "__main__":
    try:
        check_for_update()
    except Exception as err:
        print(f"Error: {err}")
        exit(1)

