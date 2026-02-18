"""
Script to update MongoDB documents from address CSV data.

For residential records (class R, X, P): Updates documents with address fields.
For non-residential records: Deletes documents from MongoDB.

Uses chunked CSV reading and bulk MongoDB operations for efficiency
when processing millions of records.
"""

import logging
import time
from pathlib import Path
from typing import Optional

import pandas as pd
from pymongo import UpdateOne, DeleteOne
from pymongo.errors import BulkWriteError
from tqdm import tqdm

from src.utils.mongo_client import MongoDBClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("mongo_update.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# Residential classification codes
RESIDENTIAL_CLASSES = {"R", "X", "P"}

# Fields to extract from CSV and map to MongoDB
ADDRESS_FIELD_MAPPING = {
    "uprn": "ab_uprn",
    "udprn": "udprn",
    "building_name": "building_name",
    "building_number": "building_number",
    "thoroughfare": "thoroughfare",
    "post_town": "post_town",
    "postcode": "ab_postcode",
    "x_coordinate": "x_coordinate",
    "y_coordinate": "y_coordinate",
    "latitude": "latitude",
    "longitude": "longitude",
    "class": "class",
}

# GeoJSON location field name
LOCATION_FIELD = "location"


def is_residential(class_value: str) -> bool:
    """Check if the class indicates a residential property."""
    if pd.isna(class_value):
        return False
    # Class values may have trailing spaces
    return class_value.strip()[:1] in RESIDENTIAL_CLASSES


def ensure_2dsphere_index(collection, field_name: str = LOCATION_FIELD) -> None:
    """
    Ensure a 2dsphere index exists on the specified field.

    Args:
        collection: MongoDB collection
        field_name: Name of the GeoJSON field to index
    """
    index_name = f"{field_name}_2dsphere"
    existing_indexes = collection.index_information()

    if index_name not in existing_indexes:
        logger.info(f"Creating 2dsphere index on '{field_name}' field...")
        collection.create_index([(field_name, "2dsphere")], name=index_name)
        logger.info(f"2dsphere index '{index_name}' created successfully")
    else:
        logger.info(f"2dsphere index '{index_name}' already exists")


def process_chunk(
    chunk: pd.DataFrame,
    collection,
    uid_field: str = "uid",
) -> dict:
    """
    Process a chunk of CSV data and prepare bulk operations.

    Args:
        chunk: DataFrame chunk from CSV
        collection: MongoDB collection
        uid_field: Name of the UID column in CSV

    Returns:
        Dictionary with counts of updates and deletes
    """
    operations = []
    update_count = 0
    delete_count = 0

    for _, row in chunk.iterrows():
        uid = row.get(uid_field)
        if pd.isna(uid) or not uid:
            continue

        class_value = row.get("class", "")

        if is_residential(class_value):
            # Build update document for residential properties
            update_doc = {}
            for csv_field, mongo_field in ADDRESS_FIELD_MAPPING.items():
                value = row.get(csv_field)
                if pd.notna(value):
                    # Convert numpy types to Python native types
                    if hasattr(value, "item"):
                        value = value.item()
                    update_doc[mongo_field] = value

            # Create GeoJSON Point from latitude and longitude
            latitude = row.get("latitude")
            longitude = row.get("longitude")
            if pd.notna(latitude) and pd.notna(longitude):
                # Convert numpy types to Python native types
                if hasattr(latitude, "item"):
                    latitude = latitude.item()
                if hasattr(longitude, "item"):
                    longitude = longitude.item()
                # GeoJSON Point format: [longitude, latitude]
                update_doc[LOCATION_FIELD] = {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }

            if update_doc:
                operations.append(
                    UpdateOne(
                        {"uid": uid},
                        {"$set": update_doc},
                    )
                )
                update_count += 1
        else:
            # Delete non-residential documents
            operations.append(DeleteOne({"uid": uid}))
            delete_count += 1

    # Execute bulk operations
    if operations:
        try:
            collection.bulk_write(operations, ordered=False)
        except BulkWriteError as e:
            logger.warning(f"Bulk write error (some operations may have succeeded): {e.details}")

    return {"updates": update_count, "deletes": delete_count}


def update_mongo_from_csv(
    csv_path: str,
    database_name: str,
    collection_name: str,
    connection_string: str = "mongodb://localhost:27017",
    chunk_size: int = 10000,
    uid_field: str = "uid",
    progress_file: Optional[str] = None,
) -> dict:
    """
    Update MongoDB documents from a CSV file.

    Args:
        csv_path: Path to the CSV file
        database_name: MongoDB database name
        collection_name: MongoDB collection name
        connection_string: MongoDB connection string
        chunk_size: Number of rows to process per chunk
        uid_field: Name of the UID column in CSV
        progress_file: Optional file to save progress

    Returns:
        Dictionary with total counts
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    # Count total rows for progress tracking (minus header)
    logger.info(f"Counting rows in {csv_path}...")
    total_rows = sum(1 for _ in open(csv_path)) - 1
    logger.info(f"Total rows to process: {total_rows:,}")

    # Initialize MongoDB connection
    client = MongoDBClient(
        connection_string=connection_string,
        database_name=database_name,
    )

    total_updates = 0
    total_deletes = 0
    processed_rows = 0
    start_time = time.time()

    try:
        with client:
            collection = client.get_collection(collection_name)

            # Ensure 2dsphere index exists for geospatial queries
            ensure_2dsphere_index(collection)

            # Process CSV in chunks with tqdm progress bar
            logger.info(f"Processing CSV in chunks of {chunk_size:,}...")

            chunks = pd.read_csv(csv_path, chunksize=chunk_size, low_memory=False)

            with tqdm(total=total_rows, desc="Processing", unit="rows") as pbar:
                for chunk in chunks:
                    result = process_chunk(chunk, collection, uid_field)

                    total_updates += result["updates"]
                    total_deletes += result["deletes"]
                    processed_rows += len(chunk)

                    # Update progress bar
                    pbar.update(len(chunk))
                    pbar.set_postfix({
                        "updates": total_updates,
                        "deletes": total_deletes,
                    })

                    # Save progress to file if specified
                    if progress_file:
                        with open(progress_file, "w") as f:
                            f.write(
                                f"processed={processed_rows}\n"
                                f"total={total_rows}\n"
                                f"updates={total_updates}\n"
                                f"deletes={total_deletes}\n"
                            )

    except Exception as e:
        logger.error(f"Error processing CSV: {e}")
        raise

    total_time = time.time() - start_time
    logger.info(
        f"Completed in {total_time/60:.1f} minutes | "
        f"Total updates: {total_updates:,} | Total deletes: {total_deletes:,}"
    )

    return {
        "total_rows": processed_rows,
        "updates": total_updates,
        "deletes": total_deletes,
        "elapsed_seconds": total_time,
    }


def main():
    """Main entry point to start the MongoDB update from CSV."""
    project_dir = Path(__file__).resolve().parent.parent.parent
    csv_path = project_dir / "data" / "found_addresses.csv"

    result = update_mongo_from_csv(
        csv_path=str(csv_path),
        database_name="leases",
        collection_name="leases",
    )
    logger.info(f"Final result: {result}")


if __name__ == "__main__":
    main()



