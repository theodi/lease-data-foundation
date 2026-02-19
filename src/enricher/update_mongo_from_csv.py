"""
Script to update MongoDB documents from address CSV data.

For residential records (class R, X, P): Updates documents with address fields.
For non-residential records: Deletes documents from MongoDB.

Uses chunked CSV reading and bulk MongoDB operations for efficiency
when processing millions of records.
"""

import json
import logging
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from pymongo import DeleteMany, UpdateMany
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
                    UpdateMany(
                        {"uid": uid},
                        {"$set": update_doc},
                    )
                )
                update_count += 1
        else:
            # Delete non-residential documents
            operations.append(DeleteMany({"uid": uid}))
            delete_count += 1

    # Execute bulk operations
    if operations:
        try:
            collection.bulk_write(operations, ordered=False)
        except BulkWriteError as e:
            logger.warning(f"Bulk write error (some operations may have succeeded): {e.details}")

    return {"updates": update_count, "deletes": delete_count}


def update_mongo_from_found_csv(
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


# Postcodes.io API configuration
POSTCODES_IO_BULK_URL = "https://api.postcodes.io/postcodes"
POSTCODES_IO_BATCH_SIZE = 100  # Max 100 postcodes per bulk request
POSTCODES_IO_RATE_LIMIT_DELAY = 0.05  # 50ms delay between requests to be respectful


class PostcodeCache:
    """
    In-memory cache for postcode lookups with optional disk persistence.

    For 1.2M records, caching unique postcodes significantly reduces API calls
    since many records share the same postcode.
    """

    def __init__(self, cache_file: Optional[str] = None):
        """
        Initialize the postcode cache.

        Args:
            cache_file: Optional path to persist cache to disk (JSON format)
        """
        self._cache: dict = {}
        self._cache_file = cache_file
        self._hits = 0
        self._misses = 0

        # Load existing cache from disk if available
        if cache_file:
            self._load_cache()

    def _load_cache(self) -> None:
        """Load cache from disk if exists."""
        if self._cache_file and Path(self._cache_file).exists():
            try:
                with open(self._cache_file, "r") as f:
                    self._cache = json.load(f)
                logger.info(f"Loaded {len(self._cache):,} postcodes from cache file")
            except Exception as e:
                logger.warning(f"Failed to load cache file: {e}")

    def save_cache(self) -> None:
        """Save cache to disk."""
        if self._cache_file:
            try:
                with open(self._cache_file, "w") as f:
                    json.dump(self._cache, f)
                logger.info(f"Saved {len(self._cache):,} postcodes to cache file")
            except Exception as e:
                logger.warning(f"Failed to save cache file: {e}")

    def get(self, postcode: str) -> Optional[dict]:
        """Get postcode data from cache."""
        normalized = self._normalize_postcode(postcode)
        if normalized in self._cache:
            self._hits += 1
            return self._cache[normalized]
        self._misses += 1
        return None

    def set(self, postcode: str, data: Optional[dict]) -> None:
        """Set postcode data in cache (including None for invalid postcodes)."""
        normalized = self._normalize_postcode(postcode)
        self._cache[normalized] = data

    def get_uncached(self, postcodes: list) -> list:
        """Return list of postcodes not in cache."""
        return [pc for pc in postcodes if self._normalize_postcode(pc) not in self._cache]

    @staticmethod
    def _normalize_postcode(postcode: str) -> str:
        """Normalize postcode for consistent cache keys."""
        if postcode is None:
            return ""
        return str(postcode).strip().upper().replace(" ", "")

    @property
    def stats(self) -> dict:
        """Return cache statistics."""
        total = self._hits + self._misses
        hit_rate = (self._hits / total * 100) if total > 0 else 0
        return {
            "size": len(self._cache),
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": f"{hit_rate:.1f}%",
        }


def bulk_lookup_postcodes(postcodes: list, session: requests.Session) -> dict:
    """
    Bulk lookup postcodes using postcodes.io API.

    Args:
        postcodes: List of postcodes to lookup (max 100)
        session: requests Session for connection pooling

    Returns:
        Dictionary mapping postcode -> result data (or None if not found)
    """
    if not postcodes:
        return {}

    # Limit to 100 postcodes per request (API limit)
    postcodes = postcodes[:POSTCODES_IO_BATCH_SIZE]

    try:
        response = session.post(
            POSTCODES_IO_BULK_URL,
            json={"postcodes": postcodes},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        # print(data)

        results = {}
        for item in data.get("result", []):
            query_postcode = item.get("query", "")
            result = item.get("result")

            if result:
                results[query_postcode] = {
                    "latitude": result.get("latitude"),
                    "longitude": result.get("longitude"),
                    "x_coordinate": result.get("eastings"),
                    "y_coordinate": result.get("northings"),
                    # "region": result.get("region"),
                    # "post_town": result.get("admin_district"),  # or use "parish" / "admin_ward"
                }
            else:
                results[query_postcode] = None

        return results

    except requests.exceptions.RequestException as e:
        logger.warning(f"Postcodes.io API error: {e}")
        return {pc: None for pc in postcodes}


def geocode_postcodes_batch(
    postcodes: list,
    cache: PostcodeCache,
    session: requests.Session,
) -> dict:
    """
    Geocode a batch of postcodes using cache and postcodes.io API.

    Args:
        postcodes: List of postcodes to geocode
        cache: PostcodeCache instance
        session: requests Session for connection pooling

    Returns:
        Dictionary mapping postcode -> geocode result (or None)
    """
    results = {}
    uncached_postcodes = []

    # First, check cache for all postcodes
    for pc in postcodes:
        if pc is None or pd.isna(pc):
            continue
        pc_str = str(pc).strip().upper()
        if not pc_str:
            continue

        cached = cache.get(pc_str)
        if cached is not None or cache._normalize_postcode(pc_str) in cache._cache:
            # Found in cache (including None for invalid postcodes)
            results[pc_str] = cached
        else:
            uncached_postcodes.append(pc_str)

    # Fetch uncached postcodes from API in batches
    for i in range(0, len(uncached_postcodes), POSTCODES_IO_BATCH_SIZE):
        batch = uncached_postcodes[i:i + POSTCODES_IO_BATCH_SIZE]
        api_results = bulk_lookup_postcodes(batch, session)

        # Update cache and results
        for pc, data in api_results.items():
            cache.set(pc, data)
            results[pc] = data

        # Small delay to respect rate limits
        if i + POSTCODES_IO_BATCH_SIZE < len(uncached_postcodes):
            time.sleep(POSTCODES_IO_RATE_LIMIT_DELAY)

    return results


def process_not_found_chunk(
    chunk: pd.DataFrame,
    collection,
    cache: PostcodeCache,
    session: requests.Session,
    uid_field: str = "uid",
    postcode_field: str = "pc",
) -> dict:
    """
    Process a chunk of not_found CSV data and prepare bulk update operations.

    For each record with a valid postcode, geocode it using postcodes.io API
    and update the MongoDB document with latitude, longitude, location (GeoJSON Point),
    rgn (region uppercase), and post_town (uppercase).

    Args:
        chunk: DataFrame chunk from CSV
        collection: MongoDB collection
        cache: PostcodeCache instance for caching lookups
        session: requests Session for API calls
        uid_field: Name of the UID column in CSV
        postcode_field: Name of the postcode column in CSV

    Returns:
        Dictionary with counts of updates and skipped records
    """
    operations = []
    update_count = 0
    skipped_count = 0

    # Extract all postcodes from chunk for batch processing
    postcodes = chunk[postcode_field].dropna().unique().tolist()

    # Batch geocode all unique postcodes in chunk
    geocode_results = geocode_postcodes_batch(postcodes, cache, session)

    for _, row in chunk.iterrows():
        uid = row.get(uid_field)
        if pd.isna(uid) or not uid:
            skipped_count += 1
            continue

        postcode = row.get(postcode_field)
        if postcode is None or pd.isna(postcode):
            skipped_count += 1
            continue

        postcode_normalized = str(postcode).strip().upper()
        geocode_result = geocode_results.get(postcode_normalized)

        if geocode_result is None:
            skipped_count += 1
            continue

        latitude = geocode_result.get("latitude")
        longitude = geocode_result.get("longitude")

        if latitude is None or longitude is None:
            skipped_count += 1
            continue

        # Build update document
        update_doc = {
            "latitude": latitude,
            "longitude": longitude,
            LOCATION_FIELD: {
                "type": "Point",
                "coordinates": [longitude, latitude],
            },
        }

        if geocode_result.get("x_coordinate") is not None and geocode_result.get("y_coordinate") is not None:
            update_doc["x_coordinate"] = geocode_result.get("x_coordinate")
            update_doc["y_coordinate"] = geocode_result.get("y_coordinate")

        # # Add rgn from region (uppercase) if present
        # region = geocode_result.get("region")
        # if region:
        #     update_doc["rgn"] = str(region).upper()
        #
        # # Add post_town (uppercase) if present
        # post_town = geocode_result.get("post_town")
        # if post_town:
        #     update_doc["post_town"] = str(post_town).upper()

        operations.append(
            UpdateMany(
                {"uid": uid},
                {"$set": update_doc},
            )
        )
        update_count += 1

    # Execute bulk operations
    if operations:
        try:
            collection.bulk_write(operations, ordered=False)
        except BulkWriteError as e:
            logger.warning(
                f"Bulk write error (some operations may have succeeded): {e.details}"
            )

    return {"updates": update_count, "skipped": skipped_count}



def update_mongo_from_not_found_csv(
    csv_path: str,
    database_name: str="leases",
    collection_name: str="leases",
    connection_string: str = "mongodb://localhost:27017",
    chunk_size: int = 10000,
    uid_field: str = "uid",
    postcode_field: str = "pc",
    progress_file: Optional[str] = None,
    cache_file: Optional[str] = None,
) -> dict:
    """
    Update MongoDB documents from a not_found CSV file using postcodes.io API.

    For each record with a valid 'pc' (postcode) column value, geocode it using
    postcodes.io API and update the matching MongoDB document with:
    - latitude: from postcodes.io
    - longitude: from postcodes.io
    - location: GeoJSON Point object
    - rgn: region from postcodes.io (uppercase)
    - post_town: admin_district from postcodes.io (uppercase)

    Uses postcode caching to minimize API calls for 1.2M+ records.

    Args:
        csv_path: Path to the not_found CSV file
        database_name: MongoDB database name
        collection_name: MongoDB collection name
        connection_string: MongoDB connection string
        chunk_size: Number of rows to process per chunk
        uid_field: Name of the UID column in CSV
        postcode_field: Name of the postcode column in CSV
        progress_file: Optional file to save progress
        cache_file: Optional file to persist postcode cache (JSON)

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

    # Initialize postcode cache (with optional disk persistence)
    if cache_file is None:
        cache_file = str(Path(csv_path).parent / "postcode_cache.json")
    logger.info(f"Initializing postcode cache (file: {cache_file})...")
    cache = PostcodeCache(cache_file=cache_file)

    # Create requests session for connection pooling
    session = requests.Session()
    session.headers.update({
        "Content-Type": "application/json",
        "Accept": "application/json",
    })

    # Initialize MongoDB connection
    client = MongoDBClient(
        connection_string=connection_string,
        database_name=database_name,
    )

    total_updates = 0
    total_skipped = 0
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

            with tqdm(total=total_rows, desc="Processing not_found", unit="rows") as pbar:
                for chunk in chunks:
                    result = process_not_found_chunk(
                        chunk, collection, cache, session, uid_field, postcode_field
                    )

                    total_updates += result["updates"]
                    total_skipped += result["skipped"]
                    processed_rows += len(chunk)

                    # Update progress bar
                    pbar.update(len(chunk))
                    pbar.set_postfix({
                        "updates": total_updates,
                        "skipped": total_skipped,
                        "cache_hit": cache.stats["hit_rate"],
                    })

                    # Save progress to file if specified
                    if progress_file:
                        with open(progress_file, "w") as f:
                            f.write(
                                f"processed={processed_rows}\n"
                                f"total={total_rows}\n"
                                f"updates={total_updates}\n"
                                f"skipped={total_skipped}\n"
                                f"cache_stats={cache.stats}\n"
                            )

                    # Periodically save cache to disk
                    if processed_rows % 100000 == 0:
                        cache.save_cache()

    except Exception as e:
        logger.error(f"Error processing not_found CSV: {e}")
        # Save cache before re-raising
        cache.save_cache()
        raise
    finally:
        session.close()
        # Always save cache at the end
        cache.save_cache()

    total_time = time.time() - start_time
    logger.info(
        f"Completed in {total_time/60:.1f} minutes | "
        f"Total updates: {total_updates:,} | Total skipped: {total_skipped:,} | "
        f"Cache stats: {cache.stats}"
    )

    return {
        "total_rows": processed_rows,
        "updates": total_updates,
        "skipped": total_skipped,
        "elapsed_seconds": total_time,
        "cache_stats": cache.stats,
    }


def main():
    """Main entry point to start the MongoDB update from CSV."""
    project_dir = Path(__file__).resolve().parent.parent.parent
    csv_path = project_dir / "data" / "found_addresses.csv"

    result = update_mongo_from_found_csv(
        csv_path=str(csv_path),
        database_name="leases",
        collection_name="leases",
    )
    logger.info(f"Final result: {result}")

    notfound_csv_path = project_dir / "data" / "not_found.csv"
    update_mongo_from_not_found_csv(
        csv_path=str(notfound_csv_path), )


if __name__ == "__main__":
    main()



