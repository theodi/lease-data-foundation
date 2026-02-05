"""
Address matching script that matches MongoDB addresses to PostgreSQL ab_plus table.

This script processes MongoDB documents in batches, parses addresses,
and matches them against the PostgreSQL ab_plus table. Results are written
to CSV files, with support for resuming after failures.
"""

import csv
import os
import logging
from typing import Optional, Generator
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from dotenv import load_dotenv
from tqdm import tqdm

from src.utils.mongo_client import MongoDBClient
from src.addressbase.address_parser import parse_address_string

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("address_matching.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# PostgreSQL configuration
DB_CONFIG = {
    "user": "postgres",
    "password": os.getenv("DB_PASSWORD"),
    "host": "localhost",
    "port": "5432",
    "dbname": "address_base"
}

# File paths
CURRENT_FOLDER = Path(__file__).parent
DATA_FOLDER = CURRENT_FOLDER / "../../data"
FOUND_CSV = os.path.join(DATA_FOLDER, "found_addresses.csv")
NOT_FOUND_CSV = os.path.join(DATA_FOLDER, "not_found.csv")
PROGRESS_FILE = os.path.join(DATA_FOLDER, "matching_progress.txt")

# Batch sizes for efficiency
MONGO_BATCH_SIZE = 10000
POSTGRES_BATCH_SIZE = 1000


def get_last_processed_uid() -> Optional[str]:
    """Get the last processed UID from progress file for resume capability."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return f.read().strip() or None
    return None


def save_progress(uid: str) -> None:
    """Save the last processed UID to progress file."""
    with open(PROGRESS_FILE, "w") as f:
        f.write(uid)


def get_postgres_connection():
    """Create and return a PostgreSQL connection with optimized settings."""
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def create_csv_writers():
    """Create CSV writers for found and not found addresses."""
    found_file_exists = os.path.exists(FOUND_CSV)
    not_found_file_exists = os.path.exists(NOT_FOUND_CSV)

    found_file = open(FOUND_CSV, "a", newline="", encoding="utf-8")
    not_found_file = open(NOT_FOUND_CSV, "a", newline="", encoding="utf-8")

    found_writer = csv.writer(found_file)
    not_found_writer = csv.writer(not_found_file)

    # Write headers if files are new
    if not found_file_exists:
        # We'll write the header once we know the ab_plus columns
        pass
    if not not_found_file_exists:
        not_found_writer.writerow(["uid", "apd", "pc"])

    return found_file, not_found_file, found_writer, not_found_writer, found_file_exists


def fetch_mongo_documents(
    collection,
    last_uid: Optional[str] = None,
    batch_size: int = MONGO_BATCH_SIZE
) -> Generator:
    """
    Fetch documents from MongoDB in batches using cursor-based pagination.

    Args:
        collection: MongoDB collection
        last_uid: Last processed UID for resume capability
        batch_size: Number of documents to fetch per batch

    Yields:
        Batches of documents
    """
    query = {}
    if last_uid:
        # Use _id for ordering since uid is a hex string, not incremental
        # We need to find the _id of the last processed document
        last_doc = collection.find_one({"uid": last_uid})
        if last_doc:
            query = {"_id": {"$gt": last_doc["_id"]}}

    # Sort by _id for consistent ordering and efficient resume
    # _id is always monotonically increasing in MongoDB
    cursor = collection.find(
        query,
        {"uid": 1, "apd": 1, "pc": 1}
    ).sort("_id", 1).batch_size(batch_size)

    batch = []
    for doc in cursor:
        batch.append(doc)
        if len(batch) >= batch_size:
            yield batch
            batch = []

    if batch:
        yield batch


def batch_lookup_addresses(
    pg_cursor,
    records: list[dict]
) -> tuple[list[dict], list[dict]]:
    """
    Perform batch lookup of addresses in PostgreSQL.

    Uses a single query with UNNEST for efficient batch lookups.

    Args:
        pg_cursor: PostgreSQL cursor
        records: List of records with uid, house_number, road, pc

    Returns:
        Tuple of (found_records, not_found_records)
    """
    if not records:
        return [], []

    # Prepare data for batch query
    lookup_data = []
    record_map = {}

    for rec in records:
        key = (rec["house_number"], rec["road"], rec["pc"])
        lookup_data.append(key)
        if key not in record_map:
            record_map[key] = []
        record_map[key].append(rec)

    # Remove duplicates for the query
    unique_lookups = list(set(lookup_data))

    if not unique_lookups:
        return [], records

    # Create temporary table for batch lookup (more efficient for large batches)
    pg_cursor.execute("""
        CREATE TEMP TABLE IF NOT EXISTS lookup_batch (
            house_number TEXT,
            road TEXT,
            postcode TEXT
        ) ON COMMIT DELETE ROWS
    """)

    # Clear and insert lookup data
    pg_cursor.execute("TRUNCATE lookup_batch")

    execute_values(
        pg_cursor,
        "INSERT INTO lookup_batch (house_number, road, postcode) VALUES %s",
        unique_lookups,
        page_size=1000
    )

    # Perform batch lookup for building_number
    pg_cursor.execute("""
        SELECT DISTINCT ON (lb.house_number, lb.road, lb.postcode)
            lb.house_number as lookup_house_number,
            lb.road as lookup_road,
            lb.postcode as lookup_postcode,
            ab.*
        FROM lookup_batch lb
        JOIN ab_plus ab ON 
            UPPER(ab.building_number) = UPPER(lb.house_number) AND 
            UPPER(ab.thoroughfare) = UPPER(lb.road) AND 
            UPPER(ab.postcode) = UPPER(lb.postcode)
    """)

    found_by_number = {
        (row["lookup_house_number"], row["lookup_road"], row["lookup_postcode"]): dict(row)
        for row in pg_cursor.fetchall()
    }

    # Find keys not found by building_number
    not_found_keys = [k for k in unique_lookups if k not in found_by_number]

    # Perform batch lookup for building_name on remaining keys
    found_by_name = {}
    if not_found_keys:
        pg_cursor.execute("TRUNCATE lookup_batch")
        execute_values(
            pg_cursor,
            "INSERT INTO lookup_batch (house_number, road, postcode) VALUES %s",
            not_found_keys,
            page_size=1000
        )

        pg_cursor.execute("""
            SELECT DISTINCT ON (lb.house_number, lb.road, lb.postcode)
                lb.house_number as lookup_house_number,
                lb.road as lookup_road,
                lb.postcode as lookup_postcode,
                ab.*
            FROM lookup_batch lb
            JOIN ab_plus ab ON 
                UPPER(ab.building_name) = UPPER(lb.house_number) AND 
                UPPER(ab.thoroughfare) = UPPER(lb.road) AND 
                UPPER(ab.postcode) = UPPER(lb.postcode)
        """)

        found_by_name = {
            (row["lookup_house_number"], row["lookup_road"], row["lookup_postcode"]): dict(row)
            for row in pg_cursor.fetchall()
        }

    # Combine results
    all_found = {**found_by_number, **found_by_name}

    found_records = []
    not_found_records = []

    for rec in records:
        key = (rec["house_number"], rec["road"], rec["pc"])
        if key in all_found:
            result = all_found[key].copy()
            # Remove lookup columns
            result.pop("lookup_house_number", None)
            result.pop("lookup_road", None)
            result.pop("lookup_postcode", None)
            result["uid"] = rec["uid"]
            result["original_apd"] = rec["apd"]
            found_records.append(result)
        else:
            not_found_records.append(rec)

    return found_records, not_found_records


def process_batch(
    batch: list[dict],
    pg_cursor,
    found_writer,
    not_found_writer,
    found_header_written: bool
) -> tuple[int, int, bool]:
    """
    Process a batch of MongoDB documents.

    Args:
        batch: List of MongoDB documents
        pg_cursor: PostgreSQL cursor
        found_writer: CSV writer for found addresses
        not_found_writer: CSV writer for not found addresses
        found_header_written: Whether the found CSV header has been written

    Returns:
        Tuple of (found_count, not_found_count, found_header_written)
    """
    # Parse addresses and prepare records
    records = []
    parse_errors = []

    for doc in batch:
        uid = str(doc.get("uid", "")).strip()
        apd = str(doc.get("apd", "")).strip()
        pc = str(doc.get("pc", "")).strip()

        if not apd:
            parse_errors.append({"uid": uid, "apd": apd, "pc": pc})
            continue

        try:
            parsed = parse_address_string(apd)
            house_number = parsed.get("house_number", "").strip()
            postcode = parsed.get("postcode")
            road = parsed.get("road", "").strip()

            if not house_number or not road:
                parse_errors.append({"uid": uid, "apd": apd, "pc": pc})
                continue

            records.append({
                "uid": uid,
                "apd": apd,
                "pc": postcode or pc,
                "house_number": house_number,
                "road": road
            })
        except Exception as e:
            logger.warning(f"Failed to parse address '{apd}': {e}")
            parse_errors.append({"uid": uid, "apd": apd, "pc": pc})

    # Batch lookup in PostgreSQL
    found_records, not_found_records = batch_lookup_addresses(pg_cursor, records)

    # Add parse errors to not found
    not_found_records.extend(parse_errors)

    # Write found records
    for rec in found_records:
        if not found_header_written:
            # Write header from first record
            headers = list(rec.keys())
            found_writer.writerow(headers)
            found_header_written = True
        found_writer.writerow(list(rec.values()))

    # Write not found records
    for rec in not_found_records:
        not_found_writer.writerow([rec["uid"], rec["apd"], rec["pc"]])

    return len(found_records), len(not_found_records), found_header_written


def main(
    mongo_database: str = "leases",
    mongo_collection: str = "leases"
):
    """
    Main function to match MongoDB addresses to PostgreSQL ab_plus table.

    Args:
        mongo_database: Name of the MongoDB database
        mongo_collection: Name of the MongoDB collection
    """
    logger.info("Starting address matching process")

    # Get last processed UID for resume
    last_uid = get_last_processed_uid()
    if last_uid:
        logger.info(f"Resuming from UID: {last_uid}")

    # Initialize connections
    mongo_client = MongoDBClient(database_name=mongo_database)
    collection = mongo_client.get_collection(mongo_collection)

    # Get total count for progress bar
    total_count = collection.count_documents({})
    if last_uid:
        # Find the _id of the last processed document to count remaining
        last_doc = collection.find_one({"uid": last_uid})
        if last_doc:
            processed_count = collection.count_documents({"_id": {"$lte": last_doc["_id"]}})
            remaining_count = total_count - processed_count
        else:
            logger.warning(f"Could not find document with uid {last_uid}, starting from beginning")
            remaining_count = total_count
            last_uid = None
    else:
        remaining_count = total_count

    logger.info(f"Total documents: {total_count}, Remaining: {remaining_count}")

    # Connect to PostgreSQL
    pg_conn = get_postgres_connection()
    pg_cursor = pg_conn.cursor(cursor_factory=RealDictCursor)

    # Create index on lookup columns if not exists (run once)
    create_postgres_index(pg_conn, pg_cursor)

    # Open CSV files
    found_file, not_found_file, found_writer, not_found_writer, found_header_exists = create_csv_writers()
    found_header_written = found_header_exists

    total_found = 0
    total_not_found = 0
    last_processed_uid = None

    try:
        with tqdm(total=remaining_count, desc="Processing addresses") as pbar:
            for batch in fetch_mongo_documents(collection, last_uid, MONGO_BATCH_SIZE):
                try:
                    found_count, not_found_count, found_header_written = process_batch(
                        batch, pg_cursor, found_writer, not_found_writer, found_header_written
                    )

                    total_found += found_count
                    total_not_found += not_found_count

                    # Save progress after each batch
                    if batch:
                        last_processed_uid = str(batch[-1].get("uid", ""))
                        save_progress(last_processed_uid)

                    # Flush CSV files periodically
                    found_file.flush()
                    not_found_file.flush()

                    # Commit PostgreSQL transaction
                    pg_conn.commit()

                    pbar.update(len(batch))
                    pbar.set_postfix({
                        "found": total_found,
                        "not_found": total_not_found
                    })

                except Exception as e:
                    logger.error(f"Error processing batch: {e}")
                    pg_conn.rollback()
                    raise

    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
    finally:
        # Clean up
        found_file.close()
        not_found_file.close()
        pg_cursor.close()
        pg_conn.close()
        mongo_client.close()

        logger.info(f"Process completed. Found: {total_found}, Not Found: {total_not_found}")
        if last_processed_uid:
            logger.info(f"Last processed UID: {last_processed_uid}")


def create_postgres_index(pg_conn, pg_cursor):
    """
    Create index on ab_plus table for faster lookups if not exists.
    :param pg_conn:
    :param pg_cursor:
    :return:
    """
    try:
        pg_cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_ab_plus_building_number_lookup 
            ON ab_plus (UPPER(building_number), UPPER(thoroughfare), UPPER(postcode));
        """)
        pg_cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_ab_plus_building_name_lookup 
            ON ab_plus (UPPER(building_name), UPPER(thoroughfare), UPPER(postcode));
        """)
        pg_conn.commit()
        logger.info("PostgreSQL indexes verified")
    except Exception as e:
        logger.warning(f"Could not create indexes: {e}")
        pg_conn.rollback()


if __name__ == "__main__":
    main()

