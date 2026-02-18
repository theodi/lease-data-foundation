"""
Address matching script that matches MongoDB addresses to PostgreSQL ab_plus table.

This script processes MongoDB documents in batches, parses addresses,
and matches them against the PostgreSQL ab_plus table. Results are written
to CSV files, with support for resuming after failures.
"""

import csv
import os
import re
import logging
from typing import Optional, Generator
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from psycopg2 import pool
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

MONGO_BATCH_SIZE = 50000


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

    # Optimize connection for bulk operations
    with conn.cursor() as cur:
        cur.execute("SET work_mem = '256MB'")
        cur.execute("SET maintenance_work_mem = '512MB'")
        cur.execute("SET synchronous_commit = OFF")  # Faster for bulk reads
    conn.commit()

    return conn


def create_connection_pool(min_conn: int = 2, max_conn: int = 10):
    """Create a PostgreSQL connection pool for parallel processing."""
    return pool.ThreadedConnectionPool(min_conn, max_conn, **DB_CONFIG)


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
        not_found_writer.writerow(["uid", "apd_original", "apd", "pc", "uprn"])

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
        # {"uid": 1, "uprn":1, "rpd": 1, "pc": 1}
        {"uid": 1, "uprn":1, "apd": 1, "pc": 1}
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
    First tries UPRN matching (fastest), then falls back to address-based matching.
    Supports matching by postcode or by city/post_town when postcode is unavailable.

    Args:
        pg_cursor: PostgreSQL cursor
        records: List of records with uid, house_number, road, pc, uprn, and optionally city

    Returns:
        Tuple of (found_records, not_found_records)
    """
    if not records:
        return [], []

    found_records = []
    remaining_records = records

    # Step 1: Try UPRN matching first (fastest approach)
    uprn_records = [r for r in remaining_records if r.get("uprn")]
    non_uprn_records = [r for r in remaining_records if not r.get("uprn")]

    if uprn_records:
        uprn_found, uprn_not_found = _batch_lookup_by_uprn(pg_cursor, uprn_records)
        found_records.extend(uprn_found)
        # Records not found by UPRN will be tried with address matching
        remaining_records = uprn_not_found + non_uprn_records
    else:
        remaining_records = non_uprn_records

    # Step 2: Separate remaining records by matching strategy
    postcode_records = [r for r in remaining_records if r.get("pc")]
    city_records = [r for r in remaining_records if not r.get("pc") and r.get("city")]
    no_match_records = [r for r in remaining_records if not r.get("pc") and not r.get("city")]

    not_found_records = list(no_match_records)  # These cannot be matched

    # Step 3: Process postcode-based lookups
    if postcode_records:
        postcode_found, postcode_not_found = _batch_lookup_by_postcode(pg_cursor, postcode_records)
        found_records.extend(postcode_found)
        not_found_records.extend(postcode_not_found)

    # Step 4: Process city-based lookups (using post_town)
    if city_records:
        city_found, city_not_found = _batch_lookup_by_city(pg_cursor, city_records)
        found_records.extend(city_found)
        not_found_records.extend(city_not_found)

    return found_records, not_found_records


def _batch_lookup_by_uprn(
    pg_cursor,
    records: list[dict]
) -> tuple[list[dict], list[dict]]:
    """
    Perform batch lookup of addresses by UPRN (fastest matching method).

    Args:
        pg_cursor: PostgreSQL cursor
        records: List of records with uprn field

    Returns:
        Tuple of (found_records, not_found_records)
    """
    # Prepare data for batch query
    lookup_data = []
    record_map = {}

    for rec in records:
        uprn = rec["uprn"]
        lookup_data.append((rec["uid"], uprn))
        if uprn not in record_map:
            record_map[uprn] = []
        record_map[uprn].append(rec)

    # Remove duplicates for the query (keep unique UPRNs)
    unique_uprns = list(set(rec["uprn"] for rec in records))

    if not unique_uprns:
        return [], records

    # Create temporary table for batch lookup
    pg_cursor.execute("""
        CREATE TEMP TABLE IF NOT EXISTS lookup_batch_uprn (
            uprn BIGINT
        ) ON COMMIT DELETE ROWS
    """)

    # Clear and insert lookup data
    pg_cursor.execute("TRUNCATE lookup_batch_uprn")

    execute_values(
        pg_cursor,
        "INSERT INTO lookup_batch_uprn (uprn) VALUES %s",
        [(int(u),) for u in unique_uprns if u and str(u).isdigit()],
        page_size=1000
    )

    # Perform batch lookup by UPRN
    pg_cursor.execute("""
        SELECT 
            lb.uprn as lookup_uprn,
            ab.*
        FROM lookup_batch_uprn lb
        JOIN ab_plus ab ON ab.uprn = lb.uprn
    """)

    found_by_uprn = {
        row["lookup_uprn"]: dict(row)
        for row in pg_cursor.fetchall()
    }

    found_records = []
    not_found_records = []

    for rec in records:
        uprn = rec["uprn"]
        # Convert to int for lookup since keys are BIGINT
        uprn_key = int(uprn) if uprn and str(uprn).isdigit() else None
        if uprn_key and uprn_key in found_by_uprn:
            result = found_by_uprn[uprn_key].copy()
            # Remove lookup columns
            result.pop("lookup_uprn", None)
            result["uid"] = rec["uid"]
            result["original_apd"] = rec["apd"]
            result["uprn"] = rec["uprn"]
            found_records.append(result)
        else:
            not_found_records.append(rec)

    return found_records, not_found_records



def _batch_lookup_by_postcode(
    pg_cursor,
    records: list[dict]
) -> tuple[list[dict], list[dict]]:
    """
    Perform batch lookup of addresses by postcode.

    Optimized to use combined queries with UNION ALL to reduce round-trips.

    Args:
        pg_cursor: PostgreSQL cursor
        records: List of records with postcode

    Returns:
        Tuple of (found_records, not_found_records)
    """
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

    # Prepare data with base numbers for fuzzy matching upfront
    lookup_with_base = []
    for house_num, road, pc in unique_lookups:
        base_num = extract_base_number(house_num)
        lookup_with_base.append((house_num, base_num, road, pc))

    # Create temporary table with all lookup data including base numbers
    pg_cursor.execute("""
        CREATE TEMP TABLE IF NOT EXISTS lookup_batch_pc (
            house_number TEXT,
            base_number TEXT,
            road TEXT,
            postcode TEXT
        ) ON COMMIT DELETE ROWS
    """)

    pg_cursor.execute("TRUNCATE lookup_batch_pc")

    execute_values(
        pg_cursor,
        "INSERT INTO lookup_batch_pc (house_number, base_number, road, postcode) VALUES %s",
        lookup_with_base,
        page_size=5000
    )

    # Single combined query: exact building_number OR exact building_name OR fuzzy matches
    # Priority: 1=exact number, 2=exact name, 3=fuzzy number, 4=fuzzy name
    pg_cursor.execute("""
        WITH ranked_matches AS (
            SELECT 
                lb.house_number as lookup_house_number,
                lb.road as lookup_road,
                lb.postcode as lookup_postcode,
                ab.*,
                CASE
                    WHEN UPPER(ab.building_number) = UPPER(lb.house_number) THEN 1
                    WHEN UPPER(ab.building_name) = UPPER(lb.house_number) THEN 2
                    WHEN UPPER(ab.building_number) = UPPER(lb.base_number) 
                         OR UPPER(ab.building_number) LIKE UPPER(lb.base_number) || '%' THEN 3
                    WHEN UPPER(ab.building_name) = UPPER(lb.base_number)
                         OR UPPER(ab.building_name) LIKE UPPER(lb.base_number) || '%' THEN 4
                END as match_priority
            FROM lookup_batch_pc lb
            JOIN ab_plus ab ON
                UPPER(ab.thoroughfare) = UPPER(lb.road) AND
                UPPER(ab.postcode) = UPPER(lb.postcode) AND
                (
                    UPPER(ab.building_number) = UPPER(lb.house_number) OR
                    UPPER(ab.building_name) = UPPER(lb.house_number) OR
                    UPPER(ab.building_number) = UPPER(lb.base_number) OR
                    UPPER(ab.building_number) LIKE UPPER(lb.base_number) || '%' OR
                    UPPER(ab.building_name) = UPPER(lb.base_number) OR
                    UPPER(ab.building_name) LIKE UPPER(lb.base_number) || '%'
                )
        )
        SELECT DISTINCT ON (lookup_house_number, lookup_road, lookup_postcode)
            *
        FROM ranked_matches
        ORDER BY lookup_house_number, lookup_road, lookup_postcode, match_priority
    """)

    all_found = {
        (row["lookup_house_number"], row["lookup_road"], row["lookup_postcode"]): dict(row)
        for row in pg_cursor.fetchall()
    }

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
            result.pop("match_priority", None)
            result["uid"] = rec["uid"]
            result["original_apd"] = rec["apd"]
            result["uprn"] = rec["uprn"]
            found_records.append(result)
        else:
            not_found_records.append(rec)

    return found_records, not_found_records


def _batch_lookup_by_city(
    pg_cursor,
    records: list[dict]
) -> tuple[list[dict], list[dict]]:
    """
    Perform batch lookup of addresses by city (matching against post_town).

    Used when postcode is not available but city is.
    Optimized to use combined query with ranking.

    Args:
        pg_cursor: PostgreSQL cursor
        records: List of records with city (no postcode)

    Returns:
        Tuple of (found_records, not_found_records)
    """
    # Prepare data for batch query
    lookup_data = []
    record_map = {}

    for rec in records:
        key = (rec["house_number"], rec["road"], rec["city"])
        lookup_data.append(key)
        if key not in record_map:
            record_map[key] = []
        record_map[key].append(rec)

    # Remove duplicates for the query
    unique_lookups = list(set(lookup_data))

    if not unique_lookups:
        return [], records

    # Prepare data with base numbers for fuzzy matching upfront
    lookup_with_base = []
    for house_num, road, city in unique_lookups:
        base_num = extract_base_number(house_num)
        lookup_with_base.append((house_num, base_num, road, city))

    # Create temporary table with all lookup data including base numbers
    pg_cursor.execute("""
        CREATE TEMP TABLE IF NOT EXISTS lookup_batch_city (
            house_number TEXT,
            base_number TEXT,
            road TEXT,
            city TEXT
        ) ON COMMIT DELETE ROWS
    """)

    pg_cursor.execute("TRUNCATE lookup_batch_city")

    execute_values(
        pg_cursor,
        "INSERT INTO lookup_batch_city (house_number, base_number, road, city) VALUES %s",
        lookup_with_base,
        page_size=5000
    )

    # Single combined query: exact building_number OR exact building_name OR fuzzy matches
    # Priority: 1=exact number, 2=exact name, 3=fuzzy number, 4=fuzzy name
    pg_cursor.execute("""
        WITH ranked_matches AS (
            SELECT 
                lb.house_number as lookup_house_number,
                lb.road as lookup_road,
                lb.city as lookup_city,
                ab.*,
                CASE
                    WHEN UPPER(ab.building_number) = UPPER(lb.house_number) THEN 1
                    WHEN UPPER(ab.building_name) = UPPER(lb.house_number) THEN 2
                    WHEN UPPER(ab.building_number) = UPPER(lb.base_number) 
                         OR UPPER(ab.building_number) LIKE UPPER(lb.base_number) || '%' THEN 3
                    WHEN UPPER(ab.building_name) = UPPER(lb.base_number)
                         OR UPPER(ab.building_name) LIKE UPPER(lb.base_number) || '%' THEN 4
                END as match_priority
            FROM lookup_batch_city lb
            JOIN ab_plus ab ON
                UPPER(ab.thoroughfare) = UPPER(lb.road) AND
                UPPER(ab.post_town) = UPPER(lb.city) AND
                (
                    UPPER(ab.building_number) = UPPER(lb.house_number) OR
                    UPPER(ab.building_name) = UPPER(lb.house_number) OR
                    UPPER(ab.building_number) = UPPER(lb.base_number) OR
                    UPPER(ab.building_number) LIKE UPPER(lb.base_number) || '%' OR
                    UPPER(ab.building_name) = UPPER(lb.base_number) OR
                    UPPER(ab.building_name) LIKE UPPER(lb.base_number) || '%'
                )
        )
        SELECT DISTINCT ON (lookup_house_number, lookup_road, lookup_city)
            *
        FROM ranked_matches
        ORDER BY lookup_house_number, lookup_road, lookup_city, match_priority
    """)

    all_found = {
        (row["lookup_house_number"], row["lookup_road"], row["lookup_city"]): dict(row)
        for row in pg_cursor.fetchall()
    }

    found_records = []
    not_found_records = []

    for rec in records:
        key = (rec["house_number"], rec["road"], rec["city"])
        if key in all_found:
            result = all_found[key].copy()
            # Remove lookup columns
            result.pop("lookup_house_number", None)
            result.pop("lookup_road", None)
            result.pop("lookup_city", None)
            result.pop("match_priority", None)
            result["uid"] = rec["uid"]
            result["original_apd"] = rec["apd"]
            result["uprn"] = rec["uprn"]
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
        # apd_original = str(doc.get("rpd", "")).strip()
        apd_original = str(doc.get("apd", "")).strip()
        apd = normalise_address(apd_original)
        pc = str(doc.get("pc", "")).strip()
        uprn = str(doc.get("uprn", "")).strip()

        if not apd:
            parse_errors.append({"uid": uid, "apd_original": apd_original, "apd": apd, "pc": pc, "uprn": uprn})
            continue

        try:
            parsed = parse_address_string(apd)
            house_number = parsed.get("house_number", "").strip()
            house = parsed.get("house", "").strip()
            if not house_number:
                if house != "" and pc != "":
                    # Fallback to "house" label if "house_number" is not found (some addresses might be parsed differently)
                    house_number = house
                else:
                    # parsing might have failed to extract house number, try parsing the original unnormalised address as a last resort
                    parsed = parse_address_string(apd_original)
                    house_number = parsed.get("house_number", parsed.get("house", "")).strip()

            postcode = parsed.get("postcode", "").strip()
            city = parsed.get("city", "").strip()
            road = parsed.get("road", "").strip()

            if not house_number or not road:
                parse_errors.append({"uid": uid, "apd_original": apd_original, "apd": apd, "pc": pc, "uprn": uprn})
                continue

            # Determine the final postcode value
            final_postcode = postcode or pc

            # If no postcode available, use city for matching with post_town
            use_city_match = not final_postcode and city

            records.append({
                "uid": uid,
                "apd_original": apd_original,
                "apd": apd,
                "pc": final_postcode,
                "city": city if use_city_match else None,
                "house_number": normalise_house_number(house_number),
                "road": road,
                "uprn": uprn
            })
        except Exception as e:
            logger.warning(f"Failed to parse address '{apd}': {e}")
            parse_errors.append({"uid": uid, "apd_original": apd_original, "apd": apd, "pc": pc, "uprn": uprn})

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
        not_found_writer.writerow([rec["uid"], rec["apd_original"], rec["apd"], rec["pc"], rec.get("uprn", "")])

    return len(found_records), len(not_found_records), found_header_written

def normalise_address(address: str) -> str:
    """
    Normalise address string for better parsing.

    Args:
        address: The original address string
    """
    normalized_address = address.strip()
    # remove flat related info which can interfere with parsing
    if address.count(',') > 1:
        parts = [part.strip() for part in address.split(',')]
        normalized_address =', '.join(parts[1:])

    return normalized_address

def normalise_house_number(house_number: str) -> str:
    """
    Normalise house number for better matching.

    Args:
        house_number: The original house number string
    """
    normalized_house_number = house_number.strip()
    if "-" in normalized_house_number:
        # 153-157 NEW BOND STREET -> 153 NEW BOND STREET
        normalized_house_number = normalized_house_number.split("-")[0].strip()
    # if house number contains letters, remove them (e.g. 3B -> 3)
    # if any(char.isalpha() for char in normalized_house_number):
    #     normalized_house_number = re.sub(r'[A-Za-z]', '', normalized_house_number)
    return normalized_house_number


def extract_base_number(house_number: str) -> str:
    """
    Extract the base numeric part from a house number.

    Examples:
        "85A" -> "85"
        "1" -> "1"
        "153-157" -> "153"
        "3B" -> "3"

    Args:
        house_number: The house number string

    Returns:
        The base numeric part of the house number
    """
    # First handle ranges (e.g., 153-157 -> 153)
    if "-" in house_number:
        house_number = house_number.split("-")[0].strip()

    # Extract leading digits (e.g., 85A -> 85, 3B -> 3)
    match = re.match(r'^(\d+)', house_number)
    if match:
        return match.group(1)

    return house_number

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
    Create indexes on ab_plus table for faster lookups if not exists.
    :param pg_conn:
    :param pg_cursor:
    :return:
    """
    try:
        # Primary lookup indexes for postcode-based matching
        pg_cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_ab_plus_building_number_lookup 
            ON ab_plus (UPPER(building_number), UPPER(thoroughfare), UPPER(postcode));
        """)
        pg_cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_ab_plus_building_name_lookup 
            ON ab_plus (UPPER(building_name), UPPER(thoroughfare), UPPER(postcode));
        """)

        # UPRN index for fast UPRN-based lookups (most efficient matching method)
        pg_cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_ab_plus_uprn 
            ON ab_plus (uprn);
        """)

        # City/post_town based lookup indexes for addresses without postcodes
        pg_cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_ab_plus_building_number_city_lookup 
            ON ab_plus (UPPER(building_number), UPPER(thoroughfare), UPPER(post_town));
        """)
        pg_cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_ab_plus_building_name_city_lookup 
            ON ab_plus (UPPER(building_name), UPPER(thoroughfare), UPPER(post_town));
        """)

        # Covering index for faster postcode + road lookups (reduces table scans)
        pg_cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_ab_plus_postcode_road 
            ON ab_plus (UPPER(postcode), UPPER(thoroughfare));
        """)

        pg_conn.commit()
        logger.info("PostgreSQL indexes verified")
    except Exception as e:
        logger.warning(f"Could not create indexes: {e}")
        pg_conn.rollback()


def post_process_duplicate_uids():
    """
    Post-process CSV files to handle duplicate UIDs.

    When multiple MongoDB documents share the same UID, some may be matched
    and some may not. This function:
    1. Reads found_addresses.csv to get all matched UIDs and their data
    2. Reads not_found.csv to find records with UIDs that have matches
    3. Removes those records from not_found.csv
    4. Adds them to found_addresses.csv with the matched data

    This ensures that if any document with a given UID is matched,
    all documents with that UID are considered matched.
    """
    logger.info("Starting post-processing for duplicate UIDs")

    # Check if both files exist
    if not os.path.exists(FOUND_CSV):
        logger.warning(f"Found CSV file does not exist: {FOUND_CSV}")
        return

    if not os.path.exists(NOT_FOUND_CSV):
        logger.warning(f"Not found CSV file does not exist: {NOT_FOUND_CSV}")
        return

    # Step 1: Read found_addresses.csv and build a map of uid -> first matched record
    uid_to_found_data = {}
    found_headers = None
    found_records = []

    with open(FOUND_CSV, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        found_headers = next(reader, None)
        if not found_headers:
            logger.warning("Found CSV has no headers")
            return

        # Find the index of 'uid' column
        try:
            uid_idx = found_headers.index("uid")
        except ValueError:
            logger.error("'uid' column not found in found_addresses.csv")
            return

        for row in reader:
            if len(row) > uid_idx:
                uid = row[uid_idx]
                found_records.append(row)
                # Store the first occurrence of each UID
                if uid not in uid_to_found_data:
                    uid_to_found_data[uid] = row

    logger.info(f"Loaded {len(found_records)} found records with {len(uid_to_found_data)} unique UIDs")

    # Step 2: Read not_found.csv and separate records
    not_found_headers = None
    remaining_not_found = []
    records_to_move = []

    with open(NOT_FOUND_CSV, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        not_found_headers = next(reader, None)
        if not not_found_headers:
            logger.warning("Not found CSV has no headers")
            return

        # Find the index of 'uid' column in not_found
        try:
            nf_uid_idx = not_found_headers.index("uid")
        except ValueError:
            logger.error("'uid' column not found in not_found.csv")
            return

        for row in reader:
            if len(row) > nf_uid_idx:
                uid = row[nf_uid_idx]
                if uid in uid_to_found_data:
                    # This UID has a match - create a new found record
                    # Copy the matched data but update original_apd with this record's info
                    matched_data = uid_to_found_data[uid].copy()

                    # Find original_apd index in found headers and apd_original in not_found
                    try:
                        original_apd_idx = found_headers.index("original_apd")
                        nf_apd_original_idx = not_found_headers.index("apd_original")
                        # Update original_apd with this record's address
                        matched_data[original_apd_idx] = row[nf_apd_original_idx]
                    except ValueError:
                        pass  # Keep the original apd if columns not found

                    records_to_move.append(matched_data)
                else:
                    remaining_not_found.append(row)

    if not records_to_move:
        logger.info("No duplicate UID records to move")
        return

    logger.info(f"Moving {len(records_to_move)} records from not_found to found")

    # Step 3: Append moved records to found_addresses.csv
    with open(FOUND_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for row in records_to_move:
            writer.writerow(row)

    # Step 4: Rewrite not_found.csv without the moved records
    with open(NOT_FOUND_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(not_found_headers)
        for row in remaining_not_found:
            writer.writerow(row)

    logger.info(f"Post-processing complete. Moved {len(records_to_move)} records. "
                f"Remaining not found: {len(remaining_not_found)}")


if __name__ == "__main__":
    main()
    # Run post-processing after main matching is complete
    post_process_duplicate_uids()

