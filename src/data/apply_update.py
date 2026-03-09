"""
Apply lease data updates from change CSV files.

Processes CSV files containing change indicators ('A' for add, 'D' for delete)
and applies them to the MongoDB lease database with dry-run support.
"""

import argparse
import csv
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, List, Any, Set

from dotenv import load_dotenv
from pymongo import InsertOne, UpdateOne, DeleteOne
from pymongo.errors import BulkWriteError
from bson import ObjectId
from tqdm import tqdm
import psycopg2
from psycopg2.extras import RealDictCursor

from src.addressbase.match_addresses import (
    parse_and_prepare_records,
    batch_lookup_addresses,
)
from src.utils.mongo_client import MongoDBClient
from src.main_regex_extractor import process_record as regex_process_record
from src.main_t5_extractor import initialize_t5_extractor

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        # logging.FileHandler("apply_update.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# PostgreSQL configuration
PG_CONFIG = {
    "user": os.getenv("POSTGRES_USERNAME", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "address_base")
}

# Residential classification codes
RESIDENTIAL_CLASSES = {"R", "X", "P"}

# Field mapping from CSV to MongoDB short keys
FIELD_MAP = {
    "Unique Identifier": "uid",
    "Register Property Description": "rpd",
    "County": "cty",
    "Region": "rgn",
    "Associated Property Description ID": "apid",
    "Associated Property Description": "apd",
    "OS UPRN": "uprn",
    "Price Paid": "ppd",
    "Reg Order": "ro",
    "Date of Lease": "dol",
    "Term": "term",
    "Alienation Clause Indicator": "aci",
}

# Enable debug mode via environment
DEBUG = False


def extract_postcode(row: Dict[str, str]) -> Optional[str]:
    """
    Extract postcode from property descriptions.

    Args:
        row: Original CSV row dictionary

    Returns:
        Extracted postcode or None
    """
    text = f"{row.get('Register Property Description', '')} {row.get('Associated Property Description', '')}"
    postcode_regex = r"\b[A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2}\b"
    match = re.search(postcode_regex, text, re.IGNORECASE)
    if match:
        postcode = match.group(0).upper()
        # Normalize spacing
        postcode = re.sub(r"\s+", " ", postcode).strip()
        return postcode
    return None


def map_row(original_row: Dict[str, str]) -> Dict[str, Any]:
    """
    Map CSV row to MongoDB document format.

    Args:
        original_row: Original CSV row with full field names

    Returns:
        Mapped row with short field names
    """
    mapped_row = {}
    for csv_key, db_key in FIELD_MAP.items():
        value = original_row.get(csv_key, "")
        if isinstance(value, str):
            value = value.strip()
            if value == "":
                continue
            if db_key in ['uprn', 'ro', 'ppd', 'apid'] and value.isdigit():
                value = int(value)
        mapped_row[db_key] = value

    # Add extracted postcode
    postcode = extract_postcode(original_row)
    if postcode:
        mapped_row["pc"] = postcode

    return mapped_row


def normalize_value(value: Any) -> str:
    """Normalize a value for comparison."""
    return str(value or "").strip()


def calculate_char_differences(
    original_row: Dict[str, str],
    db_record: Dict[str, Any]
) -> tuple[int, List[Dict[str, Any]]]:
    """
    Calculate character differences between CSV row and DB record.

    Args:
        original_row: Original CSV row
        db_record: Database record

    Returns:
        Tuple of (total_char_diffs, list of diff details)
    """
    total_char_diffs = 0
    diff_details = []

    for csv_key, db_key in FIELD_MAP.items():
        csv_val = str(original_row.get(csv_key, "")).strip()
        db_val = str(db_record.get(db_key, "")).strip()

        if csv_val != db_val:
            max_len = max(len(csv_val), len(db_val))
            char_diff = sum(1 for i in range(max_len) if (
                i >= len(csv_val) or i >= len(db_val) or csv_val[i] != db_val[i]
            ))
            total_char_diffs += char_diff
            diff_details.append({
                "field": csv_key,
                "csv_val": csv_val,
                "db_val": db_val,
                "char_diff": char_diff,
            })

    return total_char_diffs, diff_details


def prompt_user(question: str) -> str:
    """Prompt user for input and return lowercase trimmed answer."""
    return input(question).strip().lower()


def process_enrichment(add_rows: List[Dict[str, str]]) -> tuple[list[dict[str, Any] | None], int, int]:
    """
    Enrich records with extracted lease term data using regex and T5.

    First attempts regex extraction for all records. For records where regex fails,
    uses T5 model for extraction. Returns enriched records in the same order as input.

    Args:
        add_rows: List of CSV row dictionaries to enrich

    Returns:
        List of enriched dictionaries (same length and order as add_rows), percentage of valid term_parsings, percentage of successful AddressBase mappings
    """
    if not add_rows:
        return [], 0, 0

    total_records = len(add_rows)
    logger.info(f"📊 Enriching {total_records} records...")

    # Initialize result list with None placeholders
    enriched_records: List[Optional[Dict[str, Any]]] = [None] * total_records

    # Term string processing with regex and T5
    terms_processed_percentage = process_term_string(add_rows, enriched_records, total_records)
    mapped_records_percentage = map_to_addressbase(add_rows, enriched_records, total_records)

    # Fill any remaining None entries (shouldn't happen, but safety check)
    for idx in range(total_records):
        if enriched_records[idx] is None:
            original_row = add_rows[idx]
            mapped_row = map_row(original_row)
            enriched_records[idx] = {"uid": mapped_row["uid"]}

    return enriched_records, terms_processed_percentage, mapped_records_percentage


def process_term_string(add_rows: list[dict[str, str]], enriched_records: list[dict[str, Any] | None],
                        total_records: int) -> int:
    # Track records that need T5 processing
    t5_needed_indices = []
    t5_needed_records = []

    # Step 1: Process all records with regex
    logger.info("🔍 Processing records with regex extractor...")
    regex_valid_count = 0
    t5_valid_count = 0
    empty_term_count = 0

    for idx, row in enumerate(tqdm(add_rows, desc="Regex extraction", unit="rows")):
        # Map the row first to get the format expected by process_record
        mapped_row = map_row(row)
        enriched_record = {"uid": mapped_row["uid"]}

        # Process with regex extractor
        try:
            result = regex_process_record(mapped_row)
        except Exception as e:
            # logger.warning(f"❌ Regex extractor failed for record {idx} (uid: {mapped_row.get('uid', 'N/A')}): {e}")
            result = None

        if result and result.get("regex_is_valid"):
            result_copy = result.copy()
            for key in result_copy:
                if key != "regex_is_valid":
                    enriched_record[key] = result_copy[key]
            regex_valid_count += 1
        else:
            if mapped_row and mapped_row.get("term") and mapped_row["term"].strip():
                # Collect for T5 processing
                t5_needed_indices.append(idx)
                t5_needed_records.append(mapped_row)
            else:
                empty_term_count += 1
        enriched_records[idx] = enriched_record

    regex_percentage = (regex_valid_count / total_records * 100) if total_records > 0 else 0
    logger.info(f"✅ Regex extraction: {regex_valid_count}/{total_records} valid ({regex_percentage:.2f}%)")
    # logger.info(f"⚠️ {empty_term_count} records had empty term field and were skipped for T5 processing")

    # Step 2: Process remaining records with T5
    if t5_needed_records:
        logger.info(f"🤖 Processing {len(t5_needed_records)} records with T5 extractor...")

        try:
            # Initialize T5 extractor
            t5_extractor = initialize_t5_extractor()

            # Process in batch
            logger.info("🔄 Running T5 batch extraction (this step may take up to 10 minutes)...")
            t5_results = t5_extractor.extract_batch(t5_needed_records)

            # Process T5 results
            t5_valid_count = 0
            for idx_in_batch, (original_idx, mapped_row) in enumerate(zip(t5_needed_indices, t5_needed_records)):
                if idx_in_batch < len(t5_results):
                    result = t5_results[idx_in_batch]

                    if result and result.get("t5_is_valid"):
                        # Remove t5_is_valid key and merge with mapped row
                        result_copy = result.copy()
                        result_copy.pop("t5_is_valid", None)
                        result_copy["uid"] = mapped_row["uid"]

                        enriched_records[original_idx] = result_copy
                        t5_valid_count += 1
                    else:
                        # T5 failed, add minimal record with just uid
                        enriched_records[original_idx] = {"uid": mapped_row["uid"]}
                else:
                    # No result for this record, add minimal record
                    enriched_records[original_idx] = {"uid": mapped_row["uid"]}

            t5_percentage = (t5_valid_count / len(t5_needed_records) * 100) if t5_needed_records else 0
            logger.info(f"✅ T5 extraction: {t5_valid_count}/{len(t5_needed_records)} valid ({t5_percentage:.2f}%)")

        except Exception as e:
            logger.error(f"❌ T5 extraction failed: {e}")
            logger.info("⚠️ Filling remaining records with minimal data (uid only)...")

    # Calculate total term processing statistics
    total_valid = regex_valid_count + (t5_valid_count if t5_needed_records else 0)
    total_percentage = (total_valid / total_records * 100) if total_records > 0 else 0
    logger.info(f"📊 Total lease term extraction: {total_valid}/{total_records} valid ({total_percentage:.2f}%)")

    # iterate through enriched_records and rename keys
    for idx in range(total_records):
        if enriched_records[idx] is not None:
            record = enriched_records[idx]
            # Rename keys if they exist
            if "start_date" in record:
                record["st"] = record.pop("start_date")
            if "expiry_date" in record:
                record["exp"] = record.pop("expiry_date")
            if "tenure_years" in record:
                record["ty"] = int(record.pop("tenure_years"))
    return total_percentage


def map_to_addressbase(
    add_rows: List[Dict[str, str]],
    enriched_records: List[Optional[Dict[str, Any]]],
    total_records: int
) -> int:
    """
    Map records to AddressBase database and enrich with geographical data.

    Args:
        add_rows: Original CSV rows
        enriched_records: List of enriched records (modified in place)
        total_records: Total number of records

    Returns:
        Percentage of successfully mapped records
    """
    if not add_rows:
        return 0

    logger.info("🗺️ Mapping records to AddressBase...")

    # Connect to PostgreSQL
    try:
        pg_conn = psycopg2.connect(**PG_CONFIG)
        pg_cursor = pg_conn.cursor(cursor_factory=RealDictCursor)
        logger.info("✅ Connected to PostgreSQL AddressBase")
    except Exception as e:
        logger.error(f"❌ Failed to connect to PostgreSQL: {e}")
        logger.warning("⚠️ Skipping AddressBase mapping")
        return 0

    mapped_count = 0
    mapped_percentage = 0

    try:
        # Prepare batch for lookup - convert to format expected by parse_and_prepare_records
        batch = []
        for idx, row in enumerate(add_rows):
            # Get the mapped row data
            mapped_row = map_row(row)

            # Create document in the format expected by parse_and_prepare_records
            doc = {
                "uid": mapped_row["uid"],
                "apd": mapped_row["apd"],
                "pc": mapped_row["pc"],
                "uprn": mapped_row["uprn"]
            }
            batch.append(doc)

        # Parse addresses and prepare for lookup
        logger.info("🔍 Parsing addresses...")
        records_for_lookup, parse_errors = parse_and_prepare_records(batch)

        # Perform batch lookup
        logger.info(f"🔎 Looking up {len(records_for_lookup)} addresses in AddressBase...")
        found_records, not_found_records = batch_lookup_addresses(pg_cursor, records_for_lookup)

        logger.info(f"✅ AddressBase lookup: {len(found_records)} found, {len(not_found_records)} not found")

        # Create a mapping from uid to AddressBase data
        uid_to_ab_data = {}
        for rec in found_records:
            uid = rec.get("uid")
            if uid:
                # Extract relevant AddressBase fields
                ab_data = {
                    "aup": int(rec.get("uprn")) if rec.get("uprn") and str(rec.get("uprn")).isdigit() else rec.get("uprn"),
                    "bn": rec.get("building_number"),
                    "bnam": str(rec.get("building_name")).strip(),
                    "tf": str(rec.get("thoroughfare")).strip(),
                    "pt": str(rec.get("post_town")).strip(),
                    "apc": str(rec.get("postcode")).strip(),
                    "lat": rec.get("latitude"),
                    "lon": rec.get("longitude"),
                    "cl": str(rec.get("class")).strip(),
                    "ud": rec.get("udprn"),
                    "xc": rec.get("x_coordinate"),
                    "yc": rec.get("y_coordinate"),
                }
                if rec.get("latitude") and rec.get("longitude"):
                    # GeoJSON Point format: [longitude, latitude]
                    ab_data["loc"] = {
                        "type": "Point",
                        "coordinates": [float(rec.get("longitude")), float(rec.get("latitude"))]
                    }
                # Remove None values to keep documents clean
                ab_data = {k: v for k, v in ab_data.items() if v is not None and v != ""}
                uid_to_ab_data[uid] = ab_data

        # Merge AddressBase data into enriched_records
        for idx in range(total_records):
            if enriched_records[idx] is not None:
                uid = enriched_records[idx].get("uid")
                if uid in uid_to_ab_data:
                    # Merge AddressBase data into the existing enriched record
                    enriched_records[idx].update(uid_to_ab_data[uid])
                    mapped_count += 1
            else:
                # If record doesn't exist yet, create it with just the uid and AB data
                original_row = add_rows[idx]
                mapped_row = map_row(original_row)
                uid = mapped_row["uid"]

                if uid in uid_to_ab_data:
                    enriched_records[idx] = {**mapped_row, **uid_to_ab_data[uid]}
                    mapped_count += 1
                else:
                    enriched_records[idx] = mapped_row

        mapped_percentage = (mapped_count / total_records * 100) if total_records > 0 else 0
        logger.info(f"📊 AddressBase mapping: {mapped_count}/{total_records} records enriched ({mapped_percentage:.2f}%)")

    except Exception as e:
        logger.error(f"❌ Error during AddressBase mapping: {e}")
        logger.warning("⚠️ Continuing without AddressBase enrichment for remaining records")
    finally:
        pg_cursor.close()
        pg_conn.close()

    return mapped_percentage


def cascade_delete_leasesext(
    lease_ids: List[Any],
    leasesext_collection,
    dry_run: bool,
) -> int:
    """
    Perform cascade delete on leasesext collection.

    Deletes records from leasesext collection where 'lid' field matches
    any of the provided lease _ids.

    Args:
        lease_ids: List of lease document _ids to cascade delete
        leasesext_collection: MongoDB leasesext collection
        dry_run: Whether to run in dry-run mode

    Returns:
        Number of leasesext records deleted
    """
    if not lease_ids:
        return 0

    if dry_run:
        # In dry-run mode, just count how many would be deleted
        count = leasesext_collection.count_documents({"lid": {"$in": lease_ids}})
        if DEBUG and count > 0:
            logger.info(f"🔗 Would cascade delete {count} record(s) from leasesext")
        return count
    else:
        # Actually perform the cascade delete
        result = leasesext_collection.delete_many({"lid": {"$in": lease_ids}})
        if DEBUG and result.deleted_count > 0:
            logger.info(f"🔗 Cascade deleted {result.deleted_count} record(s) from leasesext")
        return result.deleted_count


def extract_version_from_filename(csv_filename: str) -> Optional[str]:
    """
    Extract version string from CSV filename.

    Args:
        csv_filename: Name of the CSV file

    Returns:
        Version string in format "YYYY-MM" or None
    """
    last_updated_match = re.search(r"(\d{4})_(\d{2})", csv_filename)
    return f"{last_updated_match.group(1)}-{last_updated_match.group(2)}" if last_updated_match else None


def read_csv_changes(csv_path: Path) -> tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """
    Read CSV file and separate delete and add rows.

    Args:
        csv_path: Path to the CSV file

    Returns:
        Tuple of (delete_rows, add_rows)
    """
    logger.info("📖 Reading CSV file...")
    delete_rows = []
    add_rows = []

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            indicator = row.get("Change Indicator", "").strip().upper()
            if indicator == "D":
                delete_rows.append(row)
            elif indicator == "A":
                add_rows.append(row)

    logger.info(f"To delete: {len(delete_rows)}")
    logger.info(f"To add: {len(add_rows)}")

    return delete_rows, add_rows


def process_deletions(
    delete_rows: List[Dict[str, str]],
    collection,
    lease_tracker_collection,
    dry_run: bool,
    last_updated: Optional[str],
    updated_uids: Set[str],
    leasesext_collection=None,
) -> tuple[int, int, int]:
    """
    Process all deletion operations in batches.

    Args:
        delete_rows: List of rows to delete
        collection: MongoDB collection
        lease_tracker_collection: LeaseTracker collection
        dry_run: Whether to run in dry-run mode
        last_updated: Version string for tracking
        updated_uids: Set of UIDs already updated
        leasesext_collection: Optional leasesext collection for cascade delete

    Returns:
        Tuple of (delete_count, ext_delete_count, unknown_count)
    """
    logger.info("PROCESSING DELETE" + (" (DRY-RUN)" if dry_run else ""))

    if not delete_rows:
        logger.info("DELETE COMPLETE (no rows to delete)")
        return 0, 0, 0

    delete_count = 0
    ext_delete_count = 0
    unknown_count = 0

    BATCH_SIZE = 500  # Process deletions in batches
    total_rows = len(delete_rows)

    for batch_start in tqdm(range(0, total_rows, BATCH_SIZE), desc="Deleting (batches)", unit="batch"):
        batch_end = min(batch_start + BATCH_SIZE, total_rows)
        batch_rows = delete_rows[batch_start:batch_end]

        # Process batch
        batch_deletes, batch_ext_deletes, batch_unknowns = process_delete_batch(
            batch_rows,
            collection,
            dry_run,
            last_updated,
            updated_uids,
            lease_tracker_collection,
            leasesext_collection,
        )

        delete_count += batch_deletes
        ext_delete_count += batch_ext_deletes
        unknown_count += batch_unknowns

    logger.info("DELETE COMPLETE")
    return delete_count, ext_delete_count, unknown_count


def process_delete_batch(
    batch_rows: List[Dict[str, str]],
    collection,
    dry_run: bool,
    last_updated: Optional[str],
    updated_uids: Set[str],
    lease_tracker_collection,
    leasesext_collection,
) -> tuple[int, int, int]:
    """
    Process a batch of delete operations efficiently.

    Args:
        batch_rows: Batch of CSV rows to delete
        collection: MongoDB collection (leases)
        dry_run: Whether to run in dry-run mode
        last_updated: Version string for tracking
        updated_uids: Set of UIDs already updated
        lease_tracker_collection: LeaseTracker collection
        leasesext_collection: Optional leasesext collection for cascade delete

    Returns:
        Tuple of (delete_count, leasesext_del_count, unknown_count)
    """
    # Map all rows and extract UIDs
    mapped_rows = [map_row(row) for row in batch_rows]
    uids = [row["uid"] for row in mapped_rows]

    # Batch fetch all records with these UIDs
    db_records = list(collection.find({"uid": {"$in": uids}}))

    # Create a lookup dictionary: uid -> list of db records
    uid_to_records: Dict[str, List[Dict[str, Any]]] = {}
    for record in db_records:
        uid = record.get("uid")
        if uid:
            if uid not in uid_to_records:
                uid_to_records[uid] = []
            uid_to_records[uid].append(record)

    # Process each deletion and collect operations
    delete_ops = []
    lease_tracker_ops = []
    lease_ids_to_cascade = []
    delete_count = 0
    unknown_count = 0
    ambiguous_cases = []

    for original_row, mapped_row in zip(batch_rows, mapped_rows):
        uid = mapped_row["uid"]
        ro = str(mapped_row["ro"])
        apid = str(mapped_row["apid"])

        # Get records for this UID
        db_matches = uid_to_records.get(uid, [])

        # Filter to candidates matching RO and APID
        candidate_matches = [
            record for record in db_matches
            if normalize_value(record.get("ro")) == ro
            and normalize_value(record.get("apid")) == apid
        ]

        if not candidate_matches:
            if DEBUG:
                logger.debug(f"❌ Delete UID {uid} — no matches for RO {ro} and APID {apid}")
            unknown_count += 1
            continue

        # If exactly one candidate, delete it directly
        if len(candidate_matches) == 1:
            if DEBUG:
                logger.info(f"🗑️ Deleting single candidate match for UID {uid}")
            lease_id = candidate_matches[0]["_id"]
            delete_ops.append(DeleteOne({"_id": lease_id}))
            lease_ids_to_cascade.append(lease_id)
            delete_count += 1

            # Schedule LeaseTracker update (batch it, don't execute immediately!)
            if last_updated and uid not in updated_uids:
                lease_tracker_ops.append(UpdateOne(
                    {"uid": uid},
                    {"$set": {"lastUpdated": last_updated}},
                    upsert=True,
                ))
                updated_uids.add(uid)
            continue

        # Check for exact matches across all fields
        exact_matches = []
        for db_record in candidate_matches:
            is_exact = all(
                normalize_value(original_row.get(csv_key)) == normalize_value(db_record.get(db_key))
                for csv_key, db_key in FIELD_MAP.items()
            )
            if is_exact:
                exact_matches.append(db_record)

        if exact_matches:
            # Exact match found - schedule for deletion
            ids = [doc["_id"] for doc in exact_matches]
            for doc_id in ids:
                delete_ops.append(DeleteOne({"_id": doc_id}))
                lease_ids_to_cascade.append(doc_id)
            delete_count += len(exact_matches)

            # Schedule LeaseTracker update
            if last_updated and uid not in updated_uids:
                lease_tracker_ops.append(UpdateOne(
                    {"uid": uid},
                    {"$set": {"lastUpdated": last_updated}},
                    upsert=True,
                ))
                updated_uids.add(uid)
            continue

        # No exact match - check character differences
        char_diff_details = []
        for record in candidate_matches:
            total_char_diffs, diff_details = calculate_char_differences(original_row, record)
            if diff_details:
                char_diff_details.append({
                    "_id": record["_id"],
                    "total_diffs": total_char_diffs,
                    "details": diff_details,
                })

        # If only 1 character difference total, treat as exact match
        total_char_diffs = sum(detail["total_diffs"] for detail in char_diff_details)
        if total_char_diffs == 1:
            ids = [doc["_id"] for doc in candidate_matches]
            for doc_id in ids:
                delete_ops.append(DeleteOne({"_id": doc_id}))
                lease_ids_to_cascade.append(doc_id)
            delete_count += len(candidate_matches)

            # Schedule LeaseTracker update
            if last_updated and uid not in updated_uids:
                lease_tracker_ops.append(UpdateOne(
                    {"uid": uid},
                    {"$set": {"lastUpdated": last_updated}},
                    upsert=True,
                ))
                updated_uids.add(uid)
            continue

        # Ambiguous case - store for later user interaction
        ambiguous_cases.append({
            "original_row": original_row,
            "mapped_row": mapped_row,
            "candidate_matches": candidate_matches,
            "char_diff_details": char_diff_details,
            "total_char_diffs": total_char_diffs,
        })

    # Handle ambiguous cases with user interaction (if any)
    for case in ambiguous_cases:
        uid = case["mapped_row"]["uid"]
        logger.warning(f"⚠️ Ambiguous deletion for UID {uid} — no exact match:")
        for detail in case["char_diff_details"]:
            logger.warning(f"   ⚠️ _id: {detail['_id']}")
            for diff in detail["details"]:
                logger.warning(
                    f"      🔸 {diff['field']}: \"{diff['csv_val']}\" ≠ \"{diff['db_val']}\" "
                    f"(char diff: {diff['char_diff']})"
                )
        logger.warning(f"   🔢 Total character differences: {case['total_char_diffs']}")

        choice = prompt_user("❓ [k]eep DB, [d]elete anyway, [s]kip? (k/d/s): ")

        if choice == "d":
            ids = [doc["_id"] for doc in case["candidate_matches"]]
            for doc_id in ids:
                delete_ops.append(DeleteOne({"_id": doc_id}))
                lease_ids_to_cascade.append(doc_id)
            delete_count += len(case["candidate_matches"])

            # Schedule LeaseTracker update
            if last_updated and uid not in updated_uids:
                lease_tracker_ops.append(UpdateOne(
                    {"uid": uid},
                    {"$set": {"lastUpdated": last_updated}},
                    upsert=True,
                ))
                updated_uids.add(uid)
        else:
            unknown_count += 1

    # Execute batch delete operations
    if delete_ops and not dry_run:
        try:
            collection.bulk_write(delete_ops, ordered=False)
            if DEBUG:
                logger.debug(f"✅ Batch deleted {len(delete_ops)} record(s)")
        except BulkWriteError as e:
            logger.warning(f"⚠️ Bulk delete error: {e.details}")
            # Count successful deletes
            delete_count -= len(e.details.get("writeErrors", []))

    # Execute batch LeaseTracker updates
    if lease_tracker_ops and not dry_run:
        try:
            lease_tracker_collection.bulk_write(lease_tracker_ops, ordered=False)
        except BulkWriteError as e:
            logger.warning(f"⚠️ LeaseTracker bulk update error: {e.details}")

    # Cascade delete from leasesext (in batch)
    ext_delete_count = 0
    if lease_ids_to_cascade:
        ext_delete_count = cascade_delete_leasesext(
            lease_ids_to_cascade,
            leasesext_collection,
            dry_run
        )

    return delete_count, ext_delete_count, unknown_count


def process_bulk_additions(
    add_rows: List[Dict[str, Any]],
    enriched_records: List[Dict[str, Any]],
    collection,
    lease_tracker_collection,
    dry_run: bool,
    last_updated: Optional[str],
    updated_uids: Set[str],
    leasesext_collection
) -> tuple[int, int]:
    """
    Process all addition operations in bulk for both leases and leasesext.

    Args:
        add_rows: List of rows to add
        enriched_records: List of enriched records with parsed lease terms and AddressBase data
        collection: MongoDB collection
        lease_tracker_collection: LeaseTracker collection
        dry_run: Whether to run in dry-run mode
        last_updated: Version string for tracking
        updated_uids: Set of UIDs already updated
        leasesext_collection: MongoDB leasesext collection

    Returns:
        Number of records added, number of commercial records skipped
    """
    logger.info("BULK ADDING" + (" (DRY-RUN)" if dry_run else ""))

    if dry_run:
        # Dry run: just count
        add_count = 0
        commercial_count = 0
        for idx, original_row in enumerate(tqdm(add_rows, desc="Adding (dry-run)", unit="rows")):
            enriched_record = enriched_records[idx]
            # Process only if we are sure it is not commercial (based on AddressBase class)
            if str(enriched_record.get("cl", "R")).strip().upper() in RESIDENTIAL_CLASSES:
                add_count += 1
            else:
                commercial_count += 1
        return add_count, commercial_count

    # Actual bulk add
    BATCH_SIZE = 1000
    batch = []
    enriched_batch = []
    lease_tracker_ops = []
    batch_count = 0
    commercial_count = 0

    for idx, original_row in enumerate(tqdm(add_rows, desc="Adding", unit="rows")):
        mapped_row = map_row(original_row)
        enriched_record = enriched_records[idx]
        # Process only if we are sure it is not commercial (based on AddressBase class)
        if enriched_record.get("cl", "R") in RESIDENTIAL_CLASSES:
            # Pre-generate the _id
            lease_id = ObjectId()
            mapped_row["_id"] = lease_id

            batch.append(InsertOne(mapped_row))
            enriched_record["lid"] = lease_id
            enriched_batch.append(enriched_record)

            # Prepare LeaseTracker upserts for unique UIDs
            uid = mapped_row["uid"]
            if last_updated and uid not in updated_uids:
                lease_tracker_ops.append(UpdateOne(
                    {"uid": uid},
                    {"$set": {"lastUpdated": last_updated}},
                    upsert=True,
                ))
                updated_uids.add(uid)
        else:
            commercial_count += 1

        if len(batch) >= BATCH_SIZE:
            try:
                # Insert to main collection and get result
                result = collection.bulk_write(batch, ordered=False)
                batch_count += len(batch)
                # logger.info(f"📊 Bulk added {batch_count} records so far...")

                # Insert to leasesext collection
                if enriched_batch:
                    leasesext_batch = [InsertOne(record) for record in enriched_batch]
                    leasesext_collection.bulk_write(leasesext_batch, ordered=False)
                    # logger.info(f"📊 Added {len(leasesext_batch)} enriched records to leasesext")
            except BulkWriteError as e:
                logger.warning(f"Bulk write error: {e.details}")
                batch_count += len(batch) - len(e.details.get("writeErrors", []))

            batch = []
            enriched_batch = []

    # Process remaining batch
    if batch:
        try:
            # Insert to main collection and get result
            result = collection.bulk_write(batch, ordered=False)
            batch_count += len(batch)
            # logger.info(f"📊 Bulk added {batch_count} records in total.")

            # Insert to leasesext collection
            if enriched_batch:
                leasesext_batch = [InsertOne(record) for record in enriched_batch]
                leasesext_collection.bulk_write(leasesext_batch, ordered=False)
                # logger.info(f"📊 Added {len(leasesext_batch)} enriched records to leasesext")
        except BulkWriteError as e:
            logger.warning(f"Bulk write error: {e.details}")
            batch_count += len(batch) - len(e.details.get("writeErrors", []))

    # Update LeaseTracker
    if lease_tracker_ops:
        lease_tracker_collection.bulk_write(lease_tracker_ops)

    return batch_count, commercial_count


def update_database_log(
    lease_update_log_collection,
    last_updated: str,
    csv_filename: str,
    add_count: int,
    delete_count: int,
    skipped_count: int,
    unknown_count: int,
) -> None:
    """
    Update the database log with operation results.

    Args:
        lease_update_log_collection: LeaseUpdateLog collection
        last_updated: Version string
        csv_filename: Name of the CSV file
        add_count: Number of additions
        delete_count: Number of deletions
        skipped_count: Number of skipped records
        unknown_count: Number of manual/skipped records
    """
    lease_update_log_collection.update_one(
        {"version": last_updated},
        {
            "$set": {
                "added": add_count,
                "deleted": delete_count,
                "skipped": skipped_count,
                "manualReview": unknown_count,
                "notes": f"Change file: {csv_filename}",
                "updatedAt": datetime.now(timezone.utc),
            }
        },
        upsert=True,
    )
    logger.info(f"✅ Updated LeaseUpdateLog for version {last_updated}")


def write_enriched_records_to_csv(
    enriched_records: List[Dict[str, Any]],
    csv_path: Path,
) -> None:
    """
    Write enriched records to a CSV file.

    Args:
        enriched_records: List of enriched record dictionaries
        csv_path: Path where the CSV file should be written
    """
    if not enriched_records:
        logger.warning("⚠️ No enriched records to write to CSV")
        return

    # Get all unique field names from all records
    fieldnames = set()
    for record in enriched_records:
        if record:
            fieldnames.update(record.keys())

    # Sort fieldnames for consistent output, but put uid first if it exists
    fieldnames = sorted(fieldnames)
    if "uid" in fieldnames:
        fieldnames.remove("uid")
        fieldnames = ["uid"] + fieldnames

    try:
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for record in enriched_records:
                if record:
                    # Handle nested fields (like location) by converting to string
                    row = {}
                    for key, value in record.items():
                        if isinstance(value, (dict, list)):
                            row[key] = str(value)
                        else:
                            row[key] = value
                    writer.writerow(row)

        logger.info(f"✅ Wrote {len(enriched_records)} enriched records to {csv_path}")
    except Exception as e:
        logger.error(f"❌ Failed to write enriched records to CSV: {e}")


def print_summary(
    add_count: int,
    delete_count: int,
    unknown_count: int,
    skipped_count: int,
    parsed_valid_terms_percentage: int,
    mapped_records_percentage: int,
    total_add_rows: int,
    commercial_count: int,
) -> None:
    """
    Print summary of operations.

    Args:
        add_count: Number of additions
        delete_count: Number of deletions
        unknown_count: Number of manual/skipped records
        skipped_count: Number of skipped records
        parsed_valid_terms_percentage: Number of successfully parsed terms
        mapped_records_percentage: Number of records mapped to AddressBase
        total_add_rows: Total number of add rows
        commercial_count: Number of commercial records skipped
    """
    logger.info("\n🔍 Summary:")
    logger.info(f" - Additions: {add_count}")
    logger.info(f" - Deletions: {delete_count}")
    logger.info(f" - Commercial skipped: {commercial_count}")
    logger.info(f" - Manual/Skipped: {unknown_count}")
    logger.info(f" - Skipped (bad columns): {skipped_count}")
    logger.info(f" - Term enrichment percentage: {parsed_valid_terms_percentage:.2f}")
    logger.info(f" - AddressBase mapping percentage: {mapped_records_percentage:.2f}")


def process_changes(
    csv_path: str,
    database_name: str,
    collection_name: str,
    collection_ext_name: str,
    connection_string: str,
    dry_run: bool = True,
    write_enriched: bool = False,
) -> Dict[str, int]:
    """
    Process changes from a CSV file.

    Args:
        csv_path: Path to the change CSV file
        database_name: MongoDB database name
        collection_name: MongoDB collection name
        collection_ext_name: MongoDB extensions collection name
        connection_string: MongoDB connection string
        dry_run: Whether to run in dry-run mode
        write_enriched: Whether to write enriched records to CSV file

    Returns:
        Dictionary with operation counts
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    # Extract version from filename
    csv_filename = csv_path.name
    last_updated = extract_version_from_filename(csv_filename)

    # Initialize MongoDB connection
    client = MongoDBClient(
        connection_string=connection_string,
        database_name=database_name,
    )

    skipped_count = 0
    updated_uids: Set[str] = set()

    try:
        with client:
            collection = client.get_collection(collection_name)
            leasesext_collection = client.get_collection(collection_ext_name)
            lease_tracker_collection = client.get_collection("lease_tracker")
            lease_update_log_collection = client.get_collection("lease_update_log")

            logger.info(f"📦 Connected to database: {database_name}.{collection_name}")

            # Read CSV and separate changes
            delete_rows, add_rows = read_csv_changes(csv_path)

            # Process deletions
            delete_count, ext_delete_count, unknown_count = process_deletions(
                delete_rows,
                collection,
                lease_tracker_collection,
                dry_run,
                last_updated,
                updated_uids,
                leasesext_collection,
            )

            # Enrich data
            logger.info("ENRICHING DATA" + (" (DRY-RUN)" if dry_run else ""))
            enriched_records, parsed_valid_terms_percentage, mapped_records_percentage = process_enrichment(add_rows)
            logger.info("ENRICHMENT COMPLETE")

            if write_enriched:
                output_csv_path = csv_path.parent / "enriched_results.csv"
                write_enriched_records_to_csv(enriched_records, output_csv_path)

            # Process bulk additions
            add_count, commercial_count = process_bulk_additions(
                add_rows,
                enriched_records,
                collection,
                lease_tracker_collection,
                dry_run,
                last_updated,
                updated_uids,
                leasesext_collection
            )

            # Print summary
            print_summary(
                add_count,
                delete_count,
                unknown_count,
                skipped_count,
                parsed_valid_terms_percentage,
                mapped_records_percentage,
                len(add_rows),
                commercial_count
            )

            # Update database log
            if not dry_run and last_updated:
                update_database_log(
                    lease_update_log_collection,
                    last_updated,
                    csv_filename,
                    add_count,
                    delete_count,
                    skipped_count,
                    unknown_count,
                )

    except Exception as e:
        import traceback
        traceback_str = traceback.format_exc()
        logger.error(f"❌ Exception occurred:\n{traceback_str}")
        logger.error(f"❌ Error processing changes: {e}")
        raise

    return {
        "additions": add_count,
        "deletions": delete_count,
        "extensions deleted": ext_delete_count,
        "commercial_skipped": commercial_count,
        "manual_skipped": unknown_count,
        "skipped": skipped_count,
        "term_enrichment_percentage": parsed_valid_terms_percentage,
        "addressbase_mapping_percentage": mapped_records_percentage,
    }


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Apply lease data updates from change CSV files"
    )
    parser.add_argument(
        "csv_path",
        type=str,
        help="Path to the change CSV file",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes to database (default is dry-run)",
    )
    parser.add_argument(
        "--database",
        type=str,
        default=os.getenv("MONGO_DATABASE"),
        help=f"MongoDB database name (default from env file)",
    )
    parser.add_argument(
        "--collection",
        type=str,
        default=os.getenv("MONGO_COLLECTION"),
        help=f"MongoDB collection name (default from env file)",
    )
    parser.add_argument(
        "--collection-ext",
        type=str,
        default=os.getenv("MONGO_COLLECTION_EXT"),
        help=f"MongoDB extension collection name (default from env file)",
    )
    parser.add_argument(
        "--connection-string",
        type=str,
        default=os.getenv("MONGO_URI"),
        help=f"MongoDB connection string (default from env file)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--write-enriched",
        action="store_true",
        help="Write enriched records to enriched_results.csv in the same folder as input CSV",
    )

    args = parser.parse_args()

    # Set debug mode
    global DEBUG
    DEBUG = args.debug
    if DEBUG:
        logger.setLevel(logging.DEBUG)

    # Determine dry-run mode
    dry_run = not args.apply

    if dry_run:
        logger.info("🧪 Running in DRY-RUN mode — no database changes will be made.")
    else:
        logger.info("🚨 APPLY mode — database will be modified.")

    try:
        result = process_changes(
            csv_path=args.csv_path,
            database_name=args.database,
            collection_name=args.collection,
            collection_ext_name=args.collection_ext,
            connection_string=args.connection_string,
            dry_run=dry_run,
            write_enriched=args.write_enriched,
        )
        logger.info(f"✅ Processing complete: {result}")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

