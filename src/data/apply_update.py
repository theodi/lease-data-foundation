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
from pymongo import InsertOne, UpdateOne
from pymongo.errors import BulkWriteError
from tqdm import tqdm

from src.utils.mongo_client import MongoDBClient

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("apply_update.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

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


def map_row(original_row: Dict[str, str]) -> Dict[str, str]:
    """
    Map CSV row to MongoDB document format.

    Args:
        original_row: Original CSV row with full field names

    Returns:
        Mapped row with short field names
    """
    mapped_row = {}
    for csv_key, db_key in FIELD_MAP.items():
        mapped_row[db_key] = str(original_row.get(csv_key, "")).strip()

    # Add extracted postcode
    mapped_row["pc"] = extract_postcode(original_row)

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


def process_delete(
    original_row: Dict[str, str],
    collection,
    dry_run: bool,
    last_updated: Optional[str],
    updated_uids: Set[str],
    lease_tracker_collection,
) -> tuple[int, int]:
    """
    Process a single delete operation.

    Args:
        original_row: CSV row to delete
        collection: MongoDB collection
        dry_run: Whether to run in dry-run mode
        last_updated: Version string for tracking
        updated_uids: Set of UIDs already updated
        lease_tracker_collection: LeaseTracker collection

    Returns:
        Tuple of (delete_count, unknown_count)
    """
    mapped_row = map_row(original_row)
    uid = mapped_row["uid"]
    ro = mapped_row["ro"]
    apid = mapped_row["apid"]

    # Find all records with this UID
    db_matches = list(collection.find({"uid": uid}))

    # Filter to candidates matching RO and APID
    candidate_matches = [
        record for record in db_matches
        if normalize_value(record.get("ro")) == ro
        and normalize_value(record.get("apid")) == apid
    ]

    if not candidate_matches:
        if DEBUG:
            logger.info(f"❌ Delete UID {uid} — no matches for RO {ro} and APID {apid}")
        return 0, 1

    # If exactly one candidate, delete it directly
    if len(candidate_matches) == 1:
        if DEBUG:
            logger.info(f"🗑️ Deleting single candidate match for UID {uid}")
        if not dry_run:
            collection.delete_one({"_id": candidate_matches[0]["_id"]})
            if last_updated and uid not in updated_uids:
                lease_tracker_collection.update_one(
                    {"uid": uid},
                    {"$set": {"lastUpdated": last_updated}},
                    upsert=True,
                )
                updated_uids.add(uid)
        return 1, 0

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
        if DEBUG:
            logger.info(f"🗑️ Would delete {len(exact_matches)} record(s) for UID {uid}")
        if not dry_run:
            ids = [doc["_id"] for doc in exact_matches]
            collection.delete_many({"_id": {"$in": ids}})
            if last_updated and uid not in updated_uids:
                lease_tracker_collection.update_one(
                    {"uid": uid},
                    {"$set": {"lastUpdated": last_updated}},
                    upsert=True,
                )
                updated_uids.add(uid)
        return len(exact_matches), 0

    # Calculate character differences for ambiguous deletion
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
        if not dry_run:
            ids = [doc["_id"] for doc in candidate_matches]
            collection.delete_many({"_id": {"$in": ids}})
            if last_updated and uid not in updated_uids:
                lease_tracker_collection.update_one(
                    {"uid": uid},
                    {"$set": {"lastUpdated": last_updated}},
                    upsert=True,
                )
                updated_uids.add(uid)
        return len(candidate_matches), 0

    # Ambiguous deletion - prompt user
    logger.warning(f"⚠️ Ambiguous deletion for UID {uid} — no exact match:")
    for detail in char_diff_details:
        logger.warning(f"   ⚠️ _id: {detail['_id']}")
        for diff in detail["details"]:
            logger.warning(
                f"      🔸 {diff['field']}: \"{diff['csv_val']}\" ≠ \"{diff['db_val']}\" "
                f"(char diff: {diff['char_diff']})"
            )
    logger.warning(f"   🔢 Total character differences: {total_char_diffs}")

    choice = prompt_user("❓ [k]eep DB, [d]elete anyway, [s]kip? (k/d/s): ")

    if choice == "d":
        if not dry_run:
            ids = [doc["_id"] for doc in candidate_matches]
            collection.delete_many({"_id": {"$in": ids}})
            if last_updated and uid not in updated_uids:
                lease_tracker_collection.update_one(
                    {"uid": uid},
                    {"$set": {"lastUpdated": last_updated}},
                    upsert=True,
                )
                updated_uids.add(uid)
        return len(candidate_matches), 0
    else:
        return 0, 1


def process_changes(
    csv_path: str,
    database_name: str,
    collection_name: str,
    connection_string: str,
    dry_run: bool = True,
) -> Dict[str, int]:
    """
    Process changes from a CSV file.

    Args:
        csv_path: Path to the change CSV file
        database_name: MongoDB database name
        collection_name: MongoDB collection name
        connection_string: MongoDB connection string
        dry_run: Whether to run in dry-run mode

    Returns:
        Dictionary with operation counts
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    # Extract lastUpdated from CSV filename
    csv_filename = csv_path.name
    last_updated_match = re.search(r"(\d{4})_(\d{2})", csv_filename)
    last_updated = f"{last_updated_match.group(1)}-{last_updated_match.group(2)}" if last_updated_match else None

    # Initialize MongoDB connection
    client = MongoDBClient(
        connection_string=connection_string,
        database_name=database_name,
    )

    delete_count = 0
    add_count = 0
    unknown_count = 0
    processed_count = 0
    skipped_count = 0
    updated_uids: Set[str] = set()

    try:
        with client:
            collection = client.get_collection(collection_name)
            lease_tracker_collection = client.get_collection("lease_tracker")
            lease_update_log_collection = client.get_collection("lease_update_log")

            logger.info(f"📦 Connected to database: {database_name}.{collection_name}")

            # First pass: separate delete and add rows
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

            # Process deletes
            logger.info("PROCESSING DELETE")
            for original_row in tqdm(delete_rows, desc="Deleting", unit="rows"):
                deletes, unknowns = process_delete(
                    original_row,
                    collection,
                    dry_run,
                    last_updated,
                    updated_uids,
                    lease_tracker_collection,
                )
                delete_count += deletes
                unknown_count += unknowns
                processed_count += 1

                if processed_count % 1000 == 0:
                    logger.info(f"📊 Processed {processed_count} delete records...")

            logger.info("DELETE COMPLETE")
            logger.info("BULK ADDING")

            # Process adds in bulk
            if not dry_run:
                BATCH_SIZE = 1000
                batch = []
                lease_tracker_ops = []
                batch_count = 0

                for original_row in tqdm(add_rows, desc="Adding", unit="rows"):
                    mapped_row = map_row(original_row)
                    batch.append(InsertOne(mapped_row))

                    # Prepare LeaseTracker upserts for unique UIDs
                    uid = mapped_row["uid"]
                    if last_updated and uid not in updated_uids:
                        lease_tracker_ops.append(UpdateOne(
                            {"uid": uid},
                            {"$set": {"lastUpdated": last_updated}},
                            upsert=True,
                        ))
                        updated_uids.add(uid)

                    if len(batch) >= BATCH_SIZE:
                        try:
                            collection.bulk_write(batch, ordered=False)
                            batch_count += len(batch)
                            logger.info(f"📊 Bulk added {batch_count} records so far...")
                        except BulkWriteError as e:
                            logger.warning(f"Bulk write error: {e.details}")
                            batch_count += len(batch) - len(e.details.get("writeErrors", []))
                        batch = []

                # Process remaining batch
                if batch:
                    try:
                        collection.bulk_write(batch, ordered=False)
                        batch_count += len(batch)
                        logger.info(f"📊 Bulk added {batch_count} records in total.")
                    except BulkWriteError as e:
                        logger.warning(f"Bulk write error: {e.details}")
                        batch_count += len(batch) - len(e.details.get("writeErrors", []))

                # Update LeaseTracker
                if lease_tracker_ops:
                    lease_tracker_collection.bulk_write(lease_tracker_ops)

                add_count = batch_count
            else:
                # Dry run: just count
                add_count = len(add_rows)
                for i, _ in enumerate(add_rows, 1):
                    if i % 1000 == 0:
                        logger.info(f"📊 Would process {i} add records...")

            # Summary
            logger.info("\n🔍 Summary:")
            logger.info(f" - Additions: {add_count}")
            logger.info(f" - Deletions: {delete_count}")
            logger.info(f" - Manual/Skipped: {unknown_count}")
            logger.info(f" - Skipped (bad columns): {skipped_count}")

            # Update the database log
            if not dry_run and last_updated:
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

    except Exception as e:
        logger.error(f"❌ Error processing changes: {e}")
        raise

    return {
        "additions": add_count,
        "deletions": delete_count,
        "manual_skipped": unknown_count,
        "skipped": skipped_count,
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
        default=os.getenv("MONGODB_DATABASE"),
        help=f"MongoDB database name (default from env file)",
    )
    parser.add_argument(
        "--collection",
        type=str,
        default=os.getenv("MONGODB_COLLECTION"),
        help=f"MongoDB collection name (default from env file)",
    )
    parser.add_argument(
        "--connection-string",
        type=str,
        default=os.getenv("MONGODB_URI"),
        help=f"MongoDB connection string (default from env file)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
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
            connection_string=args.connection_string,
            dry_run=dry_run,
        )
        logger.info(f"✅ Processing complete: {result}")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

