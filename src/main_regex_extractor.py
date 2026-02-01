"""
Main script for Lease Data Foundation.

Processes all lease records in MongoDB, extracts lease term data using regex,
validates the results, and updates records with extracted fields.

Designed to efficiently handle 8 million+ records using batch processing
and bulk updates.
"""

import time
from typing import Dict, Any, Optional

from pymongo import UpdateOne
from tqdm import tqdm

from utils.mongo_client import MongoDBClient
from utils.regex_extractors import parse_lease_term
from utils.lease_term_validator import is_lease_term_valid

# Configuration
CONNECTION_STRING = "mongodb://localhost:27017"
DATABASE_NAME = "leases"
COLLECTION_NAME = "leases"
TERM_FIELD = "term"
DOL_FIELD = "dol"  # Date of Lease field

# Batch processing settings
BATCH_SIZE = 1000  # Number of documents to process before bulk update
PROGRESS_LOG_INTERVAL = 10000  # Log progress every N documents


def process_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Process a single record: extract lease term and validate.

    Args:
        record: MongoDB document with 'term' field and optional 'dol' (date of lease) field

    Returns:
        Dictionary with fields to update, or None if no term field
    """
    term_str = record.get(TERM_FIELD)
    if not term_str:
        return {
            "regex_is_valid": False,
            "regex_parse_error": "No term field found"
        }

    # Get date of lease (dol) if available for patterns that reference it
    dol = record.get(DOL_FIELD)
    lease_data = parse_lease_term(term_str, dol=dol)
    if lease_data is None:
        return {
            "regex_is_valid": False,
            "regex_parse_error": "Failed to parse term"
        }

    is_valid = is_lease_term_valid(lease_data)
    if is_valid:
        return {
            "regex_is_valid": True,
            "start_date": lease_data["start_date"],
            "expiry_date": lease_data["expiry_date"],
            "tenure_years": lease_data["tenure_years"]
        }
    else:
        return {
            "regex_is_valid": False,
            "regex_parse_error": "Validation failed"
        }


def process_all_records():
    """
    Process all records in the collection using batch processing.

    Uses cursor-based iteration and bulk updates for efficiency.
    """
    with MongoDBClient(CONNECTION_STRING, DATABASE_NAME) as mongo:
        collection = mongo.get_collection(COLLECTION_NAME)

        # Filter to skip already processed records and empty term fields (supports incremental runs)
        query_filter = {
            "regex_is_valid": {"$ne": True},
            "t5_is_valid": {"$ne": True},
            TERM_FIELD: {"$exists": True, "$ne": ""}
        }

        # Get total count for progress bar
        total_count = collection.count_documents(query_filter)
        print(f"Total documents to process: {total_count:,}")

        if total_count == 0:
            print("No documents found in collection.")
            return

        # Statistics
        stats = {
            "processed": 0,
            "valid": 0,
            "invalid": 0,
            "errors": 0
        }

        start_time = time.time()
        bulk_operations = []

        # Use cursor with no_cursor_timeout for long-running operations
        # batch_size controls how many documents MongoDB returns per network round-trip
        cursor = collection.find(
            query_filter,
            no_cursor_timeout=True,
            batch_size=BATCH_SIZE
        )

        try:
            with tqdm(total=total_count, desc="Processing records", unit="docs") as pbar:
                for record in cursor:
                    try:
                        update_fields = process_record(record)

                        if update_fields:
                            # Create bulk update operation
                            bulk_operations.append(
                                UpdateOne(
                                    {"_id": record["_id"]},
                                    {"$set": update_fields}
                                )
                            )

                            # Update statistics
                            if update_fields.get("regex_is_valid"):
                                stats["valid"] += 1
                            else:
                                stats["invalid"] += 1

                        stats["processed"] += 1

                        # Execute bulk update when batch is full
                        if len(bulk_operations) >= BATCH_SIZE:
                            collection.bulk_write(bulk_operations, ordered=False)
                            bulk_operations = []

                        pbar.update(1)

                    except Exception as e:
                        stats["errors"] += 1
                        pbar.update(1)
                        # Continue processing other records

                # Process remaining operations
                if bulk_operations:
                    collection.bulk_write(bulk_operations, ordered=False)

        finally:
            cursor.close()

        # Calculate and print statistics
        elapsed_time = time.time() - start_time
        docs_per_second = stats["processed"] / elapsed_time if elapsed_time > 0 else 0

        print("\n" + "=" * 60)
        print("Processing Complete!")
        print("=" * 60)
        print(f"Total processed:    {stats['processed']:,}")
        print(f"Valid extractions:  {stats['valid']:,} ({100 * stats['valid'] / max(stats['processed'], 1):.1f}%)")
        print(f"Invalid/failed:     {stats['invalid']:,} ({100 * stats['invalid'] / max(stats['processed'], 1):.1f}%)")
        print(f"Errors:             {stats['errors']:,}")
        print(f"Time elapsed:       {elapsed_time:.2f} seconds")
        print(f"Processing rate:    {docs_per_second:.0f} docs/second")
        print("=" * 60)


def process_all_with_t5_fallback():
    """
    Process all records with regex, then run T5 on failures.

    This is the recommended flow for complete extraction:
    1. Run regex extraction (fast) on all records
    2. Run T5 extraction (slow but thorough) on regex failures
    """
    print("=" * 60)
    print("Phase 1: Regex Extraction")
    print("=" * 60)
    process_all_records()

    print("\n")
    print("=" * 60)
    print("Phase 2: T5 Extraction for Regex Failures")
    print("=" * 60)

    # Import and run T5 processing
    from main_t5_extractor import process_t5_records
    process_t5_records()


def main():
    """Main entry point."""
    print("=" * 60)
    print("Lease Data Foundation - Batch Processing")
    print("=" * 60)
    print(f"Database:   {DATABASE_NAME}")
    print(f"Collection: {COLLECTION_NAME}")
    print(f"Batch size: {BATCH_SIZE:,}")
    print("=" * 60)
    print()

    process_all_records()


if __name__ == "__main__":
    main()

