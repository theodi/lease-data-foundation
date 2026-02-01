"""
Main queries script for Lease Data Foundation.

Contains frequently used MongoDB queries for lease data analysis.
"""

from typing import List, Dict, Any, Optional

from utils.mongo_client import MongoDBClient

# Configuration
CONNECTION_STRING = "mongodb://localhost:27017"
DATABASE_NAME = "leases"
COLLECTION_NAME = "leases"


def get_invalid_regex_records(
    start: int = 0,
    offset: int = 100,
    mongo_client: Optional[MongoDBClient] = None
) -> List[Dict[str, Any]]:
    """
    List records where regex_is_valid is False.

    Args:
        start: Starting index for pagination (default: 0)
        offset: Number of records to return (default: 100)
        mongo_client: Optional MongoDBClient instance (creates new one if not provided)

    Returns:
        List of documents with regex_is_valid = False
    """
    should_close = mongo_client is None

    if mongo_client is None:
        mongo_client = MongoDBClient(CONNECTION_STRING, DATABASE_NAME)
        mongo_client.connect()

    try:
        collection = mongo_client.get_collection(COLLECTION_NAME)

        cursor = collection.find(
            # {"regex_is_valid": False,
            {"t5_is_valid": False,
             "regex_is_valid": False,
            "term": {"$exists": True, "$ne": ""}}
        ).skip(start).limit(offset)

        return list(cursor)

    finally:
        if should_close:
            mongo_client.close()


def get_missing_field_stats(
    mongo_client: Optional[MongoDBClient] = None
) -> Dict[str, Any]:
    """
    Calculate detailed statistics for records with missing fields.

    Args:
        mongo_client: Optional MongoDBClient instance (creates new one if not provided)

    Returns:
        Dictionary with detailed stats for start_date and term fields
    """
    should_close = mongo_client is None

    if mongo_client is None:
        mongo_client = MongoDBClient(CONNECTION_STRING, DATABASE_NAME)
        mongo_client.connect()

    try:
        collection = mongo_client.get_collection(COLLECTION_NAME)

        total_count = collection.count_documents({})

        # Start date stats
        missing_start_date = collection.count_documents({
            "$or": [
                {"start_date": {"$exists": False}},
                {"start_date": None},
                {"start_date": ""}
            ]
        })
        start_date_not_exists = collection.count_documents({"start_date": {"$exists": False}})
        start_date_null = collection.count_documents({"start_date": None})
        start_date_empty = collection.count_documents({"start_date": ""})

        # Term stats
        missing_term = collection.count_documents({
            "$or": [
                {"term": {"$exists": False}},
                {"term": None},
                {"term": ""}
            ]
        })
        term_not_exists = collection.count_documents({"term": {"$exists": False}})
        term_null = collection.count_documents({"term": None})
        term_empty = collection.count_documents({"term": ""})

        return {
            "total_count": total_count,
            "start_date": {
                "missing_total": missing_start_date,
                "percentage": (missing_start_date / total_count * 100) if total_count > 0 else 0.0,
                "not_exists": start_date_not_exists,
                "null": start_date_null,
                "empty_string": start_date_empty,
            },
            "term": {
                "missing_total": missing_term,
                "percentage": (missing_term / total_count * 100) if total_count > 0 else 0.0,
                "not_exists": term_not_exists,
                "null": term_null,
                "empty_string": term_empty,
            }
        }

    finally:
        if should_close:
            mongo_client.close()


def run_invalid_regex_query():
    """Run and display invalid regex records query."""
    print("Fetching records with regex_is_valid = False...")
    print()

    records = get_invalid_regex_records(start=12020, offset=100)

    print(f"Found {len(records)} record(s):")
    print("-" * 60)

    for i, record in enumerate(records, 1):
        print(f"\nRecord {i}:")
        print(f"  _id: {record.get('_id')}")
        print(f"  term: {record.get('term', 'N/A')}")
        print(f"  regex_is_valid: {record.get('regex_is_valid')}")
        print(f"  regex_parse_error: {record.get('regex_parse_error', 'N/A')}")


def run_missing_start_date_query():
    """Run and display detailed missing field statistics."""
    print("Calculating detailed missing field statistics...")
    print()

    stats = get_missing_field_stats()

    print(f"Total records: {stats['total_count']}")
    print()

    # Start date stats
    sd = stats['start_date']
    print("START_DATE Field:")
    print(f"  Missing total: {sd['missing_total']} ({sd['percentage']:.2f}%)")
    print(f"    - Field not exists: {sd['not_exists']}")
    print(f"    - Null value: {sd['null']}")
    print(f"    - Empty string: {sd['empty_string']}")
    print()

    # Term stats
    t = stats['term']
    print("TERM Field:")
    print(f"  Missing total: {t['missing_total']} ({t['percentage']:.2f}%)")
    print(f"    - Field not exists: {t['not_exists']}")
    print(f"    - Null value: {t['null']}")
    print(f"    - Empty string: {t['empty_string']}")


def main():
    """Main entry point for testing queries."""
    print("=" * 60)
    print("Lease Data Foundation - Query Tool")
    print("=" * 60)
    print()

    # Uncomment the query you want to run:
    # run_invalid_regex_query()
    run_missing_start_date_query()

    print()
    print("-" * 60)


if __name__ == "__main__":
    main()

