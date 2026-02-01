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
            {"regex_is_valid": False,
            "term": {"$exists": True}}
        ).skip(start).limit(offset)

        return list(cursor)

    finally:
        if should_close:
            mongo_client.close()


def main():
    """Main entry point for testing queries."""
    print("=" * 60)
    print("Lease Data Foundation - Query Tool")
    print("=" * 60)
    print()

    # Example: Get first 10 invalid records
    print("Fetching records with regex_is_valid = False...")
    print()

    records = get_invalid_regex_records(start=350020, offset=100)

    print(f"Found {len(records)} record(s):")
    print("-" * 60)

    for i, record in enumerate(records, 1):
        print(f"\nRecord {i}:")
        print(f"  _id: {record.get('_id')}")
        print(f"  term: {record.get('term', 'N/A')}")
        print(f"  regex_is_valid: {record.get('regex_is_valid')}")
        print(f"  regex_parse_error: {record.get('regex_parse_error', 'N/A')}")

    print()
    print("-" * 60)


if __name__ == "__main__":
    main()

