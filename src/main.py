"""
Main script for Lease Data Foundation.

Connects to a local MongoDB instance and lists documents from a specified collection.
"""

from utils.mongo_client import MongoDBClient

# Configuration
CONNECTION_STRING = "mongodb://localhost:27017"
DATABASE_NAME = "leases"
COLLECTION_NAME = "leases"
LIMIT = 10


def main():
    """Main entry point."""
    with MongoDBClient(CONNECTION_STRING, DATABASE_NAME) as mongo:
        print(f"Connected to MongoDB at {CONNECTION_STRING}")
        print(f"Database: {DATABASE_NAME}")
        print(f"Collection: {COLLECTION_NAME}")
        print(f"Fetching first {LIMIT} documents...\n")

        documents = mongo.find_documents(
            collection_name=COLLECTION_NAME,
            limit=LIMIT,
        )

        if not documents:
            print("No documents found in the collection.")
            return

        print(f"Found {len(documents)} document(s):\n")
        print("-" * 80)

        for i, doc in enumerate(documents, 1):
            print(f"Document {i}:")
            for key, value in doc.items():
                print(f"  {key}: {value}")
            print("-" * 80)



if __name__ == "__main__":
    main()

