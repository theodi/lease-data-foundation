"""
MongoDB utility module for database connections and operations.
"""

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from typing import Optional, List, Dict, Any


class MongoDBClient:
    """A wrapper class for MongoDB operations."""

    def __init__(
        self,
        connection_string: str = "mongodb://localhost:27017",
        database_name: Optional[str] = None,
    ):
        """
        Initialize the MongoDB client.

        Args:
            connection_string: MongoDB connection URI
            database_name: Optional default database name
        """
        self.connection_string = connection_string
        self.client: Optional[MongoClient] = None
        self.database_name = database_name

    def connect(self) -> MongoClient:
        """
        Establish connection to MongoDB.

        Returns:
            MongoClient instance
        """
        if self.client is None:
            self.client = MongoClient(self.connection_string)
        return self.client

    def close(self) -> None:
        """Close the MongoDB connection."""
        if self.client:
            self.client.close()
            self.client = None

    def get_database(self, database_name: Optional[str] = None) -> Database:
        """
        Get a database instance.

        Args:
            database_name: Name of the database (uses default if not provided)

        Returns:
            Database instance
        """
        self.connect()
        db_name = database_name or self.database_name
        if not db_name:
            raise ValueError("Database name must be provided")
        return self.client[db_name]

    def get_collection(
        self, collection_name: str, database_name: Optional[str] = None
    ) -> Collection:
        """
        Get a collection instance.

        Args:
            collection_name: Name of the collection
            database_name: Name of the database (uses default if not provided)

        Returns:
            Collection instance
        """
        db = self.get_database(database_name)
        return db[collection_name]

    def list_databases(self) -> List[str]:
        """
        List all databases.

        Returns:
            List of database names
        """
        self.connect()
        return self.client.list_database_names()

    def list_collections(self, database_name: Optional[str] = None) -> List[str]:
        """
        List all collections in a database.

        Args:
            database_name: Name of the database

        Returns:
            List of collection names
        """
        db = self.get_database(database_name)
        return db.list_collection_names()

    def find_documents(
        self,
        collection_name: str,
        database_name: Optional[str] = None,
        query: Optional[Dict[str, Any]] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Find documents in a collection.

        Args:
            collection_name: Name of the collection
            database_name: Name of the database
            query: MongoDB query filter (default: empty query returns all)
            limit: Maximum number of documents to return

        Returns:
            List of documents matching the query
        """
        collection = self.get_collection(collection_name, database_name)
        query = query or {}
        cursor = collection.find(query).limit(limit)
        return list(cursor)

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

