import os
import sys
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DB_CONFIG = {
    "user": "postgres",
    "password": os.getenv("DB_PASSWORD"),
    "host": "localhost",
    "port": "5432"
}


def drop_addressbase_table():
    """Drop the ab_plus table from the address_base database."""
    DB_CONFIG["dbname"] = "address_base"

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()

        print("--- Dropping ab_plus table ---")
        cur.execute("DROP TABLE IF EXISTS ab_plus CASCADE;")
        print("Table 'ab_plus' has been dropped successfully.")

        cur.close()
        conn.close()
    except psycopg2.OperationalError as e:
        print(f"Could not connect to database 'address_base': {e}")


def drop_addressbase_database():
    """Drop the entire address_base database."""
    DB_CONFIG["dbname"] = "postgres"  # Connect to default DB to drop address_base
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    # Terminate all connections to address_base before dropping
    cur.execute("""
        SELECT pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = 'address_base'
        AND pid <> pg_backend_pid();
    """)

    print("--- Dropping address_base database ---")
    cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'address_base'")
    exists = cur.fetchone()

    if exists:
        cur.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier("address_base")))
        print("Database 'address_base' has been dropped successfully.")
    else:
        print("Database 'address_base' does not exist.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    # Drop the entire database
    # drop_addressbase_database()
    # Default: only drop the table
    # drop_addressbase_table()
    print("\nTo drop the entire database, run: python drop_data.py --database")

