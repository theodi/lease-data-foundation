import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from src.addressbase.post_process_denormalizer import expand_building_number_ranges, expand_thoroughfare_st_variants

# Load environment variables from .env file
load_dotenv()

DB_CONFIG = {
    "user": "postgres",
    "password": os.getenv("DB_PASSWORD"),
    "host": "localhost",
    "port": "5432"
}

# Path to your CSV files
DATA_DIR = '/Users/huseyinkir/Downloads/ABPOGB_CSV/data'

CSV_COLUMN_ORDER = (
    "UPRN, OS_ADDRESS_TOID, UDPRN, ORGANISATION_NAME, DEPARTMENT_NAME, "
    "PO_BOX_NUMBER, SUB_BUILDING_NAME, BUILDING_NAME, BUILDING_NUMBER, "
    "DEPENDENT_THOROUGHFARE, THOROUGHFARE, POST_TOWN, DOUBLE_DEPENDENT_LOCALITY, "
    "DEPENDENT_LOCALITY, POSTCODE, POSTCODE_TYPE, X_COORDINATE, Y_COORDINATE, "
    "LATITUDE, LONGITUDE, RPC, COUNTRY, CHANGE_TYPE, LA_START_DATE, "
    "RM_START_DATE, LAST_UPDATE_DATE, CLASS"
)

def setup_addressbase_plus():
    DB_CONFIG["dbname"] = "address_base"
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    print("--- Step 1: Extensions ---")
    cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")

    print("--- Step 2: Creating Table with Exact Headers ---")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ab_plus (
            id BIGSERIAL PRIMARY KEY,
            UPRN BIGINT,
            OS_ADDRESS_TOID TEXT,
            UDPRN INTEGER,
            ORGANISATION_NAME TEXT,
            DEPARTMENT_NAME TEXT,
            PO_BOX_NUMBER TEXT,
            SUB_BUILDING_NAME TEXT,
            BUILDING_NAME TEXT,
            BUILDING_NUMBER TEXT,
            DEPENDENT_THOROUGHFARE TEXT,
            THOROUGHFARE TEXT,
            POST_TOWN TEXT,
            DOUBLE_DEPENDENT_LOCALITY TEXT,
            DEPENDENT_LOCALITY TEXT,
            POSTCODE TEXT,
            POSTCODE_TYPE CHAR(1),
            X_COORDINATE DOUBLE PRECISION,
            Y_COORDINATE DOUBLE PRECISION,
            LATITUDE DOUBLE PRECISION,
            LONGITUDE DOUBLE PRECISION,
            RPC INTEGER,
            COUNTRY CHAR(1),
            CHANGE_TYPE CHAR(1),
            LA_START_DATE DATE,
            RM_START_DATE DATE,
            LAST_UPDATE_DATE DATE,
            CLASS CHAR(6),
            geom GEOMETRY(Point, 4326) -- Using WGS84 for easier web use
        );
    """)

    print("--- Step 3: Bulk Loading Data ---")
    load_headerless_csvs(cur)

    print("--- Step 4: Creating Geometry Points ---")
    cur.execute("UPDATE ab_plus SET geom = ST_SetSRID(ST_Point(LONGITUDE, LATITUDE), 4326) WHERE geom IS NULL;")

    print("--- Step 5: Final Indexing ---")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ab_geom ON ab_plus USING GIST (geom);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ab_postcode ON ab_plus (POSTCODE);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ab_uprn ON ab_plus (UPRN);")
    # Index for partial address matching
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ab_thoroughfare_trgm ON ab_plus USING GIN (THOROUGHFARE gin_trgm_ops);")

    print("--- Process Complete: Running Analyze ---")
    cur.execute("VACUUM ANALYZE ab_plus;")

    cur.close()
    conn.close()

    print("--- Step 6: Expanding Building Number Ranges ---")
    expand_building_number_ranges()

    print("--- Step 7: Expanding ST. Thoroughfare Variants ---")
    expand_thoroughfare_st_variants()


def load_headerless_csvs(cur):
    for filename in os.listdir(DATA_DIR):
        if filename.endswith(".csv"):
            file_path = os.path.abspath(os.path.join(DATA_DIR, filename))
            print(f"Ingesting headerless file: {filename}...")

            with open(file_path, 'r') as f:
                # We REMOVE 'WITH CSV HEADER' and keep only 'WITH CSV'
                sql = f"""
                    COPY ab_plus ({CSV_COLUMN_ORDER}) 
                    FROM STDIN 
                    WITH (FORMAT CSV, DELIMITER ',', NULL '');
                """
                cur.copy_expert(sql, f)

def bootstrap_db():
    DB_CONFIG["dbname"] = "postgres"  # Connect to default DB to create new one
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    # 2. Check if address_base exists, if not, create it
    cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'address_base'")
    exists = cur.fetchone()

    if not exists:
        print("Creating database 'address_base'...")
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier("address_base")))
    else:
        print("Database 'address_base' already exists.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    bootstrap_db()
    setup_addressbase_plus()