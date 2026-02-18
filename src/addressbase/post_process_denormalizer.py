"""
Post-process denormalizer for AddressBase Plus data.

Performs denormalization operations on the ab_plus table after initial data load:
- Expands building number ranges from BUILDING_NAME (e.g., "2-6") into individual records

Efficient approach for 35M+ records:
- Uses pure SQL with generate_series for bulk expansion
- Inserts directly into ab_plus table with auto-generated primary keys
- Preserves original UPRNs for expanded records
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "user": "postgres",
    "password": os.getenv("DB_PASSWORD"),
    "host": "localhost",
    "port": "5432",
    "dbname": "address_base"
}


def expand_building_number_ranges():
    """
    Efficiently expand BUILDING_NAME ranges like "2-6" into individual records.
    Inserts expanded records directly into ab_plus with the same UPRN as the original.
    The table uses auto-generated primary keys, so duplicate UPRNs are allowed.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    print("--- Step 1: Counting records with building number ranges ---")
    # Count records that match the pattern (e.g., "2-6", "10-20", "1-3")
    cur.execute("""
        SELECT COUNT(*) 
        FROM ab_plus 
        WHERE BUILDING_NAME ~ '^[0-9]+-[0-9]+$';
    """)
    count = cur.fetchone()[0]
    print(f"Found {count} records with building number ranges to expand")

    if count == 0:
        print("No records to expand. Exiting.")
        cur.close()
        conn.close()
        return

    print("--- Step 2: Inserting expanded records into ab_plus (this may take a while) ---")
    # Use generate_series to efficiently expand ranges in pure SQL
    # Keep the same UPRN as the original record
    cur.execute("""
        INSERT INTO ab_plus (
            UPRN, OS_ADDRESS_TOID, UDPRN, ORGANISATION_NAME, DEPARTMENT_NAME,
            PO_BOX_NUMBER, SUB_BUILDING_NAME, BUILDING_NAME, BUILDING_NUMBER,
            DEPENDENT_THOROUGHFARE, THOROUGHFARE, POST_TOWN, DOUBLE_DEPENDENT_LOCALITY,
            DEPENDENT_LOCALITY, POSTCODE, POSTCODE_TYPE, X_COORDINATE, Y_COORDINATE,
            LATITUDE, LONGITUDE, RPC, COUNTRY, CHANGE_TYPE, LA_START_DATE,
            RM_START_DATE, LAST_UPDATE_DATE, CLASS, geom
        )
        SELECT 
            UPRN,
            OS_ADDRESS_TOID,
            UDPRN,
            ORGANISATION_NAME,
            DEPARTMENT_NAME,
            PO_BOX_NUMBER,
            SUB_BUILDING_NAME,
            NULL as BUILDING_NAME,
            expanded_num::TEXT as BUILDING_NUMBER,
            DEPENDENT_THOROUGHFARE,
            THOROUGHFARE,
            POST_TOWN,
            DOUBLE_DEPENDENT_LOCALITY,
            DEPENDENT_LOCALITY,
            POSTCODE,
            POSTCODE_TYPE,
            X_COORDINATE,
            Y_COORDINATE,
            LATITUDE,
            LONGITUDE,
            RPC,
            COUNTRY,
            CHANGE_TYPE,
            LA_START_DATE,
            RM_START_DATE,
            LAST_UPDATE_DATE,
            CLASS,
            geom
        FROM ab_plus,
        LATERAL generate_series(
            SPLIT_PART(BUILDING_NAME, '-', 1)::INTEGER,
            SPLIT_PART(BUILDING_NAME, '-', 2)::INTEGER
        ) AS expanded_num
        WHERE BUILDING_NAME ~ '^[0-9]+-[0-9]+$';
    """)

    inserted = cur.rowcount
    print(f"Inserted {inserted} expanded records into ab_plus")

    print("--- Step 3: Running VACUUM ANALYZE ---")
    cur.execute("VACUUM ANALYZE ab_plus;")

    print("--- Expansion Complete! ---")
    print(f"Original records with ranges (e.g., '2-6'): preserved")
    print(f"New expanded records: {inserted}")

    cur.close()
    conn.close()


def expand_thoroughfare_st_variants():
    """
    Create additional records for THOROUGHFARE containing "ST." with the dot removed.
    e.g., "ST. JAMES'S PARADE" -> adds new record with "ST JAMES'S PARADE"

    This helps match addresses where users may omit the dot after "ST".

    Uses the same UPRN as the original record. The table uses auto-generated
    primary keys, so duplicate UPRNs are allowed.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    print("--- Step 1: Counting records with 'ST.' in THOROUGHFARE ---")
    cur.execute("""
        SELECT COUNT(*) 
        FROM ab_plus 
        WHERE THOROUGHFARE LIKE '%ST.%';
    """)
    count = cur.fetchone()[0]
    print(f"Found {count} records with 'ST.' in THOROUGHFARE to expand")

    if count == 0:
        print("No records to expand. Exiting.")
        cur.close()
        conn.close()
        return

    print("--- Step 2: Inserting ST variant records into ab_plus ---")
    cur.execute("""
        INSERT INTO ab_plus (
            UPRN, OS_ADDRESS_TOID, UDPRN, ORGANISATION_NAME, DEPARTMENT_NAME,
            PO_BOX_NUMBER, SUB_BUILDING_NAME, BUILDING_NAME, BUILDING_NUMBER,
            DEPENDENT_THOROUGHFARE, THOROUGHFARE, POST_TOWN, DOUBLE_DEPENDENT_LOCALITY,
            DEPENDENT_LOCALITY, POSTCODE, POSTCODE_TYPE, X_COORDINATE, Y_COORDINATE,
            LATITUDE, LONGITUDE, RPC, COUNTRY, CHANGE_TYPE, LA_START_DATE,
            RM_START_DATE, LAST_UPDATE_DATE, CLASS, geom
        )
        SELECT 
            UPRN,
            OS_ADDRESS_TOID,
            UDPRN,
            ORGANISATION_NAME,
            DEPARTMENT_NAME,
            PO_BOX_NUMBER,
            SUB_BUILDING_NAME,
            BUILDING_NAME,
            BUILDING_NUMBER,
            DEPENDENT_THOROUGHFARE,
            REPLACE(THOROUGHFARE, 'ST.', 'ST') as THOROUGHFARE,
            POST_TOWN,
            DOUBLE_DEPENDENT_LOCALITY,
            DEPENDENT_LOCALITY,
            POSTCODE,
            POSTCODE_TYPE,
            X_COORDINATE,
            Y_COORDINATE,
            LATITUDE,
            LONGITUDE,
            RPC,
            COUNTRY,
            CHANGE_TYPE,
            LA_START_DATE,
            RM_START_DATE,
            LAST_UPDATE_DATE,
            CLASS,
            geom
        FROM ab_plus
        WHERE THOROUGHFARE LIKE '%ST.%';
    """)

    inserted = cur.rowcount
    print(f"Inserted {inserted} ST variant records into ab_plus")

    print("--- Step 3: Running VACUUM ANALYZE ---")
    cur.execute("VACUUM ANALYZE ab_plus;")

    print("--- ST Variant Expansion Complete! ---")
    print(f"Original records with 'ST.': preserved")
    print(f"New records with 'ST' (no dot): {inserted}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    expand_building_number_ranges()
    expand_thoroughfare_st_variants()


