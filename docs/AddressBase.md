# AddressBase Explained

AddressBase is a comprehensive dataset of UK addresses maintained by Ordnance Survey. It provides detailed information about every addressable location in the country, including geographic coordinates, property classifications, and unique identifiers. This dataset is essential for geocoding, address validation, and spatial analysis.

Here are the key fields and their descriptions:

### 🔑 Identifiers

**UPRN (Unique Property Reference Number)**
A unique numeric identifier for every addressable location in the UK. It never changes, even if the address text changes.

**OS_ADDRESS_TOID**
The unique identifier (TOID) for the address feature in Ordnance Survey’s Topography Layer.

**UDPRN (Unique Delivery Point Reference Number)**
Royal Mail’s identifier for a delivery point (used in the Postal Address File).

---

### 🏢 Organisation & Department Details

**ORGANISATION_NAME**
Name of the business or organisation registered at the address.

**DEPARTMENT_NAME**
Specific department within the organisation (e.g., “Accounts Dept”).

**PO_BOX_NUMBER**
PO Box number if mail is delivered via a PO Box.

---

### 🏠 Building & Sub-Building Information

**SUB_BUILDING_NAME**
Flat, apartment, suite, or unit name/number (e.g., “Flat 2”, “Suite 5”).

**BUILDING_NAME**
Name of the building (e.g., “Rose Court”).

**BUILDING_NUMBER**
Numeric part of the building address (e.g., 24).

---

### 🛣 Street & Locality Fields

**DEPENDENT_THOROUGHFARE**
A smaller road linked to a main road (e.g., “Back Lane” off “High Street”).

**THOROUGHFARE**
Main street name (e.g., “High Street”).

**DOUBLE_DEPENDENT_LOCALITY**
Very small locality within a dependent locality (e.g., hamlet within a village).

**DEPENDENT_LOCALITY**
Area within a post town (e.g., village or suburb).

**POST_TOWN**
Main postal town assigned by Royal Mail.

---

### 📮 Postal Information

**POSTCODE**
Full UK postcode (e.g., SW1A 1AA).

**POSTCODE_TYPE**
Indicates whether the postcode is geographic (linked to a location) or non-geographic (e.g., PO Box only).

---

### 📍 Geographic Coordinates

**X_COORDINATE**
Easting in the British National Grid.

**Y_COORDINATE**
Northing in the British National Grid.

**LATITUDE**
Latitude in decimal degrees (WGS84).

**LONGITUDE**
Longitude in decimal degrees (WGS84).

---

### 🗂 Classification & Status

**RPC (Representative Point Code)**
Indicates how the coordinate point represents the property (e.g., building centroid, entrance, etc.).

**COUNTRY**
Country within the UK (England, Scotland, Wales, Northern Ireland).

**CLASS**
Property classification (e.g., residential, commercial, education, etc.).

---

### 🔄 Change & Lifecycle Dates

**CHANGE_TYPE**
Indicates whether the record was inserted, updated, or deleted in that release.

**LA_START_DATE**
Date the address became valid in the Local Authority system.

**RM_START_DATE**
Date the address became valid in Royal Mail systems.

**LAST_UPDATE_DATE**
Most recent update to the record.

## Load AddressBase into PostgreSQL

run: src/addressbase/load_data.py

A post process can be used to update the data after loading. This denormalizes the data to increase the data matching. It adds new records for building number ranges (such as 2-6, adds 5 new records for 2,3,4,5,6) and also creates additional records for THOROUGHFARE containing "ST." with the dot removed (e.g., "ST. JAMES'S PARADE" -> adds new record with "ST JAMES'S PARADE").

Post processing: src/addressbase/post_process_denormalizer.py