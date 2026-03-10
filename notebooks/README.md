# Leasehold Data Analysis Notebooks - Technical Guide for AI Agents

## Overview

This directory contains Jupyter notebooks for analyzing UK leasehold property data stored in MongoDB. This guide provides complete technical information enabling AI agents to implement complex geospatial and temporal analyses autonomously based on user research questions.

**Key Capabilities**: Geospatial analysis, temporal analysis, statistical computation, heat maps, and data export

**Data Scale**: ~7 million lease documents with location and expiry date information

---

## Table of Contents

1. [Data Source & Schema](#data-source--schema)
2. [Environment Setup](#environment-setup)
3. [Available Data Files](#available-data-files)
4. [Example Notebooks](#example-notebooks)
5. [Common Analysis Patterns](#common-analysis-patterns)
6. [Technical Implementation Guide](#technical-implementation-guide)
7. [Visualization Guide](#visualization-guide)
8. [Agent Implementation Workflow](#agent-implementation-workflow)

---

## Data Source & Schema

### MongoDB Connection

**Database**: `leases`  
**Collections**: 
- `leases` - Original lease documents from HMLR data
- `leasesext` - Extended/enriched lease documents with location and expiry data (use for geospatial analysis)

**Default URI**: see `.env` file in the project root (parent directory of `notebooks/`)

⚠️ **IMPORTANT**: Never hardcode credentials. Always load from `.env` file in the notebooks directory.

### Document Schema

The database contains two collections with different purposes:

- **`leases`**: Original HMLR lease data with property descriptions and registration details
- **`leasesext`**: Extended/enriched data with parsed dates, coordinates, and geospatial fields (use for analysis)

Documents are linked via the `uid` field (unique lease identifier hash) and `lid` field (ObjectId reference from leasesext to leases).

---

#### `leases` Collection Schema

Contains the original HMLR (HM Land Registry) lease data.

| Field | Type | Description                                       | Example |
|-------|------|---------------------------------------------------|---------|
| `_id` | ObjectId | Unique MongoDB identifier                         | `ObjectId("69af5ddecb0627da68b19180")` |
| `uid` | String | Unique lease identifier (hash)                    | `"BF8C40AD7B8747ECE10F51EB9D1C44831DA74058"` |
| `rpd` | String | Register Property Description (original from HMLR) | `"21 Oakley Close, Grays (RM20 4AN)"` |
| `apid` | Integer | AddressBase Property ID                           | `23267916` |
| `apd` | String | AddressBase Property Description                  | `"21 OAKLEY CLOSE, GRAYS RM20 4AN"` |
| `uprn` | Long | Unique Property Reference Number                  | `100090731576` |
| `ppd` | Integer | Price Paid Data (purchase price in GBP)           | `160000` |
| `ro` | Integer | Register Order                                    | `2` |
| `dol` | String | Date of Lease (registration date, DD-MM-YYYY)     | `"23-03-2020"` |
| `term` | String | Original lease term description                   | `"189 years from 1 January 1989 to 31 December 2178"` |
| `aci` | String | Additional Charges Indicator (Y/N)                | `"Y"` |
| `pc` | String | Postcode                                          | `"RM20 4AN"` |

---

#### `leasesext` Collection Schema

Contains extended/enriched data with parsed dates, coordinates, and geospatial fields. **Use this collection for geospatial and temporal analysis.**

| Field  | Type | Description                                          | Example |
|--------|------|------------------------------------------------------|---------|
| `_id`  | ObjectId | Unique MongoDB identifier                            | `ObjectId("69af5ddecb0627da68b19568")` |
| `uid`  | String | Unique lease identifier (hash, matches `leases.uid`) | `"BF8C40AD7B8747ECE10F51EB9D1C44831DA74058"` |
| `lid`  | ObjectId | Reference to parent document in `leases` collection  | `ObjectId("69af5ddecb0627da68b19180")` |
| `st`   | Date | Lease start date (parsed)                            | `ISODate("1989-01-01T00:00:00.000Z")` |
| `exp`  | Date | Lease expiration date (parsed)                       | `ISODate("2178-12-31T00:00:00.000Z")` |
| `ty`   | Integer | Tenure years (lease term length)                     | `189` |
| `aup`  | Long | AddressBase matched UPRN                             | `100090731576` |
| `bn`   | String | Building number                                      | `"21"` |
| `bnam` | String | Building name                                        | `"GALENA HOUSE"` |
| `tf`   | String | Thoroughfare (street name)                           | `"OAKLEY CLOSE"` |
| `pt`   | String | Post town                                            | `"GRAYS"` |
| `apc`  | String | AddressBase matched postcode                         | `"RM20 4AN"` |
| `lat`  | Float | WGS84 latitude                                       | `51.4762127` |
| `lon`  | Float | WGS84 longitude                                      | `0.2911122` |
| `cl`   | String | Property classification code                         | `"R"` (Residential) |
| `ud`   | Integer | Unique Delivery Point Reference Number               | `20502947` |
| `xc`   | Integer | British National Grid easting (EPSG:27700)           | `559205` |
| `yc`   | Integer | British National Grid northing (EPSG:27700)          | `177739` |
| `loc`  | GeoJSON Point | Property coordinates (WGS84) for geospatial queries  | `{"type": "Point", "coordinates": [0.2911122, 51.4762127]}` |

---

#### Location Field Structure (`leasesext.loc`)

The `loc` field in the `leasesext` collection is a GeoJSON Point object in WGS84 (EPSG:4326):

```json
{
  "type": "Point",
  "coordinates": [longitude, latitude]
}
```

- **Coordinate System**: WGS84 (EPSG:4326)
- **Order**: `[longitude, latitude]` ⚠️ Note: longitude first, then latitude
- **Range**: UK bounds approximately lon: -8 to 2, lat: 49 to 61
- **Example**: `[-0.0294264, 51.5153806]` = 7A Agnes Street, London

#### Date Field Formats

MongoDB stores dates in a special format. When queried, dates may appear as:

**In MongoDB Shell/Compass**:
```json
{
  "$date": {
    "$numberLong": "-268963200000"
  }
}
```

**In Python (PyMongo)**:
```python
datetime.datetime(1961, 6, 24, 0, 0)  # Automatically converted to datetime object
```

**Important Date Handling Notes**:
1. PyMongo automatically converts MongoDB dates to Python `datetime` objects
2. The `$numberLong` value is milliseconds since Unix epoch (Jan 1, 1970)
3. Negative values represent dates before 1970
4. Some very old leases may have dates in the 1800s or early 1900s
5. Always handle dates as `datetime` objects in Python notebooks

**Example**:
- `ex` with `$numberLong: -268963200000` = June 24, 1961 (expired lease)
- `st` with `$numberLong: -3393100800000` = June 24, 1862 (lease start)

#### Calculating Remaining Lease Years

```python
from datetime import datetime

def calculate_years_remaining(expiry_date, reference_date=datetime.now()):
    """
    Calculate years remaining on a lease.
    expiry_date is already a datetime object when retrieved via PyMongo.
    """
    if expiry_date is None:
        return None
    
    days_remaining = (expiry_date - reference_date).days
    years_remaining = days_remaining / 365.25
    
    return years_remaining

# Example usage with leasesext collection (uses 'exp' field)
years_left = calculate_years_remaining(doc['exp'])

# Example usage with leases collection (uses 'expiry_date' field)
# years_left = calculate_years_remaining(doc['expiry_date'])
# For the example: June 24, 1961 vs Feb 26, 2026 = -64.7 years (expired)
```

### Data Quality

- ~95% have valid `location` data
- ~85% have valid `expiry_date` data  
- ~80% have BOTH fields
- Some documents may have `null` or missing values
- Small percentage of locations may fall outside UK boundaries

### Common Query Patterns

**Note**: The `leasesext` collection contains geospatial and temporal fields. For property details like region, county, and term description, query the `leases` collection and join via `uid`.

#### Filter by Location (leasesext)
```python
# Query leases with valid location data
query = {"loc": {"$exists": True, "$ne": None}}
```

#### Filter by Postcode Area (leasesext)
```python
# Query leases in E14 postcode area
query = {"apc": {"$regex": "^E14"}, "loc": {"$exists": True}}
```

#### Filter by Property Type (leasesext)
```python
# Query residential properties only (class starts with 'R')
query = {"cl": {"$regex": "^R"}, "loc": {"$exists": True}}
```

#### Filter by Lease Length (leasesext)
```python
# Query leases with original term of 99 years or less
query = {"ty": {"$lte": 99}, "exp": {"$exists": True}}
```

#### Filter by Expiry Date Range (leasesext)
```python
from datetime import datetime

# Query leases expiring between 2026 and 2050
query = {
    "exp": {
        "$gte": datetime(2026, 1, 1),
        "$lte": datetime(2050, 12, 31)
    },
    "loc": {"$exists": True}
}
```

#### Combine Multiple Filters (leasesext)
```python
# Query short leases (<100 years) expiring after 2030
query = {
    "ty": {"$lt": 100},
    "exp": {"$gte": datetime(2030, 1, 1)},
    "loc": {"$exists": True}
}
```

---

## Environment Setup

### Prerequisites

- Python 3.8+
- Jupyter notebook environment

### Install Dependencies

From the `notebooks` directory:

```bash
pip install -r requirements.txt
```

**Required packages**: pymongo, python-dotenv, pandas, matplotlib, shapely, pyproj, rtree, tqdm, geopandas, notebook, ipykernel, ipywidgets

### Environment Configuration

**Create `.env` file** in project root (parent directory of `notebooks/`):

```bash
# Path: /path/to/lease-data-foundation/.env
```

**Template** (use `.env.example`):
```dotenv
MONGO_URI=mongodb://localhost:27017
MONGO_DATABASE=leases
MONGO_COLLECTION=leases
MONGO_COLLECTION_EXT=leasesext
```

**Load in notebooks**:
```python
from pathlib import Path
from dotenv import load_dotenv
import os

env_path = Path("../.env")
load_dotenv(env_path)

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DATABASE = os.getenv("MONGO_DATABASE")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")
MONGO_COLLECTION_EXT = os.getenv("MONGO_COLLECTION_EXT")
```

### Start Jupyter

```bash
jupyter notebook
```

---

## Available Data Files

### Geographic Boundaries

Located in `notebooks/data/`:

| File | Description | CRS | Count | Use Case |
|------|-------------|-----|-------|----------|
| `districts.geojson` | UK Local Authority Districts | EPSG:27700 | ~300 | Primary boundary for most analyses |
| `parishes.geojson` | UK Civil Parishes | EPSG:27700 | ~10,000 | Fine-grained analysis |
| `regions.geojson` | UK Regions | EPSG:27700 | ~12 | High-level regional analysis |

**Important**: All GeoJSON files use **British National Grid (EPSG:27700)** and must be transformed to **WGS84 (EPSG:4326)** to match MongoDB location data.

#### District Properties

Each district feature contains:
- `LAD24CD`: District code (unique identifier) - use for joins
- `LAD24NM`: District name (human-readable) - use for display

---

## Example Notebooks

### 1. `district_leasehold_counts.ipynb`

**Purpose**: Count leasehold properties in each UK district

**Key Operations**:
- Spatial joining (point-in-polygon)
- Geographic aggregation
- Ranking and visualization
- CSV export

**Use as template for**: Simple geographic counting and basic spatial analysis

### 2. `lease_expiry_heatmap.ipynb`

**Purpose**: Analyze lease expiry dates and create heat map showing percentage of leases with < 80 years remaining

**Data Source**: Uses `leasesext` collection with shorter field names:
- `loc` - Location (GeoJSON Point)
- `exp` - Expiry date

**Key Operations**:
- Date parsing (multiple formats)
- Temporal calculations
- Geographic aggregation  
- Choropleth heat map generation
- Statistical analysis

**Use as template for**: Date/time analysis, percentage calculations, heat maps, multi-criteria analysis

---

## Common Analysis Patterns

### Pattern 1: Geographic Counting

**Question**: "How many leaseholds are in each district?"

**Steps**:
1. Load geographic boundaries
2. Transform from EPSG:27700 to EPSG:4326
3. Build spatial index (STRtree)
4. Query MongoDB for documents with `loc`
5. Match each point to containing boundary
6. Aggregate counts
7. Visualize and export

### Pattern 2: Temporal Analysis

**Question**: "What percentage of leases expire before X date?"

**Steps**:
1. Define reference date (usually today)
2. Query MongoDB for `exp` field
3. Parse dates
4. Calculate time differences
5. Apply threshold logic
6. Calculate statistics
7. Visualize temporal patterns

### Pattern 3: Geospatial + Temporal

**Question**: "Show heat map of short leases by district"

**Steps**:
1. Query documents with BOTH `loc` AND `exp`
2. Match locations to boundaries
3. Calculate temporal metrics per boundary
4. Generate choropleth map
5. Add summary statistics

### Pattern 4: Filtering & Segmentation

**Question**: "Analyze only leases in London expiring before 2050"

**Steps**:
1. Apply MongoDB query filters
2. Apply geographic filters
3. Perform analysis on subset
4. Compare with overall dataset

---

## Technical Implementation Guide

### Standard Notebook Structure

Every notebook should follow this structure:

1. **Setup & Imports** - Load libraries
2. **Configuration** - Load environment variables from `.env`
3. **MongoDB Connection** - Establish connection
4. **Sample Inspection** - Examine document structure
5. **Load Spatial Data** - Load GeoJSON (if needed)
6. **Transform Coordinates** - Convert to WGS84 (if needed)
7. **Build Spatial Index** - Create STRtree (if needed)
8. **Helper Functions** - Define parsing/matching functions
9. **Process Documents** - Main analysis with progress bars
10. **Create Results** - Build pandas DataFrame
11. **Statistics** - Calculate and display
12. **Visualizations** - Generate charts/maps
13. **Export** - Save to CSV/GeoJSON
14. **Cleanup** - Close MongoDB connection

### Key Code Patterns

#### MongoDB Queries

```python
# Query with specific fields (using leasesext collection)
query = {
    "loc": {"$exists": True, "$ne": None},
    "exp": {"$exists": True, "$ne": None}
}
projection = {"loc": 1, "exp": 1, "_id": 0}

# Use batch processing
BATCH_SIZE = 50000
cursor = collection_ext.find(query, projection).batch_size(BATCH_SIZE)

# Count documents
total = collection_ext.count_documents(query)
```

#### Coordinate Transformation

```python
from pyproj import Transformer

# Create transformer (BNG to WGS84)
transformer = Transformer.from_crs("EPSG:27700", "EPSG:4326", always_xy=True)

# Transform point
lon, lat = transformer.transform(easting, northing)

# Transform polygon (see example notebooks for full function)
```

#### Spatial Indexing

```python
from shapely.geometry import shape, Point
from shapely.prepared import prep
from shapely.strtree import STRtree

# Build spatial index
districts = []
for feature in geojson['features']:
    geom = shape(transformed_geometry)
    districts.append({
        'code': feature['properties']['LAD24CD'],
        'name': feature['properties']['LAD24NM'],
        'geometry': geom,
        'prepared': prep(geom)  # Faster containment checks
    })

geometries = [d['geometry'] for d in districts]
spatial_index = STRtree(geometries)

# Find containing district
def find_district(point, spatial_index, districts, geom_to_district):
    candidates = spatial_index.query(point)
    for idx in candidates:
        if districts[idx]['prepared'].contains(point):
            return districts[idx]['code'], districts[idx]['name']
    return None, None
```

#### Date Handling

```python
from datetime import datetime

def parse_expiry_date(value):
    """
    Parse expiry date from various formats.
    Note: PyMongo automatically converts MongoDB dates to datetime objects,
    so most dates will already be datetime objects.
    """
    if value is None:
        return None
    
    # Already a datetime object (most common case with PyMongo)
    if isinstance(value, datetime):
        return value
    
    # Handle string formats (edge cases or manual data entry)
    if isinstance(value, str):
        formats = ['%Y-%m-%d', '%Y/%m/%d', '%d-%m-%Y', '%d/%m/%Y', 
                   '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f']
        for fmt in formats:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
    
    # Handle year-only values (edge case)
    if isinstance(value, (int, float)):
        year = int(value)
        if 1800 <= year <= 3000:  # Extended range for historical leases
            return datetime(year, 1, 1)
    
    return None

def calculate_years_remaining(expiry_date, reference_date=datetime(2026, 2, 26)):
    """
    Calculate years remaining from reference date to expiry date.
    
    Args:
        expiry_date: datetime object (automatically from PyMongo)
        reference_date: datetime object (default: Feb 26, 2026)
    
    Returns:
        float: Years remaining (negative if expired)
    """
    if expiry_date is None:
        return None
    
    days_remaining = (expiry_date - reference_date).days
    years_remaining = days_remaining / 365.25
    
    return years_remaining

# Example usage in notebooks:
# When you query MongoDB, dates are automatically datetime objects
# Using leasesext collection (with 'exp' field):
doc = collection_ext.find_one({"exp": {"$exists": True}})
expiry = doc['exp']  # This is already a datetime object!
years_left = calculate_years_remaining(expiry)
```

#### Progress Tracking

```python
from tqdm.notebook import tqdm

total = collection.count_documents(query)
with tqdm(total=total, desc="Processing") as pbar:
    for doc in cursor:
        # Process document
        pbar.update(1)
```

---

## Visualization Guide

### Choropleth Heat Map

```python
import geopandas as gpd

# Create GeoDataFrame
gdf = gpd.GeoDataFrame.from_features(geojson['features'])
gdf = gdf.set_crs('EPSG:27700')
gdf_merged = gdf.merge(df_results, on='district_code', how='left')

# Plot
fig, ax = plt.subplots(figsize=(16, 20))
gdf_merged.plot(
    ax=ax,
    column='percentage',
    cmap='RdYlGn_r',
    linewidth=0.5,
    edgecolor='white',
    legend=True,
    vmin=0, vmax=100
)
ax.set_title('Heat Map Title', fontsize=18)
ax.axis('off')
plt.show()
```

### Bar Charts & Histograms

See example notebooks for bar chart and histogram implementations.

---

## Agent Implementation Workflow

### When User Requests New Analysis

1. **Parse Request**: Identify geographic scope, temporal scope, metrics, aggregation level, output format

2. **Determine Fields**: Identify required MongoDB fields (`loc`, `exp`, etc. for leasesext; or `location`, `expiry_date`, etc. for leases collection)

3. **Select Boundaries**: Choose districts.geojson (most common), parishes.geojson (detailed), or regions.geojson (high-level)

4. **Choose Template**: 
   - Simple counting → `district_leasehold_counts.ipynb`
   - Date analysis → `lease_expiry_heatmap.ipynb`
   - Both → `lease_expiry_heatmap.ipynb`

5. **Implement**:
   - Follow standard notebook structure
   - Copy relevant sections from template
   - Modify queries and calculations
   - Add appropriate visualizations
   - Include documentation

6. **Validate**:
   - Test with small dataset first
   - Check coordinate transformations
   - Verify data quality handling
   - Ensure progress tracking works

### Example Research Questions

**"How many leaseholds in London boroughs?"**
- Use districts.geojson, filter London (E09 codes), count by district
- Query `leasesext` collection using `loc` field

**"Districts with most leases expiring in next 50 years?"**
- Query `loc` + `exp` from `leasesext`, calculate years remaining, filter < 50, rank districts

**"Average lease length by region?"**
- Use regions.geojson, calculate years remaining from `exp`, aggregate by region, compute statistics

**"Leases under 80 years in North West?"**
- Filter by region, filter by expiry threshold using `exp`, visualize results

---

## Best Practices for Agents

### DO ✅
- Load credentials from `.env` file
- Use batch processing (BATCH_SIZE = 50000)
- Include progress bars (tqdm)
- Handle missing/invalid data gracefully
- Transform coordinates (EPSG:27700 → EPSG:4326)
- Build spatial index for performance
- Document steps clearly
- Export results to CSV
- Close MongoDB connections

### DON'T ❌
- Hardcode connection strings
- Load entire dataset into memory
- Skip error handling
- Forget coordinate transformation
- Use inefficient spatial queries without STRtree
- Omit progress indicators
- Skip data validation

---

## Performance Tips

1. **Projections**: Only query needed fields
2. **Batch Size**: Use 50000 for optimal memory/speed
3. **Spatial Index**: Always use STRtree for point-in-polygon
4. **Prepared Geometries**: Use `prep()` for repeated containment checks
5. **Stream Processing**: Process documents one at a time, don't load all into memory

---

## Troubleshooting

### Common Issues

**"Connection refused to MongoDB"**
- Ensure MongoDB is running: `brew services start mongodb-community` (macOS) or `sudo systemctl start mongod` (Linux)

**"ModuleNotFoundError"**
- Install dependencies: `pip install -r requirements.txt`

**"No such file: '../.env'"**
- Create `.env` file in project root using `.env.example` template

**Slow spatial queries**
- Verify spatial index (STRtree) is built
- Use prepared geometries
- Check coordinate transformation is correct

**Date parsing failures**
- Use robust parser handling multiple formats (see example notebooks)

**Memory errors**
- Reduce BATCH_SIZE
- Use projections
- Don't store all results in memory

---

## Current Date Context

Notebooks use **February 25, 2026** as reference date for temporal calculations. Adjust `TODAY` variable if analyzing from different date.

---

## Summary

This guide provides complete technical information for AI agents to autonomously implement geospatial and temporal analyses on UK leasehold data. Users provide high-level research questions; agents translate these into working notebooks following provided patterns and templates.

**Key Resources**:
- Example notebooks demonstrate all major patterns
- Helper functions handle common operations
- Visualization templates for standard outputs
- Best practices ensure efficiency and reliability

For additional documentation, see:
- `../README.md` - Main project README
- `../docs/Developer.md` - Developer guide
- `../docs/Setup.md` - Setup instructions

---

**Last Updated**: March 2026  
**Purpose**: Enable AI agents to implement complex leasehold data analyses autonomously

