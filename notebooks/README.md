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
**Collection**: `leases`  
**Default URI**: `mongodb://localhost:27017`

⚠️ **IMPORTANT**: Never hardcode credentials. Always load from `.env` file in the notebooks directory.

### Document Schema

Each document represents a UK leasehold property with comprehensive information from HMLR lease data and AddressBase enrichment.

#### Core Fields (Primary for Analysis)

| Field | Type | Availability | Description | Example |
|-------|------|--------------|-------------|---------|
| `_id` | ObjectId | 100%         | Unique MongoDB identifier | `ObjectId("686ed9e9c42e8cab8e1e8d3a")` |
| `location` | GeoJSON Point | ~95%         | Property coordinates (WGS84) | `{"type": "Point", "coordinates": [-0.0294264, 51.5153806]}` |
| `expiry_date` | Date | ~85%         | Lease expiration date (MongoDB Date) | `{"$date": {"$numberLong": "-268963200000"}}` |
| `start_date` | Date | ~85%         | Lease start date (MongoDB Date) | `{"$date": {"$numberLong": "-3393100800000"}}` |
| `tenure_years` | Integer | ~85%         | Original lease term in years | `99` |

#### Property Identification Fields

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `uid` | String | Unique lease identifier (hash) | `"5D0FA4909B7C0FD9477C2275E1948C8F135E233F"` |
| `uprn` | Integer | Unique Property Reference Number | `6089966` |
| `udprn` | Integer | Unique Delivery Point Reference Number | `8071967` |
| `apid` | Integer | AddressBase Property ID | `1001484188` |

#### Address Fields (HMLR Original)

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `rpd` | String | Register Property Description (original from HMLR) | `"7 Agnes Street, Limehouse"` |
| `apd` | String | AddressBase Property Description | `"7A AGNES STREET, LONDON E14 7DG"` |
| `pc` | String | Postcode | `"E14 7DG"` |
| `cty` | String | County | `"GREATER LONDON"` |
| `rgn` | String | Region | `"GREATER LONDON"` |

#### AddressBase Enrichment Fields

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `ab_postcode` | String | AddressBase matched postcode | `"E14 7DG"` |
| `ab_uprn` | Integer | AddressBase matched UPRN | `6089966` |
| `building_number` | Integer/String | Building number | `7` |
| `building_name` | String | Building name | `"7A"` |
| `thoroughfare` | String | Street name | `"AGNES STREET"` |
| `post_town` | String | Post town | `"LONDON"` |
| `class` | String | Property classification code | `"R     "` (Residential) |

#### Geographic Coordinates

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `latitude` | Float | WGS84 latitude | `51.5153806` |
| `longitude` | Float | WGS84 longitude | `-0.0294264` |
| `x_coordinate` | Float | British National Grid easting (EPSG:27700) | `536829.58` |
| `y_coordinate` | Float | British National Grid northing (EPSG:27700) | `181446.58` |

#### Lease Details

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `term` | String | Original lease term description | `"99 years from 24 June 1862"` |
| `dol` | String | Date of Lease (registration date) | `"16-10-1866"` |
| `ro` | Integer | Register Owner indicator | `2` |
| `aci` | String | Additional Charges Indicator | `"N"` |

#### Location Field Structure

The `location` field is a GeoJSON Point object in WGS84 (EPSG:4326):

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
- `expiry_date` with `$numberLong: -268963200000` = June 24, 1961 (expired lease)
- `start_date` with `$numberLong: -3393100800000` = June 24, 1862 (lease start)

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

# Example usage
# expiry_date is automatically a datetime object from PyMongo
years_left = calculate_years_remaining(doc['expiry_date'])
# For the example: June 24, 1961 vs Feb 26, 2026 = -64.7 years (expired)
```

### Data Quality

- ~95% have valid `location` data
- ~85% have valid `expiry_date` data  
- ~80% have BOTH fields
- Some documents may have `null` or missing values
- Small percentage of locations may fall outside UK boundaries

### Common Query Patterns

#### Filter by Region
```python
# Query leases in Greater London
query = {"rgn": "GREATER LONDON", "location": {"$exists": True}}
```

#### Filter by Postcode Area
```python
# Query leases in E14 postcode area
query = {"pc": {"$regex": "^E14"}, "location": {"$exists": True}}
```

#### Filter by Property Type
```python
# Query residential properties only (class starts with 'R')
query = {"class": {"$regex": "^R"}, "location": {"$exists": True}}
```

#### Filter by Lease Length
```python
# Query leases with original term of 99 years or less
query = {"tenure_years": {"$lte": 99}, "expiry_date": {"$exists": True}}
```

#### Filter by Expiry Date Range
```python
from datetime import datetime

# Query leases expiring between 2026 and 2050
query = {
    "expiry_date": {
        "$gte": datetime(2026, 1, 1),
        "$lte": datetime(2050, 12, 31)
    },
    "location": {"$exists": True}
}
```

#### Combine Multiple Filters
```python
# Query short leases (<100 years) in London expiring after 2030
query = {
    "rgn": "GREATER LONDON",
    "tenure_years": {"$lt": 100},
    "expiry_date": {"$gte": datetime(2030, 1, 1)},
    "location": {"$exists": True}
}
```

---

## Environment Setup

### Prerequisites

- Python 3.8+
- MongoDB running locally
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
```

**Load in notebooks**:
```python
from pathlib import Path
from dotenv import load_dotenv
import os

env_path = Path("../.env")
load_dotenv(env_path)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "leases")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "leases")
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

**Purpose**: Analyze lease expiry dates and create heat map showing percentage of leases with < 100 years remaining

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
4. Query MongoDB for documents with `location`
5. Match each point to containing boundary
6. Aggregate counts
7. Visualize and export

### Pattern 2: Temporal Analysis

**Question**: "What percentage of leases expire before X date?"

**Steps**:
1. Define reference date (usually today)
2. Query MongoDB for `expiry_date` field
3. Parse dates
4. Calculate time differences
5. Apply threshold logic
6. Calculate statistics
7. Visualize temporal patterns

### Pattern 3: Geospatial + Temporal

**Question**: "Show heat map of short leases by district"

**Steps**:
1. Query documents with BOTH `location` AND `expiry_date`
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
# Query with specific fields
query = {
    "location": {"$exists": True, "$ne": None},
    "expiry_date": {"$exists": True, "$ne": None}
}
projection = {"location": 1, "expiry_date": 1, "_id": 0}

# Use batch processing
BATCH_SIZE = 50000
cursor = collection.find(query, projection).batch_size(BATCH_SIZE)

# Count documents
total = collection.count_documents(query)
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
doc = collection.find_one({"expiry_date": {"$exists": True}})
expiry = doc['expiry_date']  # This is already a datetime object!
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

2. **Determine Fields**: Identify required MongoDB fields (`location`, `expiry_date`, etc.)

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

**"Districts with most leases expiring in next 50 years?"**
- Query location + expiry_date, calculate years remaining, filter < 50, rank districts

**"Average lease length by region?"**
- Use regions.geojson, calculate years remaining, aggregate by region, compute statistics

**"Leases under 80 years in North West?"**
- Filter by region, filter by expiry threshold, visualize results

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

Notebooks use **February 26, 2026** as reference date for temporal calculations. Adjust `TODAY` variable if analyzing from different date.

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

**Last Updated**: February 2026  
**Purpose**: Enable AI agents to implement complex leasehold data analyses autonomously

