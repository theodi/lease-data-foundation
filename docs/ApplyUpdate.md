# Apply Update Script

## Overview

The `apply_update.py` script processes lease data change files (CSV format) and applies additions and deletions to the MongoDB lease database. It supports dry-run mode for safe testing before making actual database changes.

## Usage

### Basic Syntax

```bash
python -m src.data.apply_update <csv_path> [options]
```

### Options

- `csv_path` (required): Path to the change CSV file
- `--apply`: Apply changes to database (without this flag, runs in dry-run mode)
- `--database DATABASE`: MongoDB database name (default from env file)
- `--collection COLLECTION`: MongoDB collection name (default from env file)
- `--connection-string CONNECTION_STRING`: MongoDB connection string (default from env file)
- `--debug`: Enable debug logging for detailed operation information

### Examples

#### Dry-run mode (preview changes without applying)
```bash
python -m src.data.apply_update lease_data/LEASES_COU_2026_03.csv
```

#### Apply changes to database
```bash
python -m src.data.apply_update lease_data/LEASES_COU_2026_03.csv --apply
```

#### Use custom database and enable debug logging
```bash
python -m src.data.apply_update lease_data/LEASES_COU_2026_03.csv \
  --apply \
  --database my_leases_db \
  --collection leases_collection \
  --debug
```

#### Use remote MongoDB instance
```bash
python -m src.data.apply_update lease_data/LEASES_COU_2026_03.csv \
  --apply \
  --connection-string "mongodb://user:pass@remote-host:27017"
```

#### Dry-run with enrichment's export (to preview changes before enrichment)

Writes enriched results (lease term parsing and AddressBase mapping derived) into `lease_data/enriched_results.csv` for review before applying to database.

```bash
python -m src.data.apply_update lease_data/LEASES_COU_2026_02.csv --write-enriched
```

## CSV Format

The script expects CSV files with the following structure:

### Required Fields
- **Change Indicator**: 'A' (add) or 'D' (delete)
- **Unique Identifier**: Lease UID
- **Reg Order**: Registration order number
- **Associated Property Description ID**: APID for matching

### All Mapped Fields
- Unique Identifier → uid
- Register Property Description → rpd
- County → cty
- Region → rgn
- Associated Property Description ID → apid
- Associated Property Description → apd
- OS UPRN → uprn
- Price Paid → ppd
- Reg Order → ro
- Date of Lease → dol
- Term → term
- Alienation Clause Indicator → aci
- Postcode (extracted) → pc

## Processing Logic

### Deletion Process

1. **Find by UID**: Locate all records with matching UID
2. **Filter Candidates**: Narrow down to records matching RO and APID
3. **Single Match**: If only one candidate, delete immediately
4. **Exact Match**: If multiple candidates but exact field matches found, delete all exact matches
5. **Near Match (1 char diff)**: If total character differences = 1, treat as exact match and delete
6. **Ambiguous**: Prompt user for manual decision with detailed diff information

### Addition Process

1. **Map Fields**: Convert CSV fields to short MongoDB keys
2. **Extract Postcode**: Parse and normalize postcode from property descriptions
3. **Bulk Insert**: Insert records in batches of 1000 for efficiency
4. **Track Updates**: Update LeaseTracker with version information

### Tracking Updates

The script maintains two tracking collections:

1. **lease_tracker**: Stores `lastUpdated` version per UID
2. **lease_update_log**: Stores summary statistics per version:
   - `version`: Version string (YYYY-MM)
   - `added`: Count of additions
   - `deleted`: Count of deletions
   - `skipped`: Count of skipped records
   - `manualReview`: Count requiring manual review
   - `notes`: Additional information (e.g., source filename)
   - `updatedAt`: Timestamp of update

## Output

### Console Output
```
📦 Connected to database: leases.leases
📖 Reading CSV file...
To delete: 1500
To add: 2300
PROCESSING DELETE
Deleting: 100%|████████████████| 1500/1500 [00:45<00:00, 33.21rows/s]
📊 Processed 1000 delete records...
DELETE COMPLETE
BULK ADDING
Adding: 100%|██████████████████| 2300/2300 [00:12<00:00, 188.42rows/s]
📊 Bulk added 2300 records in total.

🔍 Summary:
 - Additions: 2300
 - Deletions: 1485
 - Manual/Skipped: 15
 - Skipped (bad columns): 0
✅ Updated LeaseUpdateLog for version 2026-03
✅ Processing complete: {'additions': 2300, 'deletions': 1485, 'manual_skipped': 15, 'skipped': 0}
```

### Log File

All operations are logged to `apply_update.log` with timestamps and detailed information.

## Interactive Prompts

When ambiguous deletions are encountered, the script displays:

```
⚠️ Ambiguous deletion for UID 123456789 — no exact match:
   ⚠️ _id: 507f1f77bcf86cd799439011
      🔸 Register Property Description: "123 Main St" ≠ "123 Main Street" (char diff: 3)
      🔸 County: "DEVON" ≠ "Devon" (char diff: 2)
   🔢 Total character differences: 5
❓ [k]eep DB, [d]elete anyway, [s]kip? (k/d/s):
```

Responses:
- `k` (keep): Keep database records, skip deletion
- `d` (delete): Delete records anyway
- `s` (skip): Same as keep

## Error Handling

- **CSV Not Found**: Script exits with error message
- **Bulk Write Errors**: Logs warnings but continues processing
- **Connection Errors**: Logs error and exits
- **CSV Parse Errors**: Logged and processing continues

## Version Extraction

The script automatically extracts version information from the CSV filename:
- Pattern: `_YYYY_MM` (e.g., `LEASES_COU_CHANGES_2026_03.csv`)
- Stored as: `YYYY-MM` (e.g., `2026-03`)

## Dependencies

- pymongo
- python-dotenv
- tqdm
- Standard library: csv, re, logging, argparse, pathlib

## Environment Variables

The script loads environment variables via `dotenv` from a `.env` file. Configure the following variables:

- `MONGODB_URI`: MongoDB connection string 
- `MONGODB_DATABASE`: Database name 
- `MONGODB_COLLECTION`: Collection name 

These environment variables are used as defaults when CLI arguments are not provided. CLI arguments always take precedence over environment variables.

### Example .env file

```env
MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=leases
MONGODB_COLLECTION=leases
```

## Best Practices

1. **Always run in dry-run mode first** to preview changes
2. **Review ambiguous deletions carefully** - they may indicate data quality issues
3. **Monitor the log file** for warnings and errors
4. **Backup database** before applying large change sets
5. **Run during maintenance windows** for large updates
6. **Use --debug flag** when troubleshooting issues

## Troubleshooting

### Script won't start
- Ensure you run with `python -m src.data.apply_update`
- Check that MongoDB connection is accessible
- Verify CSV file exists and is readable

### High manual review count
- Check data quality in CSV
- Review normalization rules
- Consider adjusting character difference threshold

### Slow performance
- Ensure MongoDB indexes exist on `uid`, `ro`, and `apid` fields
- Increase batch size (requires code modification)
- Run during off-peak hours

## Related Scripts

- `src/data/check_for_updates.py`: Download change files from GOV.UK API
- `src/enricher/update_mongo_from_csv.py`: Enrich leases with address data

