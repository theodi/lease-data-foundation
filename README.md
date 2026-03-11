# Lease Data Foundation

This repository contains the foundational data pipelines and processing logic for leasehold datasets, with a primary focus on HM Land Registry (HMLR) leasehold data.

The work in this repository supports the creation of a clean, structured, and high-quality "golden record" of residential leasehold information, enabling scalable analysis and downstream services.

## Scope

The repository covers:

* Filtering and preparation of residential leasehold data
* Parsing and normalisation of lease attributes (e.g. lease dates, terms, remaining years)
* Data quality improvement using deterministic rules and language models
* Batch ingestion and change-only update processing
* Confidence scoring and quality assurance flags

## Data Enrichment

The project implements a multi-stage extraction pipeline to derive structured lease information from the free-text lease descriptions in HMLR records:

* **Regex-based parsing**: Deterministic templates for common lease term formats
* **T5 Language Model**: A locally trained T5-small model handles ambiguous records (operates offline with ~512MB RAM)
* **AddressBase integration**: Cross-references with AddressBase for property classification and geolocation
* **Postcodes API**: Enriches records with coordinate data

The enrichment pipeline achieves:
- 99.75% parsing success rate for records with valid lease terms
- 95.6% geolocation coverage (latitude/longitude)
- Filtering of commercial properties using AddressBase classifications

### Enriched Data Fields

| Field | Description |
|-------|-------------|
| `uid` | Unique identifier |
| `lid` | Lease ID foreign key |
| `st` | Lease term start date |
| `exp` | Lease term expiry date |
| `ty` | Lease term tenure years |
| `cl` | Class (Residential, mixed use, unknown) |
| `apc` | AddressBase postcode |
| `aup` | AddressBase UPRN |
| `lat` / `lon` | Latitude / Longitude |
| `loc` | Geo-location point object |

## Deployment

The production environment uses Docker Compose to run:
- **PostgreSQL 16**: AddressBase database
- **pgAdmin 4**: Database management interface
- **Jupyter Notebook**: Self-service analytics environment

For detailed setup instructions, see [Server Setup Guide](docs/ServerSetup.md).

## Monthly Updates

The dataset is kept current through an automated Change-Only Update (COU) pipeline that processes monthly delta files from HMLR:

1. **Check for updates**: Downloads change files from the GOV.UK API
2. **Preview changes**: Dry-run mode to review additions and deletions
3. **Apply changes**: Process additions/deletions with enrichment pipeline
4. **Track versions**: Maintains update history and version metadata

For detailed instructions, see [Apply Update Documentation](docs/ApplyUpdate.md).

## Analytics

The repository includes Jupyter notebooks for data analysis and user behaviour insights:
- Identification of "short-lease hotspots" by local authority
- Leasehold counts by district, region, and parish
- Geographic analysis using provided GeoJSON boundary files

See [Notebooks README](notebooks/README.md) for more details.

## Status

This repository supports Phase 2 (Pillar 1: Data Foundations) of the Lease project and is under active development.


