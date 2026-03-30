# Server Setup Guide

This guide provides instructions for setting up the production environment using Docker Compose.

## Prerequisites

- Docker Engine 20.10+
- Docker Compose V2+
- Sufficient disk space for PostgreSQL data and Jupyter notebooks
- Secure server environment with firewall configured
- uv: `snap install astral-uv --classic`
- venv: `apt install python3.12-venv`
- C Compiler: `apt install -y build-essential`

## Services

The Docker Compose setup includes three main services:

1. **PostgreSQL 16** - Database server
2. **pgAdmin 4** - Database management interface
3. **Jupyter Notebook** - Data science environment with datascience-notebook image

## Initial Setup

### 1. Create Environment File

Copy the template and configure with secure credentials:

```bash
cp docker-compose.env.example docker-compose.env
```

### 2. Generate Secure Credentials

**For PostgreSQL password (32+ characters):**
```bash
openssl rand -base64 32
```

**For pgAdmin password (16+ characters):**
```bash
openssl rand -base64 24
```

**For Jupyter token (32+ characters):**
```bash
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

### 3. Edit docker-compose.env File

Open `docker-compose.env` and replace all `CHANGE_ME_*` values with the generated secure credentials:

```bash
# Use a secure editor
nano docker-compose.env
# or
vim docker-compose.env
```

**Required changes:**
- `POSTGRES_PASSWORD`: Use 32+ character password
- `PGADMIN_EMAIL`: Your admin email address
- `PGADMIN_PASSWORD`: Use 16+ character password
- `JUPYTER_TOKEN`: Use 32+ character token

### 4. Set Proper Permissions

Protect the environment file:

```bash
chmod 600 docker-compose.env
```

Jupyter will read this file to read environment variables for the notebook service. Give read permissions to the notebook service while keeping it secure:
```bash
chmod 644 .env
```

### 5. Start Services

```bash
docker compose --env-file ./docker-compose.env up -d
```

Check service status:

```bash
docker compose ps
```

View logs:

```bash
docker compose logs -f
```

### 6. Stop services:

Be careful not to delete volumes (`-v`) when stopping services to preserve AddressBase data.

```bash
docker compose down
```

## Accessing Services

### PostgreSQL
- **Host:** localhost (or server IP)
- **Port:** 5432 (configurable via `POSTGRES_PORT`)
- **Database:** db (configurable via `POSTGRES_DB`)
- **User:** db user (configurable via `POSTGRES_USER`)
- **Password:** From `docker-compose.env` file

**Connection string:**
```
postgresql://USER:YOUR_PASSWORD@localhost:5432/lease_data
```

### pgAdmin
- **URL:** http://localhost:5050 (or http://SERVER_IP:5050)
- **Email:** From `docker-compose.env` file (`PGADMIN_EMAIL`)
- **Password:** From `docker-compose.env` file (`PGADMIN_PASSWORD`)

**Adding PostgreSQL connection in pgAdmin:**
1. Login to pgAdmin
2. Right-click "Servers" → "Register" → "Server"
3. General tab: Name = "Lease Database"
4. Connection tab:
   - Host: postgres
   - Port: 5432
   - Database: lease_data
   - Username: lease_admin
   - Password: (from `docker-compose.env` POSTGRES_PASSWORD)

### Jupyter Notebook
- **URL:** http://SERVER_IP:8888/lab?token=YOUR_JUPYTER_TOKEN 
- **Token:** From `docker-compose.env` file (`JUPYTER_TOKEN`)

**First access:**
1. Navigate to http://localhost:8888
2. Enter the token when prompted
3. Optionally set a password for convenience

**Notebook Configuration:**
- Your local `notebooks/` directory is mounted at `/home/jovyan/work`
- Jupyter starts in your notebooks directory by default
- A Python virtual environment is automatically created with packages from `notebooks/requirements.txt`
- Additional mounted directories:
  - `src/` → `/home/jovyan/src` (read-only access to your source code)
  - `data/` → `/home/jovyan/data` (read-write access to data files)
  - `lease_data/` → `/home/jovyan/lease_data` (read-only access to lease data)

### Load AddressBase Data

Edit loader script `src/addressbase/load_data.py` `DATA_DIR` field to point to the correct server CSVs folder. 

Then run the script to load AddressBase data into PostgreSQL:

```bash
python -m src.addressbase.load_data
```

### Ensure Mongo Indexes

#### Set Up Search Indexes in MongoDB Atlas

The application requires three search indexes for optimal performance:
- `default`
- `addr_autocomplete`
- `postcode_autocomplete`

These must be set up manually in **MongoDB Atlas**:

1. Log in to your MongoDB Atlas account and navigate to your cluster.
2. Go to the **"Search"** tab for your database.
3. Click **"Create Search Index"**.
4. For each index:
   - Select the `leases` collection.
   - Choose **"JSON Editor"** mode.
   - Copy the JSON definition from the corresponding file in `data/atlas-search-indexes/` (`default.json`, `addr_autocomplete.json`, or `postcode_autocomplete.json`).
   - Paste it into the editor and create the index.

Repeat for all three indexes: `default`, `addr_autocomplete`, and `postcode_autocomplete`.

#### Create Field Indexes in MongoDB Atlas

The following field indexes should be created in MongoDB Atlas for the `leases` collection to optimize query performance:

- leasetermcaches
    - Index: term
- leaseviewstats
    - Index: uniqueId
- searchanalytics
    - Index: type
- userloginstats
    - Index: period
- users
    - Index: guid, lastLogin, verificationCode.expiresAt, searchHistory.timestamp

## Maintenance

If pulling changes from GitHub repository and local modifications exist, ensure to discard them (such as notebooks).

```bash
git restore file1 file2
```