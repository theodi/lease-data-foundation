# Setup Guide

## AddressBase Setup Guide
Download the installer from [EnterpriseDB](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads). Use Stack Builder (included) after installation to select and install the PostGIS extension.

### PGAdmin Setup (Optional)

```
docker run -p 5050:80 \
    --name my-pgadmin \
    -e 'PGADMIN_DEFAULT_EMAIL=user@example.com' \
    -e 'PGADMIN_DEFAULT_PASSWORD=your_password' \
    -d dpage/pgadmin4
```

http://localhost:5050

1. Right-click on **Servers** > **Register** > **Server...**
2. **General Tab:** Name it `AddressBase`.
3. **Connection Tab:**
* **Host name/address:** `host.docker.internal`
> **Note:** This is a special DNS name provided by Docker Desktop for Mac to reach your computer.


* **Port:** `5432`
* **Maintenance database:** `address_base`
* **Username:** `postgres`
* **Password:** (Your Postgres password)

```SQL
SELECT *
FROM ab_plus
LIMIT 100;
```

### LibPostal

```
brew install libpostal
```