# Server Setup Guide

This guide provides instructions for setting up the production environment using Docker Compose.

## Prerequisites

- Docker Engine 20.10+
- Docker Compose V2+
- Sufficient disk space for PostgreSQL data and Jupyter notebooks
- Secure server environment with firewall configured

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

### 5. Start Services

```bash
docker compose up -d
```

Check service status:

```bash
docker compose ps
```

View logs:

```bash
docker compose logs -f
```

## Accessing Services

### PostgreSQL
- **Host:** localhost (or server IP)
- **Port:** 5432 (configurable via `POSTGRES_PORT`)
- **Database:** lease_data (configurable via `POSTGRES_DB`)
- **User:** lease_admin (configurable via `POSTGRES_USER`)
- **Password:** From `docker-compose.env` file

**Connection string:**
```
postgresql://lease_admin:YOUR_PASSWORD@localhost:5432/lease_data
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
- **URL:** http://localhost:8888 (or http://SERVER_IP:8888)
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

**Database connection in Jupyter:**
```python
import psycopg2
from sqlalchemy import create_engine

# Connection string (credentials are available as environment variables)
import os
conn_string = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"

# Using SQLAlchemy
engine = create_engine(conn_string)

# Using psycopg2
conn = psycopg2.connect(conn_string)
```

## Security Best Practices

### 1. Environment File Security
- **Never** commit `docker-compose.env` to version control (already in `.gitignore`)
- Keep `docker-compose.env` with restricted permissions: `chmod 600 docker-compose.env`
- Store backup of credentials in secure password manager
- Rotate credentials regularly (every 90 days minimum)

### 2. Network Security
- Use firewall to restrict port access
- Only expose ports to trusted networks
- Consider using VPN for remote access
- For internet exposure, use reverse proxy with SSL/TLS

### 3. Production Hardening

**Configure firewall (example using ufw):**
```bash
# Allow SSH
sudo ufw allow ssh

# Allow only from specific IP ranges
sudo ufw allow from 10.0.0.0/8 to any port 5432  # PostgreSQL
sudo ufw allow from 10.0.0.0/8 to any port 5050  # pgAdmin
sudo ufw allow from 10.0.0.0/8 to any port 8888  # Jupyter

sudo ufw enable
```

**Use reverse proxy with SSL (nginx example):**
```nginx
server {
    listen 443 ssl http2;
    server_name jupyter.yourdomain.com;
    
    ssl_certificate /etc/ssl/certs/your-cert.pem;
    ssl_certificate_key /etc/ssl/private/your-key.pem;
    
    location / {
        proxy_pass http://localhost:8888;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

### 4. Regular Maintenance
- Update Docker images regularly: `docker compose pull && docker compose up -d`
- Monitor logs for suspicious activity: `docker compose logs --tail=100 -f`
- Backup PostgreSQL data regularly (see Backup section)
- Review and rotate credentials every 90 days

## Data Management

### Backups

**PostgreSQL backup:**
```bash
# Create backup
docker compose exec postgres pg_dump -U lease_admin lease_data > backup_$(date +%Y%m%d_%H%M%S).sql

# Create compressed backup
docker compose exec postgres pg_dump -U lease_admin lease_data | gzip > backup_$(date +%Y%m%d_%H%M%S).sql.gz
```

**Restore from backup:**
```bash
# Restore from SQL file
docker compose exec -T postgres psql -U lease_admin lease_data < backup.sql

# Restore from compressed backup
gunzip -c backup.sql.gz | docker compose exec -T postgres psql -U lease_admin lease_data
```

**Volume backups:**
```bash
# Backup all volumes
docker compose down
sudo tar -czf volumes_backup_$(date +%Y%m%d).tar.gz -C /var/lib/docker/volumes/ .
docker compose up -d
```

### Volume Management

Data is persisted in Docker volumes:
- `postgres_data` - PostgreSQL database files
- `pgadmin_data` - pgAdmin configuration and saved connections
- `jupyter_data` - Jupyter notebooks and workspace

**List volumes:**
```bash
docker volume ls | grep lease
```

**Inspect volume:**
```bash
docker volume inspect lease-data-foundation_postgres_data
```

## Common Operations

### View Logs
```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f postgres
docker compose logs -f pgadmin
docker compose logs -f jupyter
```

### Restart Services
```bash
# All services
docker compose restart

# Specific service
docker compose restart postgres
```

### Stop Services
```bash
docker compose down
```

### Update Services
```bash
docker compose pull
docker compose up -d
```

### Clean Up (⚠️ WARNING: Destroys all data)
```bash
# Stop and remove containers, networks, volumes
docker compose down -v
```

## Troubleshooting

### PostgreSQL Connection Issues
1. Check if container is running: `docker compose ps`
2. Check logs: `docker compose logs postgres`
3. Verify credentials in `docker-compose.env` file
4. Test connection: `docker compose exec postgres psql -U lease_admin -d lease_data`

### pgAdmin Can't Connect to PostgreSQL
- Use hostname `postgres` (not `localhost`) when configuring connection in pgAdmin
- Ensure PostgreSQL container is healthy: `docker compose ps`
- Check network: `docker network inspect lease-data-foundation_lease-network`

### Jupyter Notebook Won't Start
1. Check logs: `docker compose logs jupyter`
2. Verify `JUPYTER_TOKEN` is set in `docker-compose.env`
3. Check port conflicts: `lsof -i :8888`
4. Clear browser cache and cookies

### Permission Issues
```bash
# Fix Jupyter work directory permissions
docker compose exec jupyter chown -R jovyan:users /home/jovyan/work
```

### Out of Disk Space
```bash
# Check disk usage
docker system df

# Clean up unused resources
docker system prune -a

# Remove specific volumes (⚠️ data loss)
docker volume rm <volume_name>
```

## Performance Tuning

### PostgreSQL Configuration

Create `docker-compose.override.yml` for custom PostgreSQL settings:

```yaml
version: '3.8'

services:
  postgres:
    command:
      - "postgres"
      - "-c"
      - "max_connections=200"
      - "-c"
      - "shared_buffers=1GB"
      - "-c"
      - "effective_cache_size=3GB"
      - "-c"
      - "maintenance_work_mem=256MB"
      - "-c"
      - "checkpoint_completion_target=0.9"
      - "-c"
      - "wal_buffers=16MB"
      - "-c"
      - "default_statistics_target=100"
      - "-c"
      - "random_page_cost=1.1"
      - "-c"
      - "effective_io_concurrency=200"
```

### Jupyter Memory Limits

Add to `docker-compose.override.yml`:

```yaml
services:
  jupyter:
    deploy:
      resources:
        limits:
          cpus: '4'
          memory: 8G
```

## Monitoring

### Health Checks
```bash
# Check all services
docker compose ps

# PostgreSQL health
docker compose exec postgres pg_isready -U lease_admin
```

### Resource Usage
```bash
# Real-time stats
docker stats

# Disk usage
docker system df -v
```

## Support and Documentation

- Docker Compose: https://docs.docker.com/compose/
- PostgreSQL: https://www.postgresql.org/docs/
- pgAdmin: https://www.pgadmin.org/docs/
- Jupyter: https://jupyter-docker-stacks.readthedocs.io/

## Security Incident Response

If credentials are compromised:

1. **Immediately stop services:**
   ```bash
   docker compose down
   ```

2. **Change all credentials in `docker-compose.env`**

3. **Remove and recreate volumes:**
   ```bash
   docker volume rm lease-data-foundation_pgadmin_data
   ```

4. **Review logs for unauthorized access:**
   ```bash
   docker compose logs > incident_logs.txt
   ```

5. **Restart with new credentials:**
   ```bash
   docker compose up -d
   ```

6. **Force password reset in PostgreSQL:**
   ```bash
   docker compose exec postgres psql -U lease_admin -d lease_data
   ALTER USER lease_admin WITH PASSWORD 'new_secure_password';
   ```

