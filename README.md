# Procurement Graph Analyzer (Local Run)

This repository contains a Docker-based application with:
- **Frontend dashboard** (web UI)
- **Backend API** (FastAPI)
- **Neo4j** graph database

## What you will open after it starts
- Dashboard: http://localhost:3000
- API docs: http://localhost:8000/docs
- Health check: http://localhost:8000/health

---

## Requirements (Windows)
- Windows 10/11
- Docker Desktop installed
- WSL2 with Ubuntu 24.04 installed
- Recommended: 8 GB RAM (minimum around 4 GB)

---

## One-time setup

### 1) Install Ubuntu 24.04 (WSL)
1. Open **Microsoft Store**
2. Install **Ubuntu 24.04**
3. Open Ubuntu and create a username + password

### 2) Install Docker Desktop
1. Install Docker Desktop from Docker website
2. Open Docker Desktop

### 3) Enable Docker + WSL integration
In **Docker Desktop**:
1. **Settings → General**: enable **Use the WSL 2 based engine**
2. **Settings → Resources → WSL Integration**: enable **Ubuntu 24.04**
3. Click **Apply & Restart**

### 4) Verify Docker works inside Ubuntu
Open **Ubuntu** and run:
```bash
docker --version
docker compose version
```
If you see version numbers, Docker is ready.

---

## Project setup (every new machine)

### 1) Put the project folder on your Desktop
Unzip the project somewhere simple, for example:
`C:\Users\<YOUR_USER>\Desktop\procurement-graph-analyzer`

Important:
- The folder must contain `docker-compose.yml` at the top level.

### 2) Create your .env file
In the project root, copy the example file:
```bash
cp .env.example .env
```

You can keep defaults for local use, or edit `.env` if needed.

---

## Start the application

### 1) Open Ubuntu and go to the project folder
Example (adjust your Windows username and folder name):
```bash
cd "/mnt/c/Users/<YOUR_USER>/Desktop/procurement-graph-analyzer"
```

Confirm you are in the right place:
```bash
ls
```
You should see `docker-compose.yml`.

### 2) Build and start containers
```bash
docker compose up --build -d
```

### 3) Wait for startup
Wait about **60 seconds** on first start (Neo4j needs time).

### 4) Check health
```bash
curl http://localhost:8000/health
```

---

## Initialize database schema (first run)
Run once after the first successful startup:
```bash
docker compose exec backend python -m backend.queries.init_schema
```

---

## Demo data vs real data (important)

You may see a message similar to:
> All nodes in the graph are artificially created for demonstration. Real data is collected by running POST /ingest/all.

That means the UI is showing **demo** data.

### Ingest real data
Run:
```bash
curl -X POST http://localhost:8000/ingest/all
```

Watch progress:
```bash
docker compose logs -f backend --tail=200
```

If ingestion fails, the logs will usually show the reason (network/DNS/HTTP errors, rate limits, timeouts).

---

## Stop the application
```bash
docker compose down
```

## Start it again later
```bash
docker compose up -d
```

## Reset everything (deletes database)
Warning: this removes stored Neo4j data.
```bash
docker compose down -v
```

---

## Troubleshooting

### Docker command not found in Ubuntu
Fix Docker Desktop integration:
- Docker Desktop → Settings → Resources → WSL Integration → enable Ubuntu 24.04

### Ports already in use (3000 or 8000)
Another app is using the port.
- Stop the other app, or change port mappings in `docker-compose.yml`.

### Ingest does not pull real data
Check backend logs:
```bash
docker compose logs -f backend --tail=300
```
Most common reasons:
- No internet access from Docker network
- Source endpoints blocked or changed
- Timeouts/rate-limits

---

## What should NOT be committed
This repo is prepared to avoid committing:
- `.env` (secrets)
- `node_modules/`
- virtual environments (`.venv/`)
- runtime data (`data/`)

See `.gitignore` for details.
