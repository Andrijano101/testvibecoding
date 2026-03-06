# Srpska Transparentnost — Graph Intelligence

Detects corruption patterns in Serbian public procurement using a knowledge graph.
Connects MPs, government ministers, company directors, procurement contracts, and institutions — then runs automated pattern detection across the graph.

**Live data sources:** JN Portal (public contracts), APR (company registry), Otvoreni Parlament (MPs), Vlada RS (cabinet ministers)

---

## What you get

- **Dashboard** at http://localhost:3000 — graph visualization, alert list, entity browser
- **API docs** at http://localhost:8000/docs
- **Neo4j browser** at http://localhost:7474 (user: `neo4j`, password from `.env`)

### Detection patterns

| Pattern | Description |
|---|---|
| Sukob interesa | Official's family member owns/directs a company that won contracts from the official's institution |
| Poslanik/Funkcioner — direktor firme | MP or minister simultaneously directors a company winning public contracts |
| Stalni pobednik | Same company wins 50%+ of an institution's total procurement budget |
| Nova firma — veliki ugovor | Company under 3 years old wins high-value contracts |
| Fantomski direktor | One person formally directs multiple companies all winning contracts from the same institution |
| Deljenje ugovora | Same company wins multiple contracts from same institution just below legal threshold |
| Jedan ponuđač | Contract received only one bid |
| Institucionalni monopol | One company receives 70%+ of an institution's entire procurement budget |
| Rotirajuca vrata | Former official moves to a company that then wins contracts from their former institution |

---

## Requirements

| Requirement | Notes |
|---|---|
| Docker Desktop | With WSL2 backend enabled |
| WSL2 + Ubuntu 24.04 | For Windows users |
| 8 GB RAM | Neo4j uses 4 GB heap + 2 GB page cache by default |
| Internet access | Scrapers pull live data from public portals |

---

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/Andrijano101/testvibecoding.git
cd testvibecoding
```

### 2. Create environment file

```bash
cp .env.example .env
```

Default values work for local use. Edit `.env` if you want to change Neo4j password or scraping limits.

### 3. Start all containers

```bash
docker compose up --build -d
```

First build takes 2-4 minutes (downloads base images, installs Python + Node dependencies).

### 4. Wait for Neo4j to be ready (~60 seconds)

```bash
docker compose logs -f neo4j 2>&1 | grep -m1 "Started"
```

Or just check health:

```bash
curl http://localhost:8000/health
# Expected: {"status":"healthy","neo4j":"connected"}
```

### 5. Open the dashboard

http://localhost:3000

You will see a demo graph with synthetic test data. To load real data, continue below.

---

## Loading real data

Data is loaded in stages. Each stage runs scraping + graph ingestion in the background.
Watch progress with: `docker compose logs -f backend --tail=50`

### Stage 1 — Public procurement contracts (JN Portal + UJN OpenData)

```bash
curl -X POST http://localhost:8000/ingest/jnportal
```

Fetches top contracts by value from jnportal.ujn.gov.rs (~5,000 contracts, ~5 min).

```bash
curl -X POST http://localhost:8000/ingest/procurement
```

Fetches historical procurement data from UJN OpenData (~5,000 records, ~2 min).

### Stage 2 — Members of Parliament (Otvoreni Parlament)

```bash
curl -X POST http://localhost:8000/ingest/op
```

Scrapes all 250 MPs with party affiliation, committee roles, and declared company holdings (~3 min).

### Stage 3 — Government ministers (Vlada RS)

```bash
curl -X POST http://localhost:8000/ingest/vlada
```

Fetches current cabinet from Wikipedia (~30 seconds, cached for 24h).

### Stage 4 — Company directors (APR via CompanyWall)

```bash
curl -X POST http://localhost:8000/ingest/apr
```

Enriches top 500 companies by contract value with director names and property data from CompanyWall (~30-45 min due to rate limiting). This runs fully in background — you can use the dashboard while it runs.

### Stage 5 — Run detection

```bash
curl -X POST http://localhost:8000/detect/all
```

Runs all 9 detection patterns across the graph. Results appear immediately in the dashboard alerts tab.

---

## Typical full ingest sequence

```bash
# Run stages 1-3 first (fast)
curl -X POST http://localhost:8000/ingest/jnportal
curl -X POST http://localhost:8000/ingest/procurement
curl -X POST http://localhost:8000/ingest/op
curl -X POST http://localhost:8000/ingest/vlada

# Wait for each to finish, then start APR (slow, runs in background)
curl -X POST http://localhost:8000/ingest/apr

# Watch APR progress
docker compose logs -f backend --tail=20

# Once APR finishes (cw_scrape_done in logs), run detection
curl -X POST http://localhost:8000/detect/all
```

---

## Using the dashboard

### Graph tab
- Click any node to see a quick summary in the sidebar
- Click **Profil** to open the full entity detail panel (all contracts, directors, related entities)
- Click **Graf** to expand the neighborhood graph for that entity
- Use the search bar to find specific people, companies, or institutions

### Upozorenja (Alerts) tab
- Lists all detected patterns sorted by severity
- Click any alert to see full explanation, detection logic, and source portals
- Click **Profil firme** on an alert to open the company detail panel
- Export as CSV, JSON, or printable HTML report

### Podaci (Data) tab
- Shows which data sources are active and how many records each contributed
- Lists all detection patterns with hit counts

### TEST toggle (header)
- When off, hides all synthetic seed/demo data from graph, alerts, entity browser, stats, and exports
- Use this to work with real data only

---

## Environment variables

See `.env.example` for all options. Key settings:

```env
# Neo4j
NEO4J_PASSWORD=changeme123

# Scraping limits
PROCUREMENT_YEARS=last:3        # How many years of UJN OpenData to fetch
PROCUREMENT_MAX_ROWS=5000       # Max rows per year
JNPORTAL_MAX_ROWS=5000          # Max live contracts from JN Portal
CW_MAX_COMPANIES=500            # Companies to enrich with APR director data
SCRAPE_DELAY=1.5                # Seconds between requests (be respectful)
```

---

## Stopping and restarting

```bash
# Stop (data is preserved in Docker volumes)
docker compose down

# Start again (fast, no rebuild needed)
docker compose up -d

# Full reset — deletes all Neo4j data
docker compose down -v
```

---

## Rebuilding after code changes

```bash
# Backend changes (Python) — restart is enough, code is volume-mounted
docker compose restart backend

# Frontend changes (React/JSX) — requires rebuild
docker compose build frontend && docker compose up -d frontend
```

---

## Troubleshooting

**`docker: command not found` in Ubuntu**
- Docker Desktop → Settings → Resources → WSL Integration → enable Ubuntu 24.04 → Apply & Restart

**Port 3000 or 8000 already in use**
- Change the host port in `docker-compose.yml` (e.g. `"3001:80"`)

**Neo4j won't start / backend shows `neo4j_not_connected`**
- Wait longer — Neo4j needs 30-60 seconds on first start
- Check: `docker compose logs neo4j --tail=20`
- Ensure at least 4 GB RAM is available for Docker

**APR ingest gets stuck or is very slow**
- Normal — CompanyWall rate-limits to ~1 request/8 seconds
- Check progress: `docker compose logs backend 2>&1 | grep cw_scrape | tail -5`
- If backend was restarted mid-scrape, re-run `POST /ingest/apr` — already-scraped companies are cached and skipped

**Ingest returns 404 for `/ingest/op` or `/ingest/vlada`**
- Rebuild backend: `docker compose build backend && docker compose up -d backend`

**Dashboard shows demo data after ingest**
- Run `POST /detect/all` to generate alerts
- Hard-refresh the browser: `Ctrl+Shift+R`

---

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Frontend      │────▶│   Backend       │────▶│   Neo4j         │
│   React + D3    │     │   FastAPI       │     │   Graph DB      │
│   port 3000     │     │   port 8000     │     │   port 7474/7687│
└─────────────────┘     └────────┬────────┘     └─────────────────┘
                                  │
                    ┌─────────────┼─────────────┐
                    ▼             ▼             ▼
              JN Portal      Otvoreni       CompanyWall
              UJN OpenData   Parlament      (APR proxy)
              Vlada RS       (MPs)          Wikipedia
```

**Node types:** Person, Company, Institution, Contract, PoliticalParty, Address, Property, BudgetItem

**Key relationships:** EMPLOYED_BY, DIRECTS, OWNS, WON_CONTRACT, AWARDED_CONTRACT, MEMBER_OF, FAMILY_OF, DONATED_TO

---

## What is NOT committed

- `.env` — secrets/passwords
- `data/` — scraped raw data and Neo4j cache files
- `node_modules/`, `.venv/` — dependencies

See `.gitignore` for full list.
