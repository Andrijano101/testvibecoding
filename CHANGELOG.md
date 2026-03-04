# Srpska Transparentnost — Improvement Changelog

## Summary of Changes

This document details all improvements made to the Srpska Transparentnost anti-corruption graph intelligence platform.

---

## 1. `database.py` — Connection Layer

**Bug fixed:** No retry logic on transient Neo4j failures (connection drops, timeouts).

**Improvements:**
- Added `tenacity`-based retry with exponential backoff for all queries (3 attempts, handles `ServiceUnavailable`, `SessionExpired`, `TransientError`)
- Configurable connection pool via `NEO4J_MAX_POOL_SIZE` env var (default 50)
- `verify_connectivity()` call on startup — fails fast instead of silently
- Added `check_health()` utility function returning structured health status
- Added `run_query_paginated()` for paginated Cypher results
- Configurable connection timeout via env vars

---

## 2. `detection.py` — Pattern Detection

**Bugs fixed:**
- `ghost_employees()` returned duplicate pairs (A↔B and B↔A); now uses `p1.person_id < p2.person_id` for deduplication
- `contract_splitting()` had no temporal proximity check — contracts from different years could be flagged; now includes date ordering and window tracking

**Improvements:**
- Every detection query now returns a `severity` field (critical/high/medium/low) based on financial thresholds
- Every query returns a `pattern_type` field for uniform alert handling
- Every query returns entity IDs alongside names for graph highlighting
- Added `compute_risk_summary()` that aggregates all detection results into a single risk score
- Added `SEVERITY_WEIGHTS` and `ALL_DETECTORS` registry for batch execution
- `revolving_door()` now considers the time gap between leaving government and joining a company — gaps under 6 months with existing contracts are flagged as critical
- `shell_company_clusters()` severity scales with total contract value and cluster size
- `single_bidder_contracts()` severity scales with contract value (50M+ = critical)
- `contract_splitting()` lowered the lower bound from 0.7× threshold to 0.5× threshold to catch more aggressive splitting
- Null-safe with `coalesce()` on optional financial values

---

## 3. `main.py` — API Layer

**Bug fixed:** The `/stats` endpoint used a query with sequential `MATCH` clauses that create a cartesian product — with N persons, M companies, and K contracts, this produces N×M×K intermediate rows before counting. Fixed by using Neo4j `CALL {}` subqueries.

**Improvements:**
- `/search` now supports `skip` parameter for pagination
- `/detect/all` returns a `risk_summary` object with aggregate risk score and severity counts
- `/detect/donor-contracts` — new endpoint for political donor contract detection
- `/institution/{id}` — new endpoint for institution details with employees and contracts
- Added `CORS_ORIGINS` env var (default: `*`)
- Added optional APScheduler-based background scraping (enabled via `ENABLE_SCHEDULER=true`)
- Uses `ORJSONResponse` for faster JSON serialization
- Graph neighborhood edges now return proper entity IDs (not Neo4j internal IDs) for both source and target — this was broken before (used `toString(id(startNode(r)))` which returns opaque Neo4j IDs that don't match the node IDs returned in the nodes list)
- Better error messages on invalid `entity_type`
- Depth capped at 4 and limit capped at 500 for safety

---

## 4. `entity_resolver.py` — Entity Resolution

**Performance fix:** `resolve_person()` did an O(n) scan through all known entities on every call. With 100K+ persons, this becomes a major bottleneck.

**Improvements:**
- Added `_person_name_index` and `_company_name_index` dictionaries for O(1) exact normalized name lookups before falling back to fuzzy scan
- Added `save()` / `load()` methods for persisting resolver state across scraping runs (JSON serialization)
- Added `stats()` method returning resolution statistics
- Improved Cyrillic transliteration — now properly maps to Serbian Latin with diacritics (ћ→ć, ч→č, ш→š) before ASCII normalization, instead of going directly to ASCII (ћ→c is lossy)
- Added uppercase Cyrillic mappings

---

## 5. `apr_scraper.py` — APR Scraper

**Bug fixed:** All regex patterns were double-escaped (`\\\\s+` instead of `\\s+`), which means they matched literal backslashes followed by `s` instead of whitespace. This caused normalization functions to produce garbage output.

**Improvements:**
- Fixed all regex patterns in `normalize_name()` and `normalize_company_name()`
- Added `tenacity`-based retry on HTTP requests
- Extracted person parsing into `_extract_persons()` method (DRY)
- Added input validation for person names (min length 2)
- Better batch progress logging with percentage
- `search_company()` now extracts link href properly instead of returning the entire element

---

## 6. `schemas.py` — Data Models

**Improvements:**
- Added `SeverityLevel` enum (low/medium/high/critical)
- Added `RiskSummary` model for aggregate risk scoring
- Added field validation: `min_length`, `max_length`, `ge` constraints on numeric fields
- `DashboardStats` now includes optional `risk_summary` field
- Better field descriptions via `Field(..., description=...)`

---

## 7. `Dashboard.jsx` — Frontend

**Critical fix:** Dashboard was entirely hardcoded to demo data — no API calls were made. Search, stats, and graph exploration were all static.

**Improvements:**
- **Live API integration:** On mount, fetches `/stats` and `/detect/all` from the backend; falls back to demo data if API is unavailable
- **Live search:** Debounced (300ms) full-text search via `/search` endpoint with dropdown results
- **Graph exploration:** Clicking a search result calls `/graph/neighborhood` and renders the live subgraph
- **Edge rendering fix:** The APOC-based edge query now returns entity IDs (person_id, maticni_broj, etc.) instead of Neo4j internal IDs, so edge source/target actually match the node IDs
- **Risk summary display:** Shows aggregate risk score, severity breakdown, and risk level badge
- **Loading states:** Skeleton pulse animation while data loads
- **Demo indicator:** Yellow "DEMO" badge in header when running without backend
- **Staggered animations:** Alert cards and data sources animate in with delay
- **Improved graph visualization:** Dashed lines for FAMILY_OF relationships, colored edges for contract relationships, glow filter on highlighted nodes, emoji type icons
- **Design refresh:** DM Sans + IBM Plex Mono typography, darker background, more breathing room, scrollbar styling
- **"Explore network" button** on selected nodes (when connected to live API)
- **RSD formatting** utility with B/M/K abbreviations

---

## 8. `docker-compose.yml` — Infrastructure

**Improvements:**
- All credentials now reference env vars with defaults (not hardcoded)
- Added `CORS_ORIGINS` and `ENABLE_SCHEDULER` env vars
- Neo4j healthcheck changed from `cypher-shell` (requires credentials) to `wget` HTTP check
- Backend healthcheck via `curl` on `/health`
- Added `start_period` on Neo4j health check (30s warm-up)
- Neo4j heap/pagecache sizes configurable via env vars
