"""
Srpska Transparentnost - FastAPI Application
Main API for querying the graph and running detection patterns.

Improvements over v1:
- Fixed stats query (cartesian product bug)
- Pagination support on search results
- Risk summary endpoint
- Scheduled scraping via APScheduler
- Better error responses
- CORS configurable via env
"""
import os
import threading
from datetime import datetime
from typing import Optional

import csv
import io
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse, StreamingResponse, HTMLResponse
from contextlib import asynccontextmanager

from backend.api.database import (
    get_driver, close_driver, run_query, run_query_single, check_health,
)
from backend.models.schemas import (
    SearchRequest, GraphNeighborhood, DashboardStats, SuspiciousPattern,
)
from backend.queries import detection
from backend.queries.detection import ALL_DETECTORS, compute_risk_summary

import structlog

logger = structlog.get_logger()

CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*").split(",")
ENABLE_SCHEDULER = os.getenv("ENABLE_SCHEDULER", "false").lower() == "true"

scheduler = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    get_driver()
    logger.info("app_started")

    # Optional: start scheduled scraping
    if ENABLE_SCHEDULER:
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
            global scheduler
            scheduler = BackgroundScheduler()
            scheduler.add_job(_scheduled_scrape, "interval", hours=24, id="daily_scrape")
            scheduler.start()
            logger.info("scheduler_started")
        except ImportError:
            logger.warning("apscheduler_not_available")

    yield

    if scheduler:
        scheduler.shutdown(wait=False)
    close_driver()
    logger.info("app_stopped")


app = FastAPI(
    title="Srpska Transparentnost API",
    description="Anti-corruption graph intelligence for Serbia",
    version="0.2.0",
    lifespan=lifespan,
    default_response_class=ORJSONResponse,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _scheduled_scrape():
    """Background job: scrape latest procurement data."""
    logger.info("scheduled_scrape_start")
    try:
        from backend.scrapers.procurement_scraper import ProcurementScraper
        from backend.data.graph_loader import GraphLoader

        scraper = ProcurementScraper()
        scraper.scrape_recent(days=7)
        scraper.close()

        loader = GraphLoader()
        loader.load_procurement_data()

        logger.info("scheduled_scrape_complete")
    except Exception as e:
        logger.error("scheduled_scrape_failed", error=str(e))


# ── Dashboard ───────────────────────────────────────────────────
@app.get("/stats", response_model=DashboardStats)
def get_stats():
    """Get dashboard statistics.

    Fixed: uses subqueries to avoid cartesian products.
    """
    result = run_query_single("""
        CALL () {
            MATCH (p:Person) RETURN count(p) AS persons
        }
        CALL () {
            MATCH (c:Company) RETURN count(c) AS companies
        }
        CALL () {
            MATCH (ct:Contract) RETURN count(ct) AS contracts
        }
        CALL () {
            MATCH (i:Institution) RETURN count(i) AS institutions
        }
        CALL () {
            MATCH ()-[r]->() RETURN count(r) AS rels
        }
        RETURN persons, companies, contracts, institutions, rels
    """)
    if not result:
        return DashboardStats()

    # Count distinct real data sources (exclude seed/test)
    source_rows = run_query("""
        MATCH (n)
        WHERE n.source IS NOT NULL AND n.source <> 'seed'
        RETURN DISTINCT n.source AS src
    """)
    active_sources = len([r for r in source_rows if r.get("src") not in (None, "", "seed")])

    return DashboardStats(
        total_persons=result.get("persons", 0),
        total_companies=result.get("companies", 0),
        total_contracts=result.get("contracts", 0),
        total_institutions=result.get("institutions", 0),
        total_relationships=result.get("rels", 0),
        data_sources_active=active_sources,
    )


# ── Search ──────────────────────────────────────────────────────
@app.get("/search")
def search_entities(
    q: str = Query(..., min_length=2, description="Search query"),
    entity_type: Optional[str] = Query(None, description="Filter by Person, Company, or Institution"),
    limit: int = Query(25, ge=1, le=100),
    skip: int = Query(0, ge=0),
):
    """Full-text search across all entities with pagination."""
    fuzzy_q = q + "~"

    if entity_type == "Person":
        results = run_query("""
            CALL db.index.fulltext.queryNodes('person_ft', $query) YIELD node, score
            RETURN node.person_id AS id, node.full_name AS name,
                   labels(node)[0] AS type, node.current_role AS role, score
            ORDER BY score DESC SKIP $skip LIMIT $limit
        """, {"query": fuzzy_q, "limit": limit, "skip": skip})
    elif entity_type == "Company":
        results = run_query("""
            CALL db.index.fulltext.queryNodes('company_ft', $query) YIELD node, score
            RETURN node.maticni_broj AS id, node.name AS name,
                   labels(node)[0] AS type, node.status AS status, score
            ORDER BY score DESC SKIP $skip LIMIT $limit
        """, {"query": fuzzy_q, "limit": limit, "skip": skip})
    else:
        results = run_query("""
            CALL db.index.fulltext.queryNodes('person_ft', $query) YIELD node, score
            RETURN node.person_id AS id, node.full_name AS name,
                   labels(node)[0] AS type, score
            ORDER BY score DESC LIMIT $half_limit
            UNION
            CALL db.index.fulltext.queryNodes('company_ft', $query) YIELD node, score
            RETURN node.maticni_broj AS id, node.name AS name,
                   labels(node)[0] AS type, score
            ORDER BY score DESC LIMIT $half_limit
        """, {"query": fuzzy_q, "half_limit": max(limit // 2, 10)})

    return {"results": results, "query": q, "total": len(results), "skip": skip, "limit": limit}


# ── Graph exploration ───────────────────────────────────────────
@app.get("/graph/neighborhood")
def get_neighborhood(
    entity_id: str,
    entity_type: str = "Person",
    depth: int = Query(2, ge=1, le=4),
    limit: int = Query(100, ge=1, le=500),
):
    """Get the graph neighborhood around an entity."""
    match_clauses = {
        "Person": "MATCH (center:Person {person_id: $eid})",
        "Company": "MATCH (center:Company {maticni_broj: $eid})",
        "Institution": "MATCH (center:Institution {institution_id: $eid})",
    }
    match_clause = match_clauses.get(entity_type)
    if not match_clause:
        raise HTTPException(400, f"Unknown entity type: {entity_type}. Must be Person, Company, or Institution.")

    params = {"eid": entity_id, "depth": min(depth, 4), "limit": limit}

    # Try APOC first, fall back to vanilla Cypher
    try:
        nodes_query = f"""
            {match_clause}
            CALL apoc.path.subgraphAll(center, {{maxLevel: $depth, limit: $limit}})
            YIELD nodes, relationships
            UNWIND nodes AS n
            RETURN DISTINCT
                CASE
                    WHEN n:Person THEN n.person_id
                    WHEN n:Company THEN n.maticni_broj
                    WHEN n:Institution THEN n.institution_id
                    WHEN n:Contract THEN n.contract_id
                    ELSE toString(id(n))
                END AS id,
                CASE
                    WHEN n:Person THEN n.full_name
                    WHEN n:Company THEN n.name
                    WHEN n:Institution THEN n.name
                    WHEN n:Contract THEN n.title
                    ELSE 'Unknown'
                END AS name,
                labels(n)[0] AS type,
                properties(n) AS props
        """

        edges_query = f"""
            {match_clause}
            CALL apoc.path.subgraphAll(center, {{maxLevel: $depth, limit: $limit}})
            YIELD relationships
            UNWIND relationships AS r
            RETURN DISTINCT
                CASE
                    WHEN startNode(r):Person THEN startNode(r).person_id
                    WHEN startNode(r):Company THEN startNode(r).maticni_broj
                    WHEN startNode(r):Institution THEN startNode(r).institution_id
                    WHEN startNode(r):Contract THEN startNode(r).contract_id
                    ELSE toString(id(startNode(r)))
                END AS source,
                CASE
                    WHEN endNode(r):Person THEN endNode(r).person_id
                    WHEN endNode(r):Company THEN endNode(r).maticni_broj
                    WHEN endNode(r):Institution THEN endNode(r).institution_id
                    WHEN endNode(r):Contract THEN endNode(r).contract_id
                    ELSE toString(id(endNode(r)))
                END AS target,
                type(r) AS relationship,
                properties(r) AS props
        """

        nodes = run_query(nodes_query, params)
        edges = run_query(edges_query, params)
        return {"nodes": nodes, "edges": edges, "center": entity_id}

    except Exception as e:
        logger.warning("apoc_fallback", error=str(e))
        safe_depth = min(depth, 3)
        simple_query = f"""
            {match_clause}
            MATCH path = (center)-[*1..{safe_depth}]-(connected)
            UNWIND nodes(path) AS n
            WITH DISTINCT n
            RETURN
                CASE
                    WHEN n:Person THEN n.person_id
                    WHEN n:Company THEN n.maticni_broj
                    WHEN n:Institution THEN n.institution_id
                    WHEN n:Contract THEN n.contract_id
                    ELSE toString(id(n))
                END AS id,
                CASE
                    WHEN n:Person THEN n.full_name
                    WHEN n:Company THEN n.name
                    WHEN n:Institution THEN n.name
                    WHEN n:Contract THEN n.title
                    ELSE 'Unknown'
                END AS name,
                labels(n)[0] AS type
            LIMIT $limit
        """
        nodes = run_query(simple_query, params)
        return {"nodes": nodes, "edges": [], "center": entity_id}


# ── Detection patterns ──────────────────────────────────────────
@app.get("/detect/conflicts")
def detect_conflicts():
    """Detect conflict of interest patterns."""
    return {"patterns": run_query(detection.conflict_of_interest())}


@app.get("/detect/ghosts")
def detect_ghosts():
    """Detect potential ghost employees."""
    return {"patterns": run_query(detection.ghost_employees())}


@app.get("/detect/shells")
def detect_shell_companies():
    """Detect shell company clusters."""
    return {"patterns": run_query(detection.shell_company_clusters())}


@app.get("/detect/single-bidder")
def detect_single_bidder(min_value: float = 1000000):
    """Detect suspicious single-bidder contracts."""
    query, params = detection.single_bidder_contracts(min_value)
    return {"patterns": run_query(query, params)}


@app.get("/detect/revolving-door")
def detect_revolving_door():
    """Detect revolving door patterns."""
    return {"patterns": run_query(detection.revolving_door())}


@app.get("/detect/budget-allocation")
def detect_budget_allocation():
    """Detect suspicious budget self-allocation."""
    return {"patterns": run_query(detection.budget_self_allocation())}


@app.get("/detect/contract-splitting")
def detect_contract_splitting(threshold: float = 6000000):
    """Detect contract splitting patterns."""
    query, params = detection.contract_splitting(threshold)
    return {"patterns": run_query(query, params)}


@app.get("/detect/donor-contracts")
def detect_donor_contracts():
    """Detect political donor contracts."""
    return {"patterns": run_query(detection.political_donor_contracts())}


@app.get("/detect/repeated-winner")
def detect_repeated_winner(min_contracts: int = 3, min_total: float = 5_000_000):
    """Detect companies that repeatedly win from the same institution."""
    query, params = detection.repeated_winner(min_contracts=min_contracts, min_total_rsd=int(min_total))
    return {"patterns": run_query(query, params)}


@app.get("/detect/new-company-big-contract")
def detect_new_company_big_contract(max_age_years: int = 3, min_value: float = 5_000_000):
    """Detect recently-founded companies winning large contracts."""
    query, params = detection.new_company_big_contract(max_age_years=max_age_years, min_value_rsd=int(min_value))
    return {"patterns": run_query(query, params)}


@app.get("/detect/direct-official-contractor")
def detect_direct_official_contractor(min_value: float = 1_000_000):
    """Sukob interesa (direktni): official directly owns/directs a contractor company."""
    query, params = detection.direct_official_contractor(min_value_rsd=int(min_value))
    return {"patterns": run_query(query, params)}


@app.get("/detect/ghost-director")
def detect_ghost_director(min_contracts: int = 2):
    """Fantomski direktor: person directs multiple companies winning from same institution."""
    query, params = detection.ghost_director(min_contracts=min_contracts)
    return {"patterns": run_query(query, params)}


@app.get("/detect/institutional-monopoly")
def detect_institutional_monopoly(min_pct: float = 0.7, min_value: float = 10_000_000, min_contracts: int = 3):
    """Monopol institucije: one company gets ≥70% of an institution's total contract value."""
    query, params = detection.institutional_monopoly(min_pct=min_pct, min_value_rsd=int(min_value), min_contracts=min_contracts)
    return {"patterns": run_query(query, params)}


@app.get("/detect/samododeljivanje")
def detect_samododeljivanje(min_value: float = 1_000_000):
    """Samododeljivanje (proxy): official owns/directs a company that won public contracts."""
    query, params = detection.samododeljivanje_proxy(min_value_rsd=int(min_value))
    return {"patterns": run_query(query, params)}


@app.post("/ingest/apr-directors")
async def ingest_apr_directors(background_tasks: BackgroundTasks, force_refresh: bool = False):
    """
    Enrich company data with APR director/owner information.
    Loads Person→DIRECTS/OWNS→Company relationships needed for:
    - Sukob interesa (direct_official_contractor)
    - Samododeljivanje (samododeljivanje_proxy)
    - Ghost director (ghost_director)

    NOTE: APR live scraping requires reCAPTCHA — currently uses cached/curated data.
    To load custom director data: PUT data to data/raw/apr/directors.json
    with format: [{person_name, company_pib, role, institution_name?, ...}]
    """
    from backend.scrapers.apr_director_scraper import APRDirectorScraper, load_directors_to_neo4j
    import json, os

    def _run():
        scraper = APRDirectorScraper()
        summary = scraper.scrape(force_refresh=force_refresh)

        # Load scraped directors into Neo4j
        out_file = os.path.join(os.getenv("DATA_DIR", "./data"), "raw", "apr", "directors.json")
        if os.path.exists(out_file):
            with open(out_file) as f:
                directors = json.load(f)
            loaded = load_directors_to_neo4j(directors)
            logger.info("apr_directors_loaded", count=loaded)

    background_tasks.add_task(_run)
    return {"status": "started", "message": "APR director enrichment running in background"}


@app.post("/enrich/suspicious-directors")
async def enrich_suspicious_directors(background_tasks: BackgroundTasks):
    """
    Find suspicious companies (from all detection patterns) that have no director data
    in the graph, then scrape CW for exactly those companies and load results.

    This is a targeted enrichment — only scrapes the ~100 risky companies
    that are missing director/owner info.
    """
    # Find suspicious company PIBs without director data
    q = """
    MATCH (co:Company)
    WHERE co.pib IS NOT NULL AND co.pib <> ''
      AND co.pib <> 'SEED'
      AND NOT co.pib STARTS WITH 'MB-SEED'
      AND NOT ((:Person)-[:DIRECTS|OWNS]->(co))
      AND (
        (co)-[:WON_CONTRACT]->(:Contract)
      )
    RETURN co.pib AS pib, co.name AS name
    LIMIT 500
    """
    rows = run_query(q)
    pibs = [r["pib"] for r in rows if r.get("pib")]

    if not pibs:
        return {"status": "no_work", "message": "All suspicious companies already have director data"}

    logger.info("enrich_suspicious_directors_start", count=len(pibs))

    def _run():
        import time, json, os
        from backend.scrapers.companywall_scraper import CompanyWallScraper
        from backend.etl.graph_loader import GraphLoader

        scraper = CompanyWallScraper()
        DATA_DIR = os.getenv("DATA_DIR", "./data")
        apr_saved = 0

        for i, pib in enumerate(pibs):
            apr_path = os.path.join(DATA_DIR, "raw", "apr", f"{pib}.json")
            # skip fresh cache (< 48 h)
            if os.path.exists(apr_path):
                age_h = (time.time() - os.path.getmtime(apr_path)) / 3600
                if age_h < 48:
                    continue

            logger.info("enrich_cw_scrape", pib=pib, i=i + 1, total=len(pibs))
            firma_url = scraper._search(pib)
            if not firma_url:
                time.sleep(1)
                continue
            time.sleep(1)
            company = scraper._scrape_company_page(firma_url, pib)
            if not company:
                time.sleep(2)
                continue

            apr_record = {k: v for k, v in company.items() if k != "properties"}
            apr_record["source"] = "apr"
            if "maticni_broj" not in apr_record:
                apr_record["maticni_broj"] = pib
            os.makedirs(os.path.dirname(apr_path), exist_ok=True)
            with open(apr_path, "w", encoding="utf-8") as f:
                json.dump(apr_record, f, ensure_ascii=False, indent=2)
            apr_saved += 1
            time.sleep(2)

        # Reload CW data into Neo4j
        loader = GraphLoader()
        loader.load_companywall_data()
        loader.close()
        logger.info("enrich_suspicious_directors_done", scraped=apr_saved, total=len(pibs))

    background_tasks.add_task(_run)
    return {
        "status": "started",
        "companies_to_enrich": len(pibs),
        "message": f"Scraping CW for {len(pibs)} suspicious companies without director data"
    }


@app.get("/detect/all")
def detect_all():
    """Run all detection patterns and return summary with risk scoring."""
    results = {}
    detectors = ALL_DETECTORS + [
        ("single_bidder", lambda: detection.single_bidder_contracts()),
        ("contract_splitting", lambda: detection.contract_splitting()),
        ("donor_contracts", detection.political_donor_contracts),
    ]

    for name, func in detectors:
        try:
            query = func()
            if isinstance(query, tuple):
                query, params = query
            else:
                params = {}
            patterns = run_query(query, params)
            results[name] = {"count": len(patterns), "patterns": patterns[:20]}
        except Exception as e:
            logger.error("detection_failed", detector=name, error=str(e))
            results[name] = {"count": 0, "error": str(e), "patterns": []}

    # Compute aggregate risk
    risk = compute_risk_summary(results)
    return {"detections": results, "risk_summary": risk, "timestamp": datetime.utcnow().isoformat()}


# ── Entity details ──────────────────────────────────────────────
@app.get("/person/{person_id}")
def get_person(person_id: str):
    """Get person details with all relationships."""
    person = run_query_single("""
        MATCH (p:Person {person_id: $pid})
        OPTIONAL MATCH (p)-[r]->(target)
        WITH p,
             collect(DISTINCT {
                type: type(r),
                target_label: labels(target)[0],
                name: coalesce(target.name, target.full_name, target.title),
                id: coalesce(target.maticni_broj, target.institution_id, target.contract_id, target.person_id)
             }) AS outgoing
        OPTIONAL MATCH (source)-[r2]->(p)
        RETURN properties(p) AS person,
               outgoing,
               collect(DISTINCT {
                type: type(r2),
                source_label: labels(source)[0],
                name: coalesce(source.name, source.full_name),
                id: coalesce(source.maticni_broj, source.institution_id, source.person_id)
               }) AS incoming
    """, {"pid": person_id})
    if not person:
        raise HTTPException(404, "Person not found")
    return person


@app.get("/company/{maticni_broj}")
def get_company(maticni_broj: str):
    """Get company details with all relationships."""
    company = run_query_single("""
        MATCH (c:Company {maticni_broj: $mb})
        OPTIONAL MATCH (owner:Person)-[:OWNS]->(c)
        OPTIONAL MATCH (director:Person)-[:DIRECTS]->(c)
        OPTIONAL MATCH (c)-[:WON_CONTRACT]->(ct:Contract)
        OPTIONAL MATCH (inst:Institution)-[:AWARDED_CONTRACT]->(ct)
        RETURN properties(c) AS company,
               collect(DISTINCT {name: owner.full_name, id: owner.person_id}) AS owners,
               collect(DISTINCT {name: director.full_name, id: director.person_id}) AS directors,
               collect(DISTINCT {title: ct.title, value: ct.value_rsd, id: ct.contract_id,
                                  date: ct.award_date, institution: inst.name}) AS contracts
    """, {"mb": maticni_broj})
    if not company:
        raise HTTPException(404, "Company not found")
    return company


@app.get("/institution/{institution_id}")
def get_institution(institution_id: str):
    """Get institution details with employees and contracts."""
    inst = run_query_single("""
        MATCH (i:Institution {institution_id: $iid})
        OPTIONAL MATCH (emp:Person)-[:EMPLOYED_BY]->(i)
        OPTIONAL MATCH (i)-[:AWARDED_CONTRACT]->(ct:Contract)
        OPTIONAL MATCH (winner:Company)-[:WON_CONTRACT]->(ct)
        RETURN properties(i) AS institution,
               collect(DISTINCT {name: emp.full_name, id: emp.person_id, role: emp.current_role}) AS employees,
               collect(DISTINCT {title: ct.title, value: ct.value_rsd, id: ct.contract_id,
                                  date: ct.award_date, winner: winner.name}) AS contracts
    """, {"iid": institution_id})
    if not inst:
        raise HTTPException(404, "Institution not found")
    return inst


# ── Sources & graph overview ────────────────────────────────────
@app.get("/stats/sources")
def get_source_stats():
    """Per-source node counts so the UI can show live status."""
    rows = run_query("""
        MATCH (n)
        WHERE n.source IS NOT NULL
        RETURN n.source AS source, count(n) AS count
        ORDER BY count DESC
    """)
    counts = {r["source"]: r["count"] for r in rows}

    # Also get contract count (contracts have source='procurement' or 'seed')
    contract_rows = run_query("""
        CALL () {
            MATCH (n:Company) RETURN count(n) AS companies
        }
        CALL () {
            MATCH (n:Contract) RETURN count(n) AS contracts
        }
        CALL () {
            MATCH (n:Person) RETURN count(n) AS persons
        }
        CALL () {
            MATCH (n:Institution) RETURN count(n) AS institutions
        }
        CALL () {
            MATCH (n:BudgetItem) RETURN count(n) AS budgets
        }
        CALL () {
            MATCH (n:Property) RETURN count(n) AS properties
        }
        RETURN companies, contracts, persons, institutions, budgets, properties
    """)
    totals = contract_rows[0] if contract_rows else {}

    return {"by_source": counts, "totals": totals}


@app.get("/graph/overview")
def get_graph_overview(limit: int = Query(150, ge=10, le=500)):
    """Return a broad overview graph — all nodes + relationships up to limit."""
    nodes = run_query("""
        MATCH (n)
        WHERE n.source IS NOT NULL
        RETURN
            CASE
                WHEN n:Person      THEN n.person_id
                WHEN n:Company     THEN n.maticni_broj
                WHEN n:Institution THEN n.institution_id
                WHEN n:Contract    THEN n.contract_id
                WHEN n:BudgetItem  THEN n.budget_id
                WHEN n:Property    THEN n.property_id
                WHEN n:PoliticalParty THEN n.party_id
                ELSE toString(id(n))
            END AS id,
            CASE
                WHEN n:Person      THEN n.full_name
                WHEN n:Company     THEN n.name
                WHEN n:Institution THEN n.name
                WHEN n:Contract    THEN n.title
                WHEN n:BudgetItem  THEN n.program_name
                WHEN n:PoliticalParty THEN n.name
                ELSE ''
            END AS name,
            labels(n)[0] AS type,
            properties(n) AS props
        LIMIT $limit
    """, {"limit": limit})

    edges = run_query("""
        MATCH (a)-[r]->(b)
        WHERE a.source IS NOT NULL OR b.source IS NOT NULL
        RETURN
            CASE
                WHEN a:Person      THEN a.person_id
                WHEN a:Company     THEN a.maticni_broj
                WHEN a:Institution THEN a.institution_id
                WHEN a:Contract    THEN a.contract_id
                WHEN a:BudgetItem  THEN a.budget_id
                WHEN a:PoliticalParty THEN a.party_id
                ELSE toString(id(a))
            END AS source,
            CASE
                WHEN b:Person      THEN b.person_id
                WHEN b:Company     THEN b.maticni_broj
                WHEN b:Institution THEN b.institution_id
                WHEN b:Contract    THEN b.contract_id
                WHEN b:BudgetItem  THEN b.budget_id
                WHEN b:PoliticalParty THEN b.party_id
                ELSE toString(id(b))
            END AS target,
            type(r) AS relationship,
            properties(r) AS props
        LIMIT $limit
    """, {"limit": limit * 3})

    return {"nodes": nodes, "edges": edges}


@app.get("/graph/suspicious")
def get_suspicious_graph(limit: int = Query(300, ge=10, le=1000)):
    """
    Return only the subgraph of entities that appear in at least one detection pattern.
    Contracts without a flagged company, and companies not linked to any pattern, are excluded.
    This is the 'real-data' view: only shady nodes shown.
    """
    # 1. Collect flagged entity IDs from all active detectors
    flagged_company_mbs: set = set()
    flagged_contract_ids: set = set()
    flagged_institution_ids: set = set()
    flagged_person_ids: set = set()

    detectors = [
        detection.conflict_of_interest,
        detection.ghost_employees,
        detection.shell_company_clusters,
        detection.revolving_door,
        detection.political_donor_contracts,
        lambda: detection.single_bidder_contracts(1_000_000),
        lambda: detection.contract_splitting(15_000_000),
        lambda: detection.repeated_winner(min_contracts=3, min_total_rsd=5_000_000),
        lambda: detection.new_company_big_contract(max_age_years=3, min_value_rsd=5_000_000),
        # Real-data patterns (fire once APR/ACAS enrichment loaded)
        lambda: detection.direct_official_contractor(),
        lambda: detection.ghost_director(),
        lambda: detection.institutional_monopoly(),
        lambda: detection.samododeljivanje_proxy(),
    ]

    for func in detectors:
        try:
            result = func()
            query, params = (result if isinstance(result, tuple) else (result, {}))
            rows = run_query(query, params)
            for row in rows:
                for key, val in row.items():
                    if val is None:
                        continue
                    if "company_mb" in key or "winner_mb" in key:
                        flagged_company_mbs.add(str(val))
                    elif "contract_id" in key:
                        flagged_contract_ids.add(str(val))
                    elif "institution_id" in key:
                        flagged_institution_ids.add(str(val))
                    elif "official_id" in key or "person_id" in key or "family_id" in key:
                        flagged_person_ids.add(str(val))
        except Exception as e:
            logger.warning("suspicious_graph_detector_failed", error=str(e))

    if not any([flagged_company_mbs, flagged_contract_ids, flagged_institution_ids, flagged_person_ids]):
        return {"nodes": [], "edges": [], "flagged_counts": {
            "companies": 0, "contracts": 0, "institutions": 0, "persons": 0,
        }}

    # 1b. Find contracts that connect flagged companies to flagged institutions
    # This ensures the graph has edges between flagged entities (not just isolated nodes)
    if flagged_company_mbs and flagged_institution_ids:
        connecting_contracts = run_query("""
            MATCH (inst:Institution)-[:AWARDED_CONTRACT]->(ct:Contract)<-[:WON_CONTRACT]-(co:Company)
            WHERE inst.institution_id IN $inst_ids AND co.maticni_broj IN $co_mbs
            RETURN ct.contract_id AS contract_id
            LIMIT 200
        """, {"inst_ids": list(flagged_institution_ids), "co_mbs": list(flagged_company_mbs)})
        for row in connecting_contracts:
            if row.get("contract_id"):
                flagged_contract_ids.add(str(row["contract_id"]))

    # 2. Fetch nodes for each flagged set
    def _node_shape(n_alias: str) -> str:
        return f"""
            CASE WHEN {n_alias}:Person      THEN {n_alias}.person_id
                 WHEN {n_alias}:Company     THEN {n_alias}.maticni_broj
                 WHEN {n_alias}:Institution THEN {n_alias}.institution_id
                 WHEN {n_alias}:Contract    THEN {n_alias}.contract_id
                 WHEN {n_alias}:BudgetItem  THEN {n_alias}.budget_id
                 WHEN {n_alias}:PoliticalParty THEN {n_alias}.party_id
                 ELSE toString(id({n_alias}))
            END AS id,
            CASE WHEN {n_alias}:Person      THEN {n_alias}.full_name
                 WHEN {n_alias}:Company     THEN {n_alias}.name
                 WHEN {n_alias}:Institution THEN {n_alias}.name
                 WHEN {n_alias}:Contract    THEN {n_alias}.title
                 WHEN {n_alias}:BudgetItem  THEN {n_alias}.program_name
                 WHEN {n_alias}:PoliticalParty THEN {n_alias}.name
                 ELSE ''
            END AS name,
            labels({n_alias})[0] AS type,
            properties({n_alias}) AS props
        """

    nodes_raw: list = []
    params_n: dict = {}

    if flagged_company_mbs:
        params_n["co_mbs"] = list(flagged_company_mbs)
        nodes_raw += run_query(
            f"MATCH (n:Company) WHERE n.maticni_broj IN $co_mbs RETURN {_node_shape('n')} LIMIT $lim",
            {**params_n, "lim": limit},
        )
    if flagged_contract_ids:
        nodes_raw += run_query(
            f"MATCH (n:Contract) WHERE n.contract_id IN $ct_ids RETURN {_node_shape('n')} LIMIT $lim",
            {"ct_ids": list(flagged_contract_ids), "lim": limit},
        )
    if flagged_institution_ids:
        nodes_raw += run_query(
            f"MATCH (n:Institution) WHERE n.institution_id IN $inst_ids RETURN {_node_shape('n')} LIMIT $lim",
            {"inst_ids": list(flagged_institution_ids), "lim": limit},
        )
    if flagged_person_ids:
        nodes_raw += run_query(
            f"MATCH (n:Person) WHERE n.person_id IN $p_ids RETURN {_node_shape('n')} LIMIT $lim",
            {"p_ids": list(flagged_person_ids), "lim": limit},
        )

    # Deduplicate by id
    seen: set = set()
    nodes: list = []
    for n in nodes_raw:
        if n.get("id") and n["id"] not in seen:
            seen.add(n["id"])
            nodes.append(n)

    # 3. Fetch edges between flagged nodes
    all_ids = list(seen)
    edges: list = []
    if len(all_ids) >= 2:
        edge_node = _node_shape("a").replace("AS id,", "AS source,").replace(
            "AS name,", "AS _a_name,").replace("AS type,", "AS _a_type,").replace("AS props", "AS _a_props")
        edges = run_query("""
            MATCH (a)-[r]->(b)
            WHERE (
                  CASE WHEN a:Company     THEN a.maticni_broj
                       WHEN a:Contract    THEN a.contract_id
                       WHEN a:Institution THEN a.institution_id
                       WHEN a:Person      THEN a.person_id
                       ELSE toString(id(a)) END
            ) IN $all_ids
            AND (
                  CASE WHEN b:Company     THEN b.maticni_broj
                       WHEN b:Contract    THEN b.contract_id
                       WHEN b:Institution THEN b.institution_id
                       WHEN b:Person      THEN b.person_id
                       ELSE toString(id(b)) END
            ) IN $all_ids
            RETURN
                CASE WHEN a:Company     THEN a.maticni_broj
                     WHEN a:Contract    THEN a.contract_id
                     WHEN a:Institution THEN a.institution_id
                     WHEN a:Person      THEN a.person_id
                     ELSE toString(id(a)) END AS source,
                CASE WHEN b:Company     THEN b.maticni_broj
                     WHEN b:Contract    THEN b.contract_id
                     WHEN b:Institution THEN b.institution_id
                     WHEN b:Person      THEN b.person_id
                     ELSE toString(id(b)) END AS target,
                type(r) AS relationship,
                properties(r) AS props
            LIMIT $lim
        """, {"all_ids": all_ids, "lim": limit * 3})

    return {
        "nodes": nodes,
        "edges": edges,
        "flagged_counts": {
            "companies": len(flagged_company_mbs),
            "contracts": len(flagged_contract_ids),
            "institutions": len(flagged_institution_ids),
            "persons": len(flagged_person_ids),
        },
    }


@app.get("/entities")
def list_entities(
    type: str = Query(..., description="Person | Company | Institution | Contract | PoliticalParty"),
    limit: int = Query(100, ge=1, le=500),
    skip: int = Query(0, ge=0),
    q: Optional[str] = Query(None),
):
    """List all entities of a given label, searchable, paginated."""
    label_map = {
        "Person": ("Person", "person_id", "full_name"),
        "Company": ("Company", "maticni_broj", "name"),
        "Institution": ("Institution", "institution_id", "name"),
        "Contract": ("Contract", "contract_id", "title"),
        "PoliticalParty": ("PoliticalParty", "party_id", "name"),
    }
    if type not in label_map:
        raise HTTPException(400, f"Unknown type '{type}'. Use: {', '.join(label_map)}")
    label, id_field, name_field = label_map[type]

    where_clause = "WHERE n.source IS NOT NULL"
    params: dict = {"limit": limit, "skip": skip}
    if q:
        where_clause += " AND toLower(n.name_normalized) CONTAINS toLower($q) OR toLower(n.full_name) CONTAINS toLower($q) OR toLower(n.name) CONTAINS toLower($q) OR toLower(n.title) CONTAINS toLower($q)"
        params["q"] = q.lower()

    rows = run_query(f"""
        MATCH (n:{label})
        {where_clause}
        RETURN
            n.{id_field} AS id,
            n.{name_field} AS name,
            properties(n) AS props
        ORDER BY n.{name_field}
        SKIP $skip LIMIT $limit
    """, params)

    # Get total count
    count_row = run_query_single(f"""
        MATCH (n:{label})
        WHERE n.source IS NOT NULL
        RETURN count(n) AS total
    """)
    total = count_row["total"] if count_row else 0

    return {"type": type, "total": total, "items": rows}


# ── Scraping & loading ──────────────────────────────────────────

def _run_scrape(source: str, since_days: int = 7, years: str = None, force_refresh: bool = False):
    """Run a scraper. Called from background thread or directly."""
    logger.info("scrape_start", source=source)
    try:
        if source in ("rik", "all"):
            from backend.scrapers.rik_scraper import RIKScraper
            s = RIKScraper()
            s.scrape_mps()
            s.client.close()
            logger.info("scrape_done", source="rik")

        if source in ("opendata", "all"):
            from backend.scrapers.opendata_scraper import OpenDataScraper
            s = OpenDataScraper()
            s.fetch_political_parties(force_refresh=force_refresh)
            s.client.close()
            logger.info("scrape_done", source="opendata")

        if source in ("ujn", "all"):
            from backend.scrapers.procurement_bulk_scraper import ProcurementBulkScraper
            s = ProcurementBulkScraper()
            # years=None → uses PROCUREMENT_YEARS env var (default "last:2")
            # max_rows capped: we only want award-decision rows, quality > quantity
            s.scrape(years=years, max_rows=int(os.getenv("PROCUREMENT_MAX_ROWS", "1000")), force_refresh=force_refresh)
            s.client.close()
            logger.info("scrape_done", source="ujn")

        if source in ("gazette", "all"):
            from backend.scrapers.sluzbeni_glasnik_scraper import SluzbeniGlasnikScraper
            s = SluzbeniGlasnikScraper()
            s.scrape_recent(days=since_days)
            s.client.close()
            logger.info("scrape_done", source="gazette")

        if source in ("rgz", "all"):
            # RGZ requires owner name queries — skip live scrape, seed data only
            logger.info("scrape_done", source="rgz")

        if source in ("procurement", "all"):
            from backend.scrapers.procurement_scraper import ProcurementScraper
            s = ProcurementScraper()
            s.scrape_recent(days=since_days)
            s.close()
            logger.info("scrape_done", source="procurement")

        if source in ("jnportal", "all"):
            from backend.scrapers.jnportal_scraper import JNPortalScraper
            s = JNPortalScraper()
            max_rows = int(os.getenv("JNPORTAL_MAX_ROWS", "20000"))
            min_value = float(os.getenv("JNPORTAL_MIN_VALUE", "500000"))
            s.scrape(max_rows=max_rows, min_value=min_value, force_refresh=force_refresh)
            s.client.close()
            logger.info("scrape_done", source="jnportal")

        if source in ("companywall", "apr", "all"):
            from backend.scrapers.companywall_scraper import CompanyWallScraper
            s = CompanyWallScraper()
            max_co = int(os.getenv("CW_MAX_COMPANIES", "300"))
            s.scrape(max_companies=max_co, force_refresh=force_refresh)
            s.client.close()
            logger.info("scrape_done", source="companywall")

        if source in ("op", "otvoreni_parlament", "all"):
            from backend.scrapers.otvoreni_parlament_scraper import OtvoreniParlamentScraper
            s = OtvoreniParlamentScraper()
            s.scrape_all(force_refresh=force_refresh)
            s.close()
            logger.info("scrape_done", source="op")

        if source in ("vlada", "all"):
            from backend.scrapers.vlada_scraper import VladaScraper
            s = VladaScraper()
            s.scrape(force_refresh=force_refresh)
            s.close()
            logger.info("scrape_done", source="vlada")

    except Exception as e:
        logger.error("scrape_failed", source=source, error=str(e))


def _run_load(source: str):
    """Run a loader synchronously."""
    from backend.etl.graph_loader import GraphLoader
    loader = GraphLoader()
    try:
        if source == "apr":
            loader.load_apr_data()
        elif source == "procurement":
            loader.load_procurement_data()
        elif source == "rik":
            loader.load_rik_data()
        elif source == "gazette":
            loader.load_gazette_data()
        elif source == "rgz":
            loader.load_rgz_data()
        elif source == "opendata":
            loader.load_opendata()
            loader.load_parties()
        elif source == "ujn":
            loader.load_ujn_institutions()
            loader.load_ujn_procurements()
        elif source == "jnportal":
            loader.load_jnportal_data()
        elif source in ("companywall", "apr"):
            loader.load_companywall_data()
        elif source in ("op", "otvoreni_parlament"):
            loader.load_op_data()
        elif source == "vlada":
            loader.load_vlada_data()
        else:
            loader.load_all()
    finally:
        loader.resolver.save(os.path.join(os.getenv("DATA_DIR", "./data"), "resolver_state.json"))


@app.post("/scrape/{source}")
def trigger_scrape(
    source: str,
    since_days: int = Query(7, description="Days back for time-based scrapers"),
    years: Optional[str] = Query(None, description="Comma list or 'last:N', e.g. last:3 or 2025,2024"),
    force_refresh: bool = Query(False, description="Ignore cache and re-download"),
):
    """
    Trigger a scrape for the given source.
    source: rik | opendata | gazette | rgz | procurement | all
    Runs in a background thread (non-blocking).
    """
    valid = {"rik", "opendata", "gazette", "rgz", "procurement", "ujn", "jnportal", "companywall", "apr", "op", "otvoreni_parlament", "all"}
    if source not in valid:
        raise HTTPException(400, f"Unknown source '{source}'. Valid: {sorted(valid)}")
    t = threading.Thread(
        target=_run_scrape,
        kwargs={"source": source, "since_days": since_days, "years": years, "force_refresh": force_refresh},
        daemon=True,
    )
    t.start()
    return {"status": "started", "source": source}


@app.post("/load")
def load_all_data():
    """Load all scraped raw data into Neo4j (synchronous)."""
    _run_load("all")
    return {"status": "done", "source": "all"}


@app.post("/load/{source}")
def load_source(source: str):
    """
    Load scraped data for a specific source into Neo4j.
    source: apr | companywall | procurement | rik | gazette | rgz | opendata | deduplicate
    """
    valid = {"apr", "companywall", "procurement", "rik", "gazette", "rgz", "opendata", "ujn", "jnportal", "deduplicate", "op", "otvoreni_parlament", "vlada"}
    if source not in valid:
        raise HTTPException(400, f"Unknown source '{source}'. Valid: {sorted(valid)}")
    if source == "deduplicate":
        from backend.etl.graph_loader import GraphLoader
        loader = GraphLoader()
        merged = loader.deduplicate_institutions()
        return {"status": "done", "source": "deduplicate", "merged": merged}
    _run_load(source)
    return {"status": "done", "source": source}


@app.post("/seed")
def seed_graph():
    """
    Plant synthetic test data covering all 8 detection patterns.
    Safe to re-run (all nodes tagged source='seed').
    """
    from backend.etl.seed_graph import plant_seed_graph, summarize
    result = plant_seed_graph()
    result["graph"] = summarize()
    return result


@app.delete("/seed")
def clear_seed():
    """Remove all seed nodes from the graph."""
    from backend.etl.seed_graph import clear_seed_data
    clear_seed_data()
    return {"status": "cleared"}


@app.post("/ingest/{source}")
def ingest_source(
    source: str,
    since_days: int = Query(7, description="Days back for time-based scrapers"),
    years: Optional[str] = Query(None, description="Comma list or 'last:N', e.g. last:3"),
    force_refresh: bool = Query(False, description="Ignore cache and re-download"),
):
    """
    Scrape + load in one shot for a specific source or all sources.
    source: rik | opendata | gazette | rgz | procurement | ujn | all

    For 'all': scrapes all sources then loads everything into Neo4j.
    This is the recommended way to populate with real data.
    """
    valid = {"rik", "opendata", "gazette", "rgz", "procurement", "ujn", "jnportal", "companywall", "apr", "op", "otvoreni_parlament", "vlada", "all"}
    if source not in valid:
        raise HTTPException(400, f"Unknown source '{source}'. Valid: {sorted(valid)}")

    def _ingest():
        _run_scrape(source, since_days=since_days, years=years, force_refresh=force_refresh)
        load_src = source if source != "all" else "all"
        _run_load(load_src)
        logger.info("ingest_complete", source=source)

    t = threading.Thread(target=_ingest, daemon=True)
    t.start()
    return {
        "status": "started",
        "source": source,
        "params": {"since_days": since_days, "years": years, "force_refresh": force_refresh},
        "message": "Scrape + load running in background. Watch /health or docker logs.",
    }


# ── Export ──────────────────────────────────────────────────────

# Pattern metadata for rich HTML export — mirrors PATTERN_EXPLANATIONS in Dashboard.jsx
_PATTERN_META: dict = {
    "conflict_of_interest": {
        "icon": "⚖", "title": "Sukob interesa",
        "why": (
            "Funkcioner koji direktno odlučuje o dodeli ugovora ima porodičnog člana koji je vlasnik "
            "ili direktor firme koja je dobila taj ugovor od iste institucije. Ovo je klasičan obrazac "
            "korupcije koji narušava princip nepristrasnosti u javnim nabavkama."
        ),
        "how": (
            "(Funkcioner)-[EMPLOYED_BY]->(Institucija)\n"
            "(Institucija)-[AWARDED_CONTRACT]->(Ugovor)\n"
            "(Firma)-[WON_CONTRACT]->(Ugovor)\n"
            "(Porodični član)-[OWNS|DIRECTS]->(Firma)\n"
            "(Funkcioner)-[FAMILY_OF]-(Porodični član)\n\n"
            "Svi čvorovi moraju biti istovremeno prisutni."
        ),
        "sources": [
            ("APR — Registar privrednih subjekata", "https://pretraga.apr.gov.rs"),
            ("Portal javnih nabavki", "https://jnportal.ujn.gov.rs/tender-documents/search"),
            ("Javni funkcioneri — data.gov.rs", "https://data.gov.rs/sr/datasets/funkcioneri-i-javni-sluzbenici/"),
        ],
        "fields": [
            ("official_name", "Funkcioner"), ("official_role", "Pozicija"),
            ("institution", "Institucija"), ("family_member", "Porodični član"),
            ("company_name", "Firma"), ("contract_title", "Ugovor"),
            ("contract_value", "Vrednost ugovora"), ("award_date", "Datum dodele"),
        ],
    },
    "ghost_employee": {
        "icon": "👻", "title": "Fantomski zaposleni",
        "why": (
            "Isto lice pojavljuje se u platnom spisku dve ili više različitih institucija sa različitim "
            "identifikatorima. Ukazuje na lažno zaposlenje ili isplatu plata za nepostojeće radnike."
        ),
        "how": (
            "(P1:Person {name_normalized: X})-[EMPLOYED_BY]->(I1)\n"
            "(P2:Person {name_normalized: X})-[EMPLOYED_BY]->(I2)\n"
            "gde P1.person_id != P2.person_id i I1 != I2"
        ),
        "sources": [
            ("Javni funkcioneri — data.gov.rs", "https://data.gov.rs/sr/datasets/funkcioneri-i-javni-sluzbenici/"),
            ("Službeni glasnik", "https://www.pravno-informacioni-sistem.rs/SlGlasnikPortal/eli/collection"),
            ("Poslanici — Parlament RS", "https://www.parlament.gov.rs/members-of-parliament"),
        ],
        "fields": [
            ("name_1", "Ime (evidencija 1)"), ("institution_1", "Institucija 1"),
            ("name_2", "Ime (evidencija 2)"), ("institution_2", "Institucija 2"),
            ("normalized_name", "Normalizovano ime"),
        ],
    },
    "shell_company_cluster": {
        "icon": "🐚", "title": "Klaster shell kompanija",
        "why": (
            "Tri ili više firmi registrovanih na istoj adresi kolektivno osvajaju javne ugovore. "
            "Čest mehanizam za rasipanje ugovora između povezanih firmi radi zaobilaženja pragova nabavki."
        ),
        "how": (
            "(Adresa)<-[REGISTERED_AT]-(C1)\n"
            "(Adresa)<-[REGISTERED_AT]-(C2)\n"
            "(Adresa)<-[REGISTERED_AT]-(C3...)\n"
            "Gde svaka kompanija ima WON_CONTRACT odnos.\n"
            "Suma vrednosti svih ugovora = ukupna izloženost."
        ),
        "sources": [
            ("APR — Registar privrednih subjekata", "https://pretraga.apr.gov.rs"),
            ("Portal javnih nabavki", "https://jnportal.ujn.gov.rs/tender-documents/search"),
        ],
        "fields": [
            ("address", "Zajednička adresa"), ("city", "Grad"),
            ("num_companies", "Broj firmi"), ("num_contracts", "Broj ugovora"),
            ("total_value", "Ukupna vrednost"),
        ],
    },
    "single_bidder": {
        "icon": "1️⃣", "title": "Ugovor sa jednim ponuđačem",
        "why": (
            "Javna nabavka primila je samo jednu ponudu, što drastično smanjuje konkurenciju. "
            "Posebno sumnjivo kada se ponavlja sa istom firmom ili institucijom, ili kada je vrednost visoka. "
            "Proc tip 3 ili 9 u srpskom zakonu = pregovarački postupak bez prethodnog objavljivanja — "
            "konkurentno nadmetanje je zaobiđeno."
        ),
        "how": (
            "(Institucija)-[AWARDED_CONTRACT]->(Ugovor {num_bidders: 1})\n"
            "(Firma)-[WON_CONTRACT]->(Ugovor)\n"
            "gde Ugovor.value_rsd >= prag (podrazumevano 2.000.000 RSD)"
        ),
        "sources": [
            ("Portal javnih nabavki", "https://jnportal.ujn.gov.rs/tender-documents/search"),
        ],
        "fields": [
            ("contract_title", "Naziv ugovora"), ("value_rsd", "Vrednost"),
            ("award_date", "Datum dodele"), ("institution", "Naručilac"),
            ("winner", "Pobednik"), ("proc_type", "Vrsta nabavke"),
            ("directors", "Direktor(i) firme"),
        ],
    },
    "revolving_door": {
        "icon": "🔄", "title": "Rotirajuća vrata",
        "why": (
            "Bivši državni funkcioner napustio je instituciju i preuzeo rukovodeću poziciju u privatnoj firmi "
            "koja potom dobija ugovore od te iste institucije. Lice koristi insajderska znanja i poslovne "
            "kontakte stečene tokom rada u državnoj upravi."
        ),
        "how": (
            "(Osoba)-[EMPLOYED_BY {until: datum}]->(Institucija)\n"
            "(Osoba)-[DIRECTS|OWNS {since: datum >= until}]->(Firma)\n"
            "OPCIONALNO:\n"
            "(Institucija)-[AWARDED_CONTRACT]->(Ugovor)\n"
            "(Firma)-[WON_CONTRACT]->(Ugovor)"
        ),
        "sources": [
            ("Službeni glasnik", "https://www.pravno-informacioni-sistem.rs/SlGlasnikPortal/eli/collection"),
            ("APR — Registar privrednih subjekata", "https://pretraga.apr.gov.rs"),
            ("Portal javnih nabavki", "https://jnportal.ujn.gov.rs/tender-documents/search"),
        ],
        "fields": [
            ("person_name", "Osoba"), ("former_institution", "Bivša institucija"),
            ("govt_role", "Bivša pozicija"), ("left_govt", "Datum odlaska"),
            ("company_name", "Nova firma"), ("company_role", "Nova pozicija"),
            ("joined_company", "Datum ulaska"), ("contracts_between", "Ugovora između"),
            ("total_contract_value", "Ukupna vrednost"),
        ],
    },
    "budget_self_allocation": {
        "icon": "💰", "title": "Samododeljivanje budžeta",
        "why": (
            "Funkcioner je odobrio budžetsku stavku, a ugovor finansiran iz te stavke dobila je firma "
            "sa kojom ima porodičnu ili vlasničku vezu. Direktni sukob interesa na nivou budžetskog procesa."
        ),
        "how": (
            "(Osoba)-[ALLOCATED_BY]-(BudžetStavka)\n"
            "(BudžetStavka)-[FUNDS]->(Ugovor)\n"
            "(Firma)-[WON_CONTRACT]->(Ugovor)\n"
            "Gde postoji put dužine 1-3 između Osobe i Firme\n"
            "kroz FAMILY_OF, OWNS ili DIRECTS odnose."
        ),
        "sources": [
            ("Budžet RS — Ministarstvo finansija", "https://www.mfin.gov.rs/dokumenti/budzet/"),
            ("Portal javnih nabavki", "https://jnportal.ujn.gov.rs/tender-documents/search"),
            ("APR — Registar privrednih subjekata", "https://pretraga.apr.gov.rs"),
        ],
        "fields": [
            ("allocator", "Odobrio budžet"), ("budget_item", "Budžetska stavka"),
            ("amount", "Iznos"), ("contract_title", "Ugovor"),
            ("beneficiary_company", "Korisnik"),
        ],
    },
    "contract_splitting": {
        "icon": "✂", "title": "Deljenje ugovora",
        "why": (
            "Ista firma dobija više ugovora od iste institucije u kratkom vremenskom periodu, pri čemu su svi "
            "ispod zakonskog praga za obaveznu javnu licitaciju. Zbir vrednosti prelazi prag — klasičan način "
            "zaobilaženja procedure. Zakon o javnim nabavkama zabranjuje veštačko deljenje predmeta nabavke."
        ),
        "how": (
            "(Institucija)-[AWARDED_CONTRACT]->(CT1, CT2...)\n"
            "(Firma)-[WON_CONTRACT]->(CT1, CT2...)\n"
            "gde: CT.value_rsd < prag (npr. 6M)\n"
            "  i: count >= 2\n"
            "  i: svi ugovori u roku od 90 dana"
        ),
        "sources": [
            ("Portal javnih nabavki", "https://jnportal.ujn.gov.rs/tender-documents/search"),
        ],
        "fields": [
            ("institution", "Institucija"), ("company_name", "Firma"),
            ("num_contracts", "Broj ugovora"), ("total_value", "Ukupna vrednost"),
            ("first_date", "Prvi ugovor"), ("last_date", "Poslednji ugovor"),
        ],
    },
    "political_donor_contract": {
        "icon": "🤝", "title": "Donator stranke → Ugovor",
        "why": (
            "Firma koja je finansirala političku stranku osvaja javne ugovore od institucija kojima rukovode "
            "članovi te stranke. Obrazac poznat kao 'pay-to-play' — donacija kao investicija u buduće ugovore."
        ),
        "how": (
            "(Firma)-[DONATED_TO]->(PolitičkaStranka)\n"
            "(Firma)-[WON_CONTRACT]->(Ugovor)\n"
            "(Institucija)-[AWARDED_CONTRACT]->(Ugovor)\n"
            "OPCIONALNO:\n"
            "(Osoba)-[MEMBER_OF]->(PolitičkaStranka)\n"
            "(Osoba)-[EMPLOYED_BY]->(Institucija)"
        ),
        "sources": [
            ("Finansiranje stranaka — ACAS", "https://www.acas.rs/finansiranje-politickih-subjekata/"),
            ("APR — Registar privrednih subjekata", "https://pretraga.apr.gov.rs"),
            ("Portal javnih nabavki", "https://jnportal.ujn.gov.rs/tender-documents/search"),
        ],
        "fields": [
            ("donor_company", "Donator"), ("party_name", "Stranka"),
            ("donation_amount", "Iznos donacije"), ("contract_title", "Dobijeni ugovor"),
            ("contract_value", "Vrednost ugovora"), ("awarding_institution", "Institucija"),
            ("party_member_in_institution", "Član stranke u instituciji"),
        ],
    },
    "repeated_winner": {
        "icon": "🏆", "title": "Stalni pobednik",
        "why": (
            "Ista firma pobeđuje na javnim nabavkama kod iste institucije više puta zaredom, osvajajući "
            "dominantan deo njenog ukupnog budžeta za nabavke. U zdravom sistemu, različite firme trebalo bi "
            "da pobede u različitim raspisima — stalno isti pobednik ukazuje na sistemsko zarobljavanje "
            "nabavnog procesa, pisanje konkursa po meri firme, ili korupciju evaluacione komisije.\n\n"
            "Zakon o javnim nabavkama Srbije propisuje princip konkurentnosti — ponavljano osvajanje od "
            "jednog ponuđača je signal za reviziju i istragu."
        ),
        "how": (
            "(Firma)-[WON_CONTRACT]->(CT1, CT2, CT3...)\n"
            "(Institucija)-[AWARDED_CONTRACT]->(CT1, CT2, CT3...)\n"
            "gde: count(ugovori firma/institucija) >= 3\n"
            "  i: firma.ugovori / institucija.ukupno_ugovora >= 50%"
        ),
        "sources": [
            ("Portal javnih nabavki", "https://jnportal.ujn.gov.rs/tender-documents/search"),
        ],
        "fields": [
            ("company_name", "Stalni pobednik"), ("institution", "Institucija"),
            ("directors", "Direktor(i) firme"),
            ("company_founded", "Datum osnivanja firme"),
            ("num_contracts", "Broj pobeda"), ("total_value", "Ukupna vrednost"),
            ("first_win", "Prva pobeda"), ("last_win", "Poslednja pobeda"),
            ("share_pct", "% budžeta institucije"),
        ],
    },
    "new_company_big_contract": {
        "icon": "🆕", "title": "Nova firma — veliki ugovor",
        "why": (
            "Novoosnovana firma (mlađa od 3 godine) osvaja javne ugovore visoke vrednosti bez dokazanog "
            "iskustva i poslovne istorije. Zakon o javnim nabavkama zahteva od ponuđača dokaze o referentnim "
            "ugovorima i finansijskom kapacitetu — pa se nameće pitanje kako firma bez istorije ispunjava "
            "te uslove.\n\n"
            "Karakteristični scenariji:\n"
            "• Firma osnovana neposredno pre raspisivanja konkursa — napravljena posebno za taj tender\n"
            "• Ishodišna firma: postojeći direktor osniva novu firmu i 'prebacuje' ugovore na nju\n"
            "• Politički podobna firma: veze ka stranci koja kontroliše instituciju\n"
            "• 'Školjka': nova firma nema zaposlenih — posao obavljaju kooperanti"
        ),
        "how": (
            "(Firma)-[:WON_CONTRACT]->(Ugovor)\n"
            "gde: Firma.founding_date IS NOT NULL\n"
            "  i: award_year - founding_year <= 3\n"
            "  i: contract_value >= 2.000.000 RSD\n\n"
            "Severity:\n"
            "  age = 0 (ista godina osnivanja): CRITICAL\n"
            "  age <= 1 + value >= 5M: CRITICAL\n"
            "  age <= 2: HIGH\n"
            "  age = 3: MEDIUM"
        ),
        "sources": [
            ("APR — Registar privrednih subjekata", "https://pretraga.apr.gov.rs"),
            ("Portal javnih nabavki", "https://jnportal.ujn.gov.rs/tender-documents/search"),
        ],
        "fields": [
            ("company_name", "Firma"), ("founded", "Datum osnivanja"),
            ("directors", "Direktor(i) firme"),
            ("age_at_award", "Starost firme (god.)"), ("num_contracts", "Broj ugovora"),
            ("total_value", "Ukupna vrednost"), ("contract_title", "Najveći ugovor"),
            ("contract_value", "Vrednost najvećeg"), ("award_date", "Datum dodele"),
            ("institution", "Naručilac"),
        ],
    },
    "samododeljivanje_proxy": {
        "icon": "🏛", "title": "Poslanik/Funkcioner — direktor firme koja dobija ugovore",
        "why": (
            "Narodni poslanik ili javni funkcioner istovremeno obavlja direktorsku funkciju u firmi koja "
            "osvaja javne ugovore. Ovo je direktni sukob interesa — zakon o javnim nabavkama zahteva "
            "nepristrasnost, ali lice koje kontroliše firmu može koristiti politički uticaj da osigura "
            "ugovore.\n\n"
            "Primer: Dušan Bajatović (poslanik SPS) generalni je direktor JP Srbijagasa, koji dobija "
            "ugovore vredne milijarde RSD."
        ),
        "how": (
            "(Poslanik/Funkcioner)-[EMPLOYED_BY]->(Skupština/Vlada)\n"
            "(Poslanik/Funkcioner)-[DIRECTS]->(Firma)\n"
            "(Firma)-[WON_CONTRACT]->(Ugovor)\n"
            "(BilokojInstitucija)-[AWARDED_CONTRACT]->(Ugovor)\n\n"
            "Ne zahteva da institucija koja zapošljava bude ista koja dodeljuje ugovor —\n"
            "politički uticaj deluje posredno."
        ),
        "sources": [
            ("Poslanici — Parlament RS", "https://www.parlament.gov.rs/members-of-parliament"),
            ("APR — Registar privrednih subjekata", "https://pretraga.apr.gov.rs"),
            ("Portal javnih nabavki", "https://jnportal.ujn.gov.rs/tender-documents/search"),
        ],
        "fields": [
            ("official_name", "Poslanik/Funkcioner"), ("official_role", "Javna pozicija"),
            ("employer_institution", "Institucija"),
            ("company_name", "Firma kojom rukovodi"),
            ("contract_title", "Ugovor"), ("contract_value", "Vrednost ugovora"),
            ("awarding_institution", "Naručilac"), ("award_date", "Datum dodele"),
        ],
    },
    "direct_official_contractor": {
        "icon": "⚡", "title": "Funkcioner direktno na obe strane ugovora",
        "why": (
            "Isti funkcioner je zaposlen u instituciji koja dodeljuje ugovor I istovremeno rukovodi firmom "
            "koja taj ugovor dobija — bez posrednika. Najdirektniji oblik sukoba interesa: ista osoba "
            "kontroliše i naručioca i dobavljača."
        ),
        "how": (
            "(Funkcioner)-[EMPLOYED_BY]->(Institucija)\n"
            "(Institucija)-[AWARDED_CONTRACT]->(Ugovor)\n"
            "(Firma)-[WON_CONTRACT]->(Ugovor)\n"
            "(Funkcioner)-[DIRECTS|OWNS]->(Firma)\n\n"
            "Svi elementi moraju biti isti — Funkcioner je i kod naručioca i u firmi pobedniku."
        ),
        "sources": [
            ("Javni funkcioneri — data.gov.rs", "https://data.gov.rs/sr/datasets/funkcioneri-i-javni-sluzbenici/"),
            ("APR — Registar privrednih subjekata", "https://pretraga.apr.gov.rs"),
            ("Portal javnih nabavki", "https://jnportal.ujn.gov.rs/tender-documents/search"),
        ],
        "fields": [
            ("official_name", "Funkcioner"), ("official_role", "Pozicija"),
            ("institution", "Institucija naručilac"), ("company_name", "Firma"),
            ("contract_title", "Ugovor"), ("contract_value", "Vrednost"),
            ("award_date", "Datum"),
        ],
    },
    "ghost_director": {
        "icon": "👤", "title": "Fantomski direktor",
        "why": (
            "Lice je formalno direktor više firmi koje zajedno osvajaju ugovore od iste institucije — "
            "ali nema fizičku mogućnost da stvarno rukovodi svima. Čest obrazac pri korišćenju 'front' "
            "kompanija: nominalni direktor potpisuje dokumenta, a stvarni vlasnik ostaje u senci."
        ),
        "how": (
            "(Osoba)-[DIRECTS]->(Firma1)\n"
            "(Osoba)-[DIRECTS]->(Firma2)\n"
            "(Firma1)-[WON_CONTRACT]->(Ugovor1)\n"
            "(Firma2)-[WON_CONTRACT]->(Ugovor2)\n"
            "(IstaInstitucija)-[AWARDED_CONTRACT]->(Ugovor1)\n"
            "(IstaInstitucija)-[AWARDED_CONTRACT]->(Ugovor2)\n\n"
            "Najmanje 2 firme, iste institucije."
        ),
        "sources": [
            ("APR — Registar privrednih subjekata", "https://pretraga.apr.gov.rs"),
            ("Portal javnih nabavki", "https://jnportal.ujn.gov.rs/tender-documents/search"),
        ],
        "fields": [
            ("director_name", "Direktor"), ("institution", "Institucija"),
            ("num_companies", "Broj firmi"), ("total_value", "Ukupna vrednost"),
        ],
    },
    "institutional_monopoly": {
        "icon": "🏛", "title": "Institucionalni monopol",
        "why": (
            "Jedna firma prima 70% ili više celokupnog budžeta javnih nabavki jedne institucije. "
            "Ovo nije slučajnost — ukazuje na sistemsko zarobljavanje nabavnog procesa:\n\n"
            "• Konkursna dokumentacija konstruisana tako da samo jedna firma može ispuniti uslove\n"
            "• Evaluatori imaju uputstvo ili implicitni pritisak da biraju unapred određenog pobednika\n"
            "• Institucija je u neformalno zavisnom odnosu sa firmom\n"
            "• Ostale firme su naučile da ne apliciraju jer znaju da nemaju šanse\n\n"
            "Prema EU standardima i SIGMA metodologiji, koncentracija > 50% kod jednog dobavljača "
            "za jednu instituciju se klasifikuje kao crvena zastavica za korupciju."
        ),
        "how": (
            "(Institucija)-[AWARDED_CONTRACT]->(Ugovor)\n"
            "(Firma)-[WON_CONTRACT]->(Ugovor)\n\n"
            "AGREGACIJA:\n"
            "  udeo = firma_vrednost / institucija_ukupno\n\n"
            "USLOV:\n"
            "  udeo >= 0.60  (firma dobija >= 60% budžeta)\n"
            "  firma_vrednost >= 2.000.000 RSD"
        ),
        "sources": [
            ("Portal javnih nabavki (UJN)", "https://jnportal.ujn.gov.rs/tender-documents/search"),
        ],
        "fields": [
            ("institution", "Institucija"), ("company_name", "Dominantni dobavljač"),
            ("directors", "Direktor(i) firme"),
            ("company_founded", "Datum osnivanja firme"),
            ("company_pct_of_institution", "Udeo u budžetu (%)"),
            ("company_total_value", "Vrednost ugovora firme"),
            ("institution_total_value", "Ukupan budžet institucije"),
            ("num_contracts", "Broj ugovora"),
        ],
    },
    "value_just_below_threshold": {
        "icon": "🎯", "title": "Nameštanje vrednosti ispod praga",
        "why": (
            "Firma prima više ugovora čija vrednost je sistematski postavljena tik ispod zakonskih pragova "
            "za sprovođenje javnog tendera. Ovo nije slučajnost — istraživanje Transparentnosti Srbije (2024) "
            "pokazuje da čak 31,27% nabavki dobara/usluga pada u zonu 900.000–1.000.000 RSD, što je "
            "statistički nemoguće bez nameštanja.\n\n"
            "Srpski pragovi:\n"
            "• 1.000.000 RSD — ispod ovoga direktni sporazum bez tendera\n"
            "• 3.000.000 RSD — za radove: ispod ovoga direktni sporazum\n"
            "• 5.000.000 RSD — ispod ovoga pojednostavljeni postupak\n"
            "• 15.000.000 RSD — ispod ovoga ne mora se raspisati puni otvoreni tender\n\n"
            "Dokumentovani slučaj: Firma 'Avenija sistem' dobila je 5 ugovora za radove od 5 različitih "
            "obrazovnih institucija, sve u rasponu 2.948.000–2.999.250 RSD (tik ispod 3M praga). "
            "Državni sekretar uhapšen je zbog organizovanja ove sheme."
        ),
        "how": (
            "(Institucija)-[AWARDED_CONTRACT]->(Ugovor)\n"
            "(Firma)-[WON_CONTRACT]->(Ugovor)\n"
            "gde: Ugovor.value_rsd IN [850K–1M, 2.7M–3M, 4.25M–5M, 12.75M–15M]\n\n"
            "count(takvih ugovora, ista firma) >= 2"
        ),
        "sources": [
            ("Portal javnih nabavki", "https://jnportal.ujn.gov.rs/tender-documents/search"),
            ("Transparentnost Srbija — istraživanje pragova (2024)",
             "https://www.transparentnost.org.rs/index.php/sr/"),
        ],
        "fields": [
            ("company_name", "Firma"), ("institution", "Naručilac"),
            ("num_contracts", "Br. sumnjivih ugovora"), ("total_value", "Ukupna vrednost"),
        ],
    },
    "procurement_law_violation": {
        "icon": "⚖", "title": "Kršenje zakona o nabavkama — tip 11 iznad praga",
        "why": (
            "Ugovor je evidentiran kao 'jednostavna nabavka' (tip postupka 11), koja je zakonski "
            "dozvoljena SAMO za vrednosti do 1.000.000 RSD (Zakon o javnim nabavkama, čl. 27). "
            "Svaki ugovor iznad ovog praga, kodiran kao tip 11, predstavlja direktno kršenje zakona "
            "ili namerno falsifikovanje evidencije u sistemu javnih nabavki.\n\n"
            "Najteži slučajevi (> 100M RSD) mogu ukazivati na organizovanu prevaru — namerno "
            "klasifikovanje milijarderskih ugovora kao 'malih nabavki' da bi se izbegao javni tender."
        ),
        "how": (
            "(Institucija)-[AWARDED_CONTRACT]->(Ugovor)\n"
            "(Firma)-[WON_CONTRACT]->(Ugovor)\n"
            "gde: Ugovor.proc_type = '11' AND Ugovor.value_rsd > 1.000.000 RSD\n\n"
            "Sortiran po vrednosti — veće vrednosti = teže kršenje"
        ),
        "sources": [
            ("Portal javnih nabavki", "https://jnportal.ujn.gov.rs/tender-documents/search"),
        ],
        "fields": [
            ("institution", "Institucija"), ("company_name", "Firma"),
            ("value_rsd", "Vrednost ugovora"), ("award_date", "Datum"),
            ("contract_title", "Predmet"), ("directors", "Direktor(i) firme"),
        ],
    },
    "institution_threshold_cluster": {
        "icon": "🎯", "title": "Institucija sistematski koristi pragove",
        "why": (
            "Institucija dodeljuje veliki broj ugovora čija vrednost pada tik ispod zakonskih pragova, "
            "ali različitim firmama. Za razliku od klasičnog deljenja ugovora (ista firma), ovde "
            "institucija sistematski 'raspoređuje' posao ispod praga — moguće između povezanih firmi. "
            "Istraživanje TS Srbija (2024): 31.27% nabavki pada u zonu 900K–1M RSD."
        ),
        "how": (
            "(Institucija)-[AWARDED_CONTRACT]->(Ugovor)\n"
            "gde: Ugovor.value_rsd IN [850K–1M, 2.7M–3M, 4.25M–5M, 12.75M–15M]\n\n"
            "count(takvih ugovora od iste institucije) >= 3"
        ),
        "sources": [
            ("Portal javnih nabavki", "https://jnportal.ujn.gov.rs/tender-documents/search"),
            ("Transparentnost Srbija — istraživanje pragova (2024)",
             "https://www.transparentnost.org.rs/index.php/sr/"),
        ],
        "fields": [
            ("institution", "Institucija"), ("num_contracts", "Br. ugovora ispod praga"),
            ("num_companies", "Br. različitih firmi"), ("total_value", "Ukupna vrednost"),
        ],
    },
    "zero_competition_repeat": {
        "icon": "🚫", "title": "Višestruki ugovori bez konkurencije",
        "why": (
            "Ista institucija dodeljuje isti firmi više ugovora putem pregovaračkog postupka bez "
            "prethodnog objavljivanja (tip 3) ili direktnog sporazuma (tip 9). Svaki slučaj posebno "
            "je sumnjiv — ponavljanje je znak sistemskog zaobilaženja konkurencije.\n\n"
            "Srpski Zakon o javnim nabavkama dozvoljava pregovarački postupak samo u izuzetnim "
            "okolnostima (hitnost, jedinstvenost, bezbednost). Ponavlja li se 'izuzetak' uvek "
            "sa istom firmom, radi se o zloupotrebi izuzetka."
        ),
        "how": (
            "(Institucija)-[AWARDED_CONTRACT]->(Ugovor)\n"
            "(Firma)-[WON_CONTRACT]->(Ugovor)\n"
            "gde: Ugovor.proc_type IN ['3', '9'] ILI Ugovor.num_bidders = 1\n\n"
            "count(takvih ugovora) >= 2 za isti par Institucija–Firma"
        ),
        "sources": [
            ("Portal javnih nabavki", "https://jnportal.ujn.gov.rs/tender-documents/search"),
        ],
        "fields": [
            ("institution", "Institucija"), ("company_name", "Firma"),
            ("num_contracts", "Br. bez-konkurencije ugovora"), ("total_value", "Ukupna vrednost"),
        ],
    },
    "distributed_evasion": {
        "icon": "🕸", "title": "Raspoređeno izbegavanje nadzora",
        "why": (
            "Firma osvaja male ugovore od velikog broja različitih institucija — svaki pojedinacan "
            "ugovor je ispod praga koji privlači pažnju, ali zbir svih ugovora je značajan. "
            "Umesto jednog velikog ugovora koji bi bio vidljiv u statistikama, firma koristi "
            "mrežu malih ugovora koji se 'gube' u sistemu."
        ),
        "how": (
            "(Firma)-[WON_CONTRACT]->(Ugovor1, Ugovor2...)\n"
            "(Institucija1, Institucija2, ...)-[AWARDED_CONTRACT]->(Ugorovi)\n\n"
            "count(DISTINCT institucija) >= 3\n"
            "prosečna vrednost po instituciji < 40% ukupne vrednosti\n"
            "ukupna vrednost >= 2.000.000 RSD"
        ),
        "sources": [
            ("Portal javnih nabavki", "https://jnportal.ujn.gov.rs/tender-documents/search"),
        ],
        "fields": [
            ("company_name", "Firma"), ("num_institutions", "Broj institucija"),
            ("total_contracts", "Ukupno ugovora"), ("total_value", "Ukupna vrednost"),
            ("avg_per_institution", "Prosek po instituciji"),
        ],
    },
    "party_linked_contractor": {
        "icon": "🎖", "title": "Stranački kontakti — ugovor",
        "why": (
            "Narodni poslanik ili stranačko lice rukovodi firmom koja dobija ugovor od institucije "
            "gde je zaposlen drugi član iste stranke. Oba kraja lanca (naručilac i dobavljač) "
            "kontrolisana su od strane iste stranke — klasičan 'stranački zarobljeni tender'."
        ),
        "how": (
            "(MP/Član stranke A)-[OWNS|DIRECTS]->(Firma)\n"
            "(Firma)-[WON_CONTRACT]->(Ugovor)\n"
            "(Institucija)-[AWARDED_CONTRACT]->(Ugovor)\n"
            "(Funkcioner stranke A)-[EMPLOYED_BY]->(Institucija)\n\n"
            "Obe osobe moraju biti u istoj stranci; ne mogu biti ista osoba."
        ),
        "sources": [
            ("Otvoreni Parlament", "https://otvoreniparlamet.rs"),
            ("APR — Registar privrednih subjekata", "https://pretraga.apr.gov.rs"),
            ("Portal javnih nabavki", "https://jnportal.ujn.gov.rs/tender-documents/search"),
        ],
        "fields": [
            ("mp_name", "Poslanik/Član stranke"), ("party_name", "Stranka"),
            ("company_name", "Firma"), ("contract_title", "Ugovor"),
            ("contract_value", "Vrednost"), ("institution", "Naručilac"),
            ("party_official_at_institution", "Stranački funkcioner u instituciji"),
        ],
    },
    "suspicious_price_concentration": {
        "icon": "📊", "title": "Sektorski monopol",
        "why": (
            "Jedna firma osvaja 60%+ svih ugovora u svom sektoru delatnosti kod jedne institucije. "
            "U zdravom tržištu, različite firme iste delatnosti trebalo bi da ravnomerno učestvuju "
            "na tenderima. Dominacija jedne firme u specifičnom sektoru ukazuje da je tender "
            "pisan po meri te firme ili da postoji ekskluzivni aranžman."
        ),
        "how": (
            "(Firma)-[:WON_CONTRACT]->(Ugovor)\n"
            "(Institucija)-[:AWARDED_CONTRACT]->(Ugovor)\n\n"
            "GROUP BY delatnost_firme (APR šifra delatnosti)\n"
            "firma_vrednost / ukupno_sektor_institucija >= 60%\n"
            "USLOV: >= 3 konkurenta u sektoru"
        ),
        "sources": [
            ("APR — Šifrarnik delatnosti", "https://pretraga.apr.gov.rs"),
            ("Portal javnih nabavki", "https://jnportal.ujn.gov.rs/tender-documents/search"),
        ],
        "fields": [
            ("company_name", "Firma"), ("institution", "Institucija"),
            ("activity_code", "Šifra delatnosti"), ("num_contracts", "Broj ugovora"),
            ("sector_pct", "% sektora institucije"), ("company_value", "Vrednost firme"),
        ],
    },
}

_PATTERN_ALIASES: dict = {
    "conflicts": "conflict_of_interest",
    "ghosts": "ghost_employee",
    "shells": "shell_company_cluster",
    "budget_allocation": "budget_self_allocation",
    "donor_contracts": "political_donor_contract",
}

_PROC_TYPE_LABELS: dict = {
    "1": "Otvoreni postupak",
    "2": "Restriktivni postupak",
    "3": "Pregovarački bez objavljivanja poziva ⚑",
    "4": "Takmičarski dijalog",
    "5": "Partnerstvo za inovacije",
    "6": "Okvirni sporazum",
    "7": "Sistem dinamične nabavke",
    "8": "Konkurs za dizajn",
    "9": "Direktni sporazum / Jednostavna nabavka ⚑",
}


def _fmt_rsd(val) -> str:
    try:
        n = float(val)
        if n >= 1_000_000_000:
            return f"{n / 1_000_000_000:.2f}B RSD"
        if n >= 1_000_000:
            return f"{n / 1_000_000:.1f}M RSD"
        if n >= 1_000:
            return f"{n / 1_000:.0f}K RSD"
        return f"{int(n):,} RSD"
    except Exception:
        return str(val)


def _is_seed(finding: dict) -> bool:
    seed_fields = ["official_id", "family_id", "company_mb", "winner_mb",
                   "contract_id", "person_id", "institution_id", "director_id"]
    return any(
        str(finding.get(f, "")).upper().startswith(
            ("PERSON-SEED", "MB-SEED", "CT-SEED", "INST-SEED", "ADDR-SEED")
        )
        for f in seed_fields
        if finding.get(f)
    )


@app.get("/export/findings")
def export_findings(format: str = "json", exclude_seed: bool = False):
    """
    Export all detection findings.
    format: json | csv | html
    exclude_seed: if true, strip synthetic seed/test data from results
    """
    from backend.queries import detection as det
    results = {}
    detectors = det.ALL_DETECTORS + [
        ("single_bidder", lambda: det.single_bidder_contracts()),
        ("contract_splitting", lambda: det.contract_splitting()),
        ("donor_contracts", det.political_donor_contracts),
    ]
    for name, func in detectors:
        try:
            query = func()
            if isinstance(query, tuple):
                query, params = query
            else:
                params = {}
            patterns = run_query(query, params)
            results[name] = patterns
        except Exception as e:
            logger.error("export_detection_failed", detector=name, error=str(e))
            results[name] = []

    all_findings = []
    for pattern_name, patterns in results.items():
        for p in patterns:
            all_findings.append({**p, "pattern_type": p.get("pattern_type", pattern_name)})

    if exclude_seed:
        all_findings = [f for f in all_findings if not _is_seed(f)]

    if format == "csv":
        fieldnames = [
            "pattern_type", "severity", "official_name", "institution", "family_member",
            "company_name", "contract_title", "contract_value", "value_rsd", "total_value",
            "award_date", "winner", "person_name", "party_name", "donation_amount",
            "num_contracts", "address", "normalized_name", "verification_url", "contract_id",
        ]
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        for finding in all_findings:
            writer.writerow(finding)
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=findings_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.csv"},
        )

    elif format == "html":
        risk = det.compute_risk_summary({k: {"patterns": v} for k, v in results.items()})
        html = _build_html_report(all_findings, risk, exclude_seed)
        return HTMLResponse(content=html)

    else:  # json
        risk = det.compute_risk_summary({k: {"patterns": v} for k, v in results.items()})
        return {
            "exported_at": datetime.utcnow().isoformat(),
            "total_findings": len(all_findings),
            "exclude_seed": exclude_seed,
            "risk_summary": risk,
            "findings": all_findings,
            "sources": {
                "apr": "https://pretraga.apr.gov.rs",
                "procurement": "https://jnportal.ujn.gov.rs",
                "officials": "https://data.gov.rs/sr/datasets/funkcioneri-i-javni-sluzbenici/",
                "party_financing": "https://www.acas.rs/finansiranje-politickih-subjekata/",
                "gazette": "https://www.pravno-informacioni-sistem.rs/SlGlasnikPortal/eli/collection",
                "parliament": "https://www.parlament.gov.rs/members-of-parliament",
            },
        }


def _build_html_report(findings: list, risk: dict, exclude_seed: bool = False) -> str:
    """Build a detailed printable HTML report mirroring the Upozorenja detail panel."""
    SEV_PALETTE = {
        "critical": {"bg": "#fef2f2", "border": "#dc2626", "text": "#dc2626", "badge": "#fee2e2"},
        "high":     {"bg": "#fff7ed", "border": "#f97316", "text": "#c2410c", "badge": "#ffedd5"},
        "medium":   {"bg": "#fefce8", "border": "#ca8a04", "text": "#a16207", "badge": "#fef9c3"},
        "low":      {"bg": "#f9fafb", "border": "#6b7280", "text": "#4b5563", "badge": "#f3f4f6"},
    }
    SEV_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    sorted_findings = sorted(findings, key=lambda f: SEV_ORDER.get(f.get("severity", "low"), 9))

    def _esc(s: str) -> str:
        return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")

    def _card(idx: int, f: dict) -> str:
        pt = f.get("pattern_type", "")
        canonical = _PATTERN_ALIASES.get(pt, pt)
        meta = _PATTERN_META.get(canonical, {})
        sev = f.get("severity", "low")
        pal = SEV_PALETTE.get(sev, SEV_PALETTE["low"])
        icon = meta.get("icon", "⚠")
        title = meta.get("title", pt)

        # ── Evidence fields ──────────────────────────────────────────
        ev_rows = ""
        monetary_keys = {"value", "amount", "total_value", "total", "contract_value",
                         "value_rsd", "donation_amount", "total_contract_value",
                         "company_total_value", "institution_total_value"}
        for key, label in meta.get("fields", []):
            val = f.get(key)
            if val is None or val == "":
                continue
            is_mon = any(mk in key for mk in monetary_keys)
            is_pct = "pct" in key
            if is_mon:
                display = f'<span style="color:#b45309;font-weight:700;font-family:monospace">{_fmt_rsd(val)}</span>'
            elif is_pct:
                try:
                    display = f'<span style="color:#b45309;font-weight:700">{float(val):.0f}%</span>'
                except Exception:
                    display = f'<span style="color:#374151">{_esc(str(val))}</span>'
            else:
                display = f'<span style="color:#1e293b">{_esc(str(val))}</span>'
            ev_rows += (
                f'<tr>'
                f'<td style="padding:5px 14px 5px 0;font-size:11px;color:#64748b;white-space:nowrap;'
                f'font-family:monospace;vertical-align:top;min-width:160px">{_esc(label)}</td>'
                f'<td style="padding:5px 0;font-size:12.5px;font-weight:500;word-break:break-word">{display}</td>'
                f'</tr>'
            )
        evidence_html = ""
        if ev_rows:
            evidence_html = (
                '<div class="section">'
                '<div class="stitle">🔍 Detektovani entiteti</div>'
                f'<table style="border-collapse:collapse;width:100%">{ev_rows}</table>'
                '</div>'
            )

        # ── Why suspicious ───────────────────────────────────────────
        why_html = ""
        why_text = meta.get("why", "")
        if why_text:
            lines = why_text.split("\n")
            inner = ""
            for line in lines:
                stripped = line.strip()
                if not stripped:
                    continue
                if stripped.startswith("•"):
                    inner += f'<div style="padding:3px 0 3px 16px;color:#374151;line-height:1.65">{_esc(stripped)}</div>'
                else:
                    inner += f'<p style="margin:0 0 7px;line-height:1.7;color:#1e293b">{_esc(stripped)}</p>'
            why_html = (
                '<div class="section">'
                '<div class="stitle">⚠ Zašto je sumnjivo</div>'
                f'<div style="font-size:12.5px;background:#fffbeb;border-left:3px solid {pal["border"]};'
                f'padding:12px 16px;border-radius:0 6px 6px 0;line-height:1.7">{inner}</div>'
                '</div>'
            )

        # ── How detected (graph path) ────────────────────────────────
        how_html = ""
        how_text = meta.get("how", "")
        if how_text:
            how_html = (
                '<div class="section">'
                '<div class="stitle">◎ Kako je detektovano (put u grafu)</div>'
                f'<pre style="font-size:10.5px;font-family:monospace;color:#1e40af;background:#eff6ff;'
                f'padding:12px 16px;border-radius:6px;border:1px solid #bfdbfe;margin:0;'
                f'white-space:pre-wrap;word-break:break-word;line-height:1.7">{_esc(how_text)}</pre>'
                '</div>'
            )

        # ── Contract details ─────────────────────────────────────────
        det_rows = ""
        proc_type = str(f.get("proc_type") or "")
        if proc_type:
            pt_label = _PROC_TYPE_LABELS.get(proc_type, f"Tip {proc_type}")
            det_rows += (
                f'<tr><td style="padding:4px 14px 4px 0;font-size:11px;color:#64748b;white-space:nowrap;'
                f'font-family:monospace;vertical-align:top">Vrsta postupka</td>'
                f'<td style="padding:4px 0;font-size:11px;color:#374151">{_esc(pt_label)}</td></tr>'
            )
        award_date = f.get("award_date") or ""
        if award_date:
            det_rows += (
                f'<tr><td style="padding:4px 14px 4px 0;font-size:11px;color:#64748b;white-space:nowrap;'
                f'font-family:monospace;vertical-align:top">Datum dodele ugovora</td>'
                f'<td style="padding:4px 0;font-size:11px;color:#374151">{_esc(str(award_date))}</td></tr>'
            )
        contract_id = f.get("contract_id") or ""
        if contract_id:
            det_rows += (
                f'<tr><td style="padding:4px 14px 4px 0;font-size:11px;color:#64748b;white-space:nowrap;'
                f'font-family:monospace;vertical-align:top">ID ugovora</td>'
                f'<td style="padding:4px 0;font-size:11px;color:#374151;font-family:monospace">{_esc(str(contract_id))}</td></tr>'
            )
        contracts_detail = f.get("contracts_detail") or []
        if contracts_detail and isinstance(contracts_detail, list):
            for cd in contracts_detail[:5]:
                if not isinstance(cd, dict):
                    continue
                cd_title = _esc(str(cd.get("title") or cd.get("t") or "")[:80])
                cd_val = cd.get("value") or cd.get("v")
                cd_date = cd.get("date") or cd.get("d") or ""
                cd_id = cd.get("id") or ""
                val_str = _fmt_rsd(cd_val) if cd_val else "—"
                det_rows += (
                    f'<tr><td colspan="2" style="padding:5px 0;font-size:10.5px;color:#475569;'
                    f'border-top:1px solid #f1f5f9">'
                    f'<span style="font-family:monospace;color:#64748b">&rsaquo;</span> '
                    f'{cd_title or "(bez naziva)"} &nbsp;'
                    f'<span style="color:#b45309;font-weight:700;font-family:monospace">{val_str}</span>'
                    f'{f" &nbsp; {_esc(str(cd_date))}" if cd_date else ""}'
                    f'</td></tr>'
                )
        contract_detail_html = ""
        if det_rows:
            contract_detail_html = (
                '<div class="section">'
                '<div class="stitle">📋 Detalji ugovora / nabavke</div>'
                f'<table style="border-collapse:collapse;width:100%">{det_rows}</table>'
                '<div style="margin-top:8px;font-size:10px;color:#94a3b8;font-style:italic">'
                'Napomena: datum potpisivanja, identitet potpisnika i kompletna dokumentacija '
                'dostupni su na portalu javnih nabavki (link ispod).'
                '</div>'
                '</div>'
            )

        # ── Sources & verification ───────────────────────────────────
        src_items = ""
        for lbl, url in meta.get("sources", []):
            src_items += (
                f'<a href="{url}" target="_blank" rel="noopener noreferrer" '
                f'style="display:flex;align-items:center;gap:8px;padding:7px 12px;'
                f'background:#f0f9ff;border-radius:5px;text-decoration:none;'
                f'color:#0369a1;font-size:11px;margin-bottom:5px;border:1px solid #bae6fd">'
                f'<span style="font-size:10px">↗</span>{_esc(lbl)}</a>'
            )
        vurl = str(f.get("verification_url") or "")
        if vurl.startswith("http"):
            src_items += (
                f'<a href="{vurl}" target="_blank" rel="noopener noreferrer" '
                f'style="display:flex;align-items:center;gap:8px;padding:7px 12px;'
                f'background:#f0fdf4;border-radius:5px;text-decoration:none;'
                f'color:#15803d;font-size:11px;margin-bottom:5px;border:1px solid #bbf7d0">'
                f'<span style="font-size:10px">⊕</span>Direktni link na ugovor / nabavku (JN Portal)</a>'
            )
        mb = str(f.get("company_mb") or f.get("winner_mb") or "")
        if mb and not mb.upper().startswith("MB-SEED"):
            apr_url = f"https://pretraga.apr.gov.rs/unifiedsearch?searchTerm={mb}"
            src_items += (
                f'<a href="{apr_url}" target="_blank" rel="noopener noreferrer" '
                f'style="display:flex;align-items:center;gap:8px;padding:7px 12px;'
                f'background:#faf5ff;border-radius:5px;text-decoration:none;'
                f'color:#7e22ce;font-size:11px;margin-bottom:5px;border:1px solid #e9d5ff">'
                f'<span style="font-size:10px">↗</span>APR pretraga firme: {_esc(mb)}</a>'
            )
        sources_html = ""
        if src_items:
            sources_html = (
                '<div class="section">'
                '<div class="stitle">◈ Korišćeni izvori i verifikacija</div>'
                f'{src_items}'
                '</div>'
            )

        return (
            f'<div class="card" style="border-top:4px solid {pal["border"]};background:{pal["bg"]}">'
            f'<div class="card-hdr">'
            f'<div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap">'
            f'<span style="font-size:28px;line-height:1">{icon}</span>'
            f'<div>'
            f'<div style="font-size:16px;font-weight:700;color:#0f172a;margin-bottom:4px">{_esc(title)}</div>'
            f'<span style="font-size:10px;padding:2px 10px;border-radius:4px;font-weight:700;'
            f'text-transform:uppercase;letter-spacing:0.06em;'
            f'background:{pal["badge"]};color:{pal["text"]};border:1px solid {pal["border"]}">'
            f'{sev}</span>'
            f'</div>'
            f'<div style="margin-left:auto;font-size:11px;color:#94a3b8;font-family:monospace">#{idx}</div>'
            f'</div>'
            f'</div>'
            f'<div class="card-body">'
            f'{evidence_html}'
            f'{why_html}'
            f'{how_html}'
            f'{contract_detail_html}'
            f'{sources_html}'
            f'</div>'
            f'</div>'
        )

    cards_html = "\n".join(_card(i, f) for i, f in enumerate(sorted_findings, 1))

    risk_color = {"critical": "#dc2626", "high": "#f97316", "medium": "#ca8a04", "low": "#6b7280"}.get(
        risk.get("risk_level", "low"), "#6b7280"
    )
    sc = risk.get("severity_counts", {})
    ts = datetime.utcnow().strftime("%d.%m.%Y. u %H:%M UTC")
    data_note = (
        "Samo stvarni podaci — sintetički test-podaci su isključeni iz ovog izveštaja."
        if exclude_seed
        else "Uključeni su svi podaci (i test-podaci). Za izveštaj samo sa stvarnim podacima, "
             "otvorite URL sa <code>&amp;exclude_seed=true</code>."
    )

    return f"""<!DOCTYPE html>
<html lang="sr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Srpska Transparentnost — Izveštaj o nalazima</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:'Segoe UI',Arial,sans-serif;background:#f1f5f9;color:#1e293b;padding:24px 32px}}
  .header{{background:linear-gradient(135deg,#0f172a 0%,#1e3a5f 100%);color:white;padding:28px 32px;border-radius:12px;margin-bottom:22px}}
  .header h1{{font-size:22px;font-weight:700;margin-bottom:6px}}
  .header p{{font-size:12px;color:#94a3b8;margin-top:4px}}
  .notice{{background:#fffbeb;border:1px solid #fcd34d;border-radius:8px;padding:12px 16px;margin-bottom:18px;font-size:12px;color:#92400e;line-height:1.6}}
  .stats{{display:flex;gap:12px;margin-bottom:22px;flex-wrap:wrap}}
  .stat{{background:white;border-radius:8px;padding:14px 18px;border:1px solid #e2e8f0;flex:1;min-width:90px;box-shadow:0 1px 3px rgba(0,0,0,.06)}}
  .stat .val{{font-size:26px;font-weight:700}}
  .stat .lbl{{font-size:10px;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-top:4px}}
  .card{{background:white;border-radius:10px;margin-bottom:18px;box-shadow:0 1px 4px rgba(0,0,0,.08);overflow:hidden}}
  .card-hdr{{padding:16px 20px 14px;border-bottom:1px solid #e2e8f0}}
  .card-body{{padding:20px;display:flex;flex-direction:column;gap:16px}}
  .section{{display:flex;flex-direction:column;gap:6px}}
  .stitle{{font-size:10px;text-transform:uppercase;letter-spacing:.1em;color:#64748b;font-weight:700;font-family:monospace;margin-bottom:2px}}
  .sfooter{{background:white;border-radius:10px;padding:20px 24px;margin-top:22px;border:1px solid #e2e8f0}}
  .sfooter h3{{font-size:12px;color:#374151;margin-bottom:12px;text-transform:uppercase;letter-spacing:.06em}}
  .src-row{{display:flex;gap:12px;margin-bottom:6px;align-items:center}}
  .src-name{{font-size:12px;font-weight:500;min-width:260px}}
  .src-url{{font-size:11px;color:#2563eb;text-decoration:none}}
  .btn{{background:#0f172a;color:white;border:none;border-radius:6px;padding:9px 22px;cursor:pointer;font-size:13px;margin-bottom:18px;display:inline-block}}
  @media print{{body{{padding:8px;background:white}}.no-print{{display:none!important}}.card{{break-inside:avoid;box-shadow:none;border:1px solid #e2e8f0}}.header{{background:#0f172a!important;-webkit-print-color-adjust:exact;print-color-adjust:exact}}}}
</style>
</head>
<body>
<div class="header">
  <h1>🔍 Srpska Transparentnost — Analiza korupcije u javnim nabavkama</h1>
  <p>Generisano: {ts}</p>
  <p style="margin-top:8px;color:#e2e8f0;font-size:13px">Automatska detekcija obrazaca korupcije na osnovu javno dostupnih podataka</p>
</div>
<div class="notice">
  <strong>Napomena:</strong> {data_note}<br>
  Svi nalazi su zasnovani na javno dostupnim podacima. Detekcija <strong>ne znači automatski dokazanu korupciju</strong> —
  svaki nalaz zahteva dalju reviziju i proveru na navedenim izvorima podataka.
  Potpisnici ugovora, datum objave tendera i kompletna dokumentacija dostupni su direktno na JN portalu.
</div>
<div class="stats">
  <div class="stat"><div class="val" style="color:{risk_color}">{len(findings)}</div><div class="lbl">Ukupno nalaza</div></div>
  <div class="stat"><div class="val" style="color:#dc2626">{sc.get('critical',0)}</div><div class="lbl">Kritičnih</div></div>
  <div class="stat"><div class="val" style="color:#f97316">{sc.get('high',0)}</div><div class="lbl">Visokih</div></div>
  <div class="stat"><div class="val" style="color:#ca8a04">{sc.get('medium',0)}</div><div class="lbl">Srednje</div></div>
  <div class="stat"><div class="val" style="color:{risk_color}">{risk.get('risk_score',0)}</div><div class="lbl">Risk score</div></div>
</div>
<button class="btn no-print" onclick="window.print()">🖨 Štampaj / Sačuvaj PDF</button>
<div style="font-size:11px;color:#64748b;margin-bottom:16px">Nalazi sortirani po ozbiljnosti (kritični prvi)</div>
{cards_html}
<div class="sfooter no-print">
  <h3>Registrovani izvori podataka</h3>
  <div class="src-row"><span class="src-name">APR — Agencija za privredne registre</span><a class="src-url" href="https://pretraga.apr.gov.rs" target="_blank">pretraga.apr.gov.rs</a></div>
  <div class="src-row"><span class="src-name">Portal javnih nabavki (JN Portal) — UJN</span><a class="src-url" href="https://jnportal.ujn.gov.rs/tender-documents/search" target="_blank">jnportal.ujn.gov.rs</a></div>
  <div class="src-row"><span class="src-name">Otvoreni podaci — data.gov.rs</span><a class="src-url" href="https://data.gov.rs/sr/datasets/funkcioneri-i-javni-sluzbenici/" target="_blank">data.gov.rs/sr/datasets/funkcioneri-i-javni-sluzbenici</a></div>
  <div class="src-row"><span class="src-name">ACAS — Finansiranje stranaka</span><a class="src-url" href="https://www.acas.rs/finansiranje-politickih-subjekata/" target="_blank">acas.rs/finansiranje-politickih-subjekata</a></div>
  <div class="src-row"><span class="src-name">ACAS — Imovinski registar</span><a class="src-url" href="https://www.acas.rs/imovinski-registar/" target="_blank">acas.rs/imovinski-registar</a></div>
  <div class="src-row"><span class="src-name">Službeni glasnik RS</span><a class="src-url" href="https://www.pravno-informacioni-sistem.rs/SlGlasnikPortal/eli/collection" target="_blank">pravno-informacioni-sistem.rs/SlGlasnikPortal</a></div>
  <div class="src-row"><span class="src-name">Narodna skupština RS — poslanici</span><a class="src-url" href="https://www.parlament.gov.rs/members-of-parliament" target="_blank">parlament.gov.rs/members-of-parliament</a></div>
  <div class="src-row"><span class="src-name">Ministarstvo finansija — budžet</span><a class="src-url" href="https://www.mfin.gov.rs/dokumenti/budzet/" target="_blank">mfin.gov.rs/dokumenti/budzet</a></div>
  <div class="src-row"><span class="src-name">RGZ — Katastar nekretnina</span><a class="src-url" href="https://rgz.gov.rs/usluge/eLine" target="_blank">rgz.gov.rs/usluge/eLine</a></div>
</div>
</body></html>"""


# ── Health ──────────────────────────────────────────────────────
@app.get("/health")
def health():
    """Health check."""
    return check_health()


@app.get("/stats")
def stats():
    """
    Quick sanity endpoint:
    - counts by key labels
    - how many contracts have WON_CONTRACT edges (needed for detectors)
    """
    labels = ["Person", "Company", "Institution", "Contract", "PoliticalParty", "BudgetItem"]
    out = {"labels": {}, "relationships": {}, "ts": datetime.utcnow().isoformat()}

    for lab in labels:
        q = f"MATCH (n:{lab}) RETURN count(n) AS c"
        res = run_query(q, {}).single()
        out["labels"][lab] = int(res["c"]) if res else 0

    res = run_query("MATCH (:Company)-[r:WON_CONTRACT]->(:Contract) RETURN count(r) AS c", {}).single()
    out["relationships"]["WON_CONTRACT"] = int(res["c"]) if res else 0

    res = run_query("MATCH (:Institution)-[r:AWARDED_CONTRACT]->(:Contract) RETURN count(r) AS c", {}).single()
    out["relationships"]["AWARDED_CONTRACT"] = int(res["c"]) if res else 0

    return out


@app.get("/contracts/suspicious")
def suspicious_contracts(limit: int = 200):
    """
    Returns only contracts that appear in any detector findings.
    If detectors return empty (because data lacks companies), this returns empty too.
    """
    findings = run_all_detectors(limit=10000)

    contract_ids = set()
    for f in findings:
        cid = f.get("contract_id")
        if cid:
            contract_ids.add(cid)

    if not contract_ids:
        return {"contracts": [], "count": 0, "note": "No suspicious contracts found (or missing company data for detection)."}

    # Fetch contract nodes
    q = """
    MATCH (c:Contract)
    WHERE c.contract_id IN $ids
    RETURN c
    LIMIT $limit
    """
    rows = run_query(q, {"ids": list(contract_ids), "limit": limit})
    contracts = []
    for r in rows:
        node = r["c"]
        contracts.append(dict(node))

    return {"contracts": contracts, "count": len(contracts)}
