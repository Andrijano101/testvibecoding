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
            max_rows = int(os.getenv("JNPORTAL_MAX_ROWS", "2000"))
            s.scrape(max_rows=max_rows, force_refresh=force_refresh)
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
@app.get("/export/findings")
def export_findings(format: str = "json"):
    """
    Export all detection findings.
    format: json | csv | html
    """
    # Run all detectors
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

    if format == "csv":
        # Build CSV with all common fields
        fieldnames = ["pattern_type", "severity", "official_name", "institution", "family_member",
                      "company_name", "contract_title", "contract_value", "value_rsd", "total_value",
                      "award_date", "winner", "person_name", "party_name", "donation_amount",
                      "num_contracts", "address", "normalized_name", "verification_url"]

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        for finding in all_findings:
            writer.writerow(finding)

        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=findings_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.csv"}
        )

    elif format == "html":
        risk = det.compute_risk_summary({k: {"patterns": v} for k, v in results.items()})
        html = _build_html_report(all_findings, risk)
        return HTMLResponse(content=html)

    else:  # json
        risk = det.compute_risk_summary({k: {"patterns": v} for k, v in results.items()})
        return {
            "exported_at": datetime.utcnow().isoformat(),
            "total_findings": len(all_findings),
            "risk_summary": risk,
            "findings": all_findings,
            "sources": {
                "apr": "https://pretraga.apr.gov.rs",
                "procurement": "https://jnportal.ujn.gov.rs",
                "officials": "https://data.gov.rs/sr/datasets/funkcioneri-i-javni-sluzbenici/",
                "party_financing": "https://www.acas.rs/finansiranje-politickih-subjekata/",
                "gazette": "https://www.pravno-informacioni-sistem.rs/SlGlasnikPortal/eli/collection",
                "parliament": "https://www.parlament.gov.rs/members-of-parliament",
            }
        }


def _build_html_report(findings: list, risk: dict) -> str:
    """Build a printable HTML report of all findings."""
    severity_colors = {"critical": "#dc2626", "high": "#f97316", "medium": "#eab308", "low": "#6b7280"}
    pattern_labels = {
        "conflict_of_interest": "Sukob interesa", "single_bidder": "Jedan ponuđač",
        "contract_splitting": "Deljenje ugovora", "revolving_door": "Rotirajuća vrata",
        "ghost_employee": "Fantomski zaposleni", "shell_company_cluster": "Shell kompanije",
        "budget_self_allocation": "Samododeljivanje", "political_donor_contract": "Donator→Ugovor",
        "conflicts": "Sukob interesa", "ghosts": "Fantomski zaposleni", "shells": "Shell kompanije",
        "budget_allocation": "Samododeljivanje", "donor_contracts": "Donator→Ugovor",
    }

    # Real portal URLs per pattern — used to generate Izvor links in the report
    pattern_portals: dict = {
        "conflict_of_interest": [
            ("APR — vlasnici i direktori firmi", "https://pretraga.apr.gov.rs"),
            ("Portal javnih nabavki — ugovori", "https://jnportal.ujn.gov.rs/tender-documents/search"),
            ("Javni funkcioneri — data.gov.rs", "https://data.gov.rs/sr/datasets/funkcioneri-i-javni-sluzbenici/"),
        ],
        "ghost_employee": [
            ("Javni funkcioneri — data.gov.rs", "https://data.gov.rs/sr/datasets/funkcioneri-i-javni-sluzbenici/"),
            ("Službeni glasnik — postavljenja", "https://www.pravno-informacioni-sistem.rs/SlGlasnikPortal/eli/collection"),
            ("Imovinski registar — ACAS", "https://www.acas.rs/imovinski-registar/"),
        ],
        "shell_company_cluster": [
            ("APR — registrovane adrese", "https://pretraga.apr.gov.rs"),
            ("Portal javnih nabavki — ugovori", "https://jnportal.ujn.gov.rs/tender-documents/search"),
        ],
        "single_bidder": [
            ("Portal javnih nabavki — pregled nabavki", "https://jnportal.ujn.gov.rs/tender-documents/search"),
        ],
        "revolving_door": [
            ("Službeni glasnik — razrešenja", "https://www.pravno-informacioni-sistem.rs/SlGlasnikPortal/eli/collection"),
            ("APR — direktorska imenovanja", "https://pretraga.apr.gov.rs"),
            ("Portal javnih nabavki — ugovori", "https://jnportal.ujn.gov.rs/tender-documents/search"),
        ],
        "budget_self_allocation": [
            ("Budžet RS — Ministarstvo finansija", "https://www.mfin.gov.rs/dokumenti/budzet/"),
            ("Portal javnih nabavki — ugovori", "https://jnportal.ujn.gov.rs/tender-documents/search"),
            ("APR — vlasnici firmi", "https://pretraga.apr.gov.rs"),
        ],
        "contract_splitting": [
            ("Portal javnih nabavki — hronologija", "https://jnportal.ujn.gov.rs/tender-documents/search"),
        ],
        "political_donor_contract": [
            ("ACAS — finansiranje stranaka", "https://www.acas.rs/finansiranje-politickih-subjekata/"),
            ("APR — donatori", "https://pretraga.apr.gov.rs"),
            ("Portal javnih nabavki — ugovori", "https://jnportal.ujn.gov.rs/tender-documents/search"),
        ],
    }
    # Normalize aliases
    for alias, canonical in [("conflicts", "conflict_of_interest"), ("ghosts", "ghost_employee"),
                              ("shells", "shell_company_cluster"), ("budget_allocation", "budget_self_allocation"),
                              ("donor_contracts", "political_donor_contract")]:
        pattern_portals[alias] = pattern_portals.get(canonical, [])

    def _portals_html(pattern_type: str, finding: dict) -> str:
        links = []
        # Direct APR link if we have a real-looking maticni_broj
        mb = finding.get("company_mb") or finding.get("winner_mb")
        if mb and not mb.startswith("MB-SEED"):
            links.append(f'<a href="https://pretraga.apr.gov.rs/unifiedsearch?searchTerm={mb}" target="_blank" '
                         f'style="color:#2563eb;text-decoration:none;font-size:11px;display:block;margin-bottom:3px">'
                         f'↗ APR pretraga: {mb}</a>')
        # Pattern-specific portals
        for label, url in pattern_portals.get(pattern_type, []):
            links.append(f'<a href="{url}" target="_blank" '
                         f'style="color:#4b5563;text-decoration:none;font-size:10px;display:block;margin-bottom:2px">'
                         f'↗ {label}</a>')
        return "".join(links) if links else '<span style="color:#9ca3af;font-size:10px">—</span>'

    rows = ""
    for i, f in enumerate(findings, 1):
        sev = f.get("severity", "low")
        color = severity_colors.get(sev, "#6b7280")
        pt = f.get("pattern_type", "")
        label = pattern_labels.get(pt, pt)

        facts = []
        if f.get("official_name"): facts.append(f"Funkcioner: <strong>{f['official_name']}</strong>")
        if f.get("institution"): facts.append(f"Institucija: {f['institution']}")
        if f.get("family_member"): facts.append(f"Porodica: <strong>{f['family_member']}</strong>")
        if f.get("company_name"): facts.append(f"Firma: <strong>{f['company_name']}</strong>")
        if f.get("winner"): facts.append(f"Pobednik: <strong>{f['winner']}</strong>")
        if f.get("contract_title"): facts.append(f"Ugovor: {f['contract_title']}")
        if f.get("person_name") and not f.get("official_name"): facts.append(f"Osoba: <strong>{f['person_name']}</strong>")
        if f.get("party_name"): facts.append(f"Stranka: {f['party_name']}")
        if f.get("donor_company"): facts.append(f"Donator: <strong>{f['donor_company']}</strong>")
        if f.get("address"): facts.append(f"Adresa: {f['address']} ({f.get('num_companies','?')} firmi)")
        if f.get("name_1"): facts.append(f"Osoba 1: {f['name_1']} ({f.get('institution_1','')})")
        if f.get("name_2"): facts.append(f"Osoba 2: {f['name_2']} ({f.get('institution_2','')})")

        val = f.get("contract_value") or f.get("value_rsd") or f.get("total_value") or f.get("donation_amount")
        if val:
            val_m = val / 1_000_000
            facts.append(f"Vrednost: <strong>{val_m:.1f}M RSD</strong>")
        if f.get("award_date"): facts.append(f"Datum: {f['award_date']}")

        facts_html = "".join(f'<div style="padding:2px 0;color:#374151;font-size:12px">{fact}</div>' for fact in facts)
        source_html = _portals_html(pt, f)

        rows += f"""
        <tr style="border-bottom:1px solid #e5e7eb">
          <td style="padding:10px 8px;color:#6b7280;font-size:12px;white-space:nowrap;vertical-align:top">{i}</td>
          <td style="padding:10px 8px;vertical-align:top">
            <span style="background:{color}22;color:{color};border:1px solid {color};padding:2px 8px;border-radius:4px;font-size:10px;font-weight:700;text-transform:uppercase;white-space:nowrap">{sev}</span>
          </td>
          <td style="padding:10px 8px;font-weight:700;font-size:13px;color:#111827;vertical-align:top;white-space:nowrap">{label}</td>
          <td style="padding:10px 8px;vertical-align:top">{facts_html}</td>
          <td style="padding:10px 8px;vertical-align:top;min-width:200px">{source_html}</td>
        </tr>"""

    risk_color = severity_colors.get(risk.get("risk_level", "low"), "#6b7280")
    sc = risk.get("severity_counts", {})

    return f"""<!DOCTYPE html>
<html lang="sr">
<head>
<meta charset="UTF-8">
<title>Srpska Transparentnost — Izveštaj o nalazima</title>
<style>
  body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 32px; background: #f9fafb; color: #111827; }}
  .header {{ background: linear-gradient(135deg, #0f172a, #1e293b); color: white; padding: 28px 32px; border-radius: 12px; margin-bottom: 28px; }}
  .header h1 {{ margin: 0 0 6px; font-size: 22px; }}
  .header p {{ margin: 0; color: #94a3b8; font-size: 13px; }}
  .notice {{ background:#fffbeb;border:1px solid #fcd34d;border-radius:8px;padding:12px 16px;margin-bottom:20px;font-size:12px;color:#92400e; }}
  .stats {{ display: flex; gap: 16px; margin-bottom: 28px; flex-wrap: wrap; }}
  .stat {{ background: white; border-radius: 8px; padding: 16px 20px; border: 1px solid #e5e7eb; flex: 1; min-width: 120px; }}
  .stat .val {{ font-size: 28px; font-weight: 700; }}
  .stat .lbl {{ font-size: 11px; color: #6b7280; text-transform: uppercase; letter-spacing: 0.05em; margin-top: 4px; }}
  table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 10px; overflow: hidden; box-shadow: 0 1px 4px rgba(0,0,0,0.08); }}
  th {{ background: #f1f5f9; padding: 10px 8px; text-align: left; font-size: 10px; text-transform: uppercase; letter-spacing: 0.08em; color: #64748b; border-bottom: 2px solid #e2e8f0; }}
  .sources-footer {{ margin-top:28px; background:white; border-radius:8px; border:1px solid #e5e7eb; padding:20px 24px; }}
  .sources-footer h3 {{ margin:0 0 14px; font-size:13px; color:#374151; }}
  .src-row {{ display:flex; gap:12px; margin-bottom:8px; align-items:baseline; }}
  .src-name {{ font-size:12px; font-weight:600; color:#111827; min-width:220px; }}
  .src-url {{ font-size:11px; color:#2563eb; text-decoration:none; }}
  @media print {{ body {{ padding: 16px; }} .no-print {{ display: none; }} }}
</style>
</head>
<body>
<div class="header">
  <h1>Srpska Transparentnost — Izveštaj o nalazima</h1>
  <p>Generisano: {datetime.utcnow().strftime('%d.%m.%Y. u %H:%M UTC')} &nbsp;|&nbsp; Ukupno nalaza: {len(findings)}</p>
</div>
<div class="notice">
  <strong>Napomena o podacima:</strong> Ovaj izveštaj se zasniva na podacima koji su trenutno u bazi.
  Ako baza sadrži sintetičke test-podatke (source='seed'), linkovi u koloni "Izvor" vode ka pravim državnim portalima
  gde možete ručno proveriti informacije — ali konkretni entiteti (firme, lica) su izmišljeni i neće biti nađeni.
  Za pravi nalaz pokrenite <code>POST /ingest/all</code> da biste skupili stvarne podatke.
</div>
<div class="stats">
  <div class="stat"><div class="val" style="color:{risk_color}">{len(findings)}</div><div class="lbl">Ukupno nalaza</div></div>
  <div class="stat"><div class="val" style="color:#dc2626">{sc.get('critical',0)}</div><div class="lbl">Kritičnih</div></div>
  <div class="stat"><div class="val" style="color:#f97316">{sc.get('high',0)}</div><div class="lbl">Visokih</div></div>
  <div class="stat"><div class="val" style="color:#eab308">{sc.get('medium',0)}</div><div class="lbl">Srednje</div></div>
  <div class="stat"><div class="val" style="color:{risk_color}">{risk.get('risk_score',0)}</div><div class="lbl">Risk score</div></div>
</div>
<button class="no-print" onclick="window.print()" style="margin-bottom:20px;padding:8px 20px;background:#0f172a;color:white;border:none;border-radius:6px;cursor:pointer;font-size:13px">Štampaj / Sačuvaj PDF</button>
<table>
  <thead><tr><th>#</th><th>Nivo</th><th>Obrazac</th><th>Detalji</th><th>Gde proveriti (izvor)</th></tr></thead>
  <tbody>{rows}</tbody>
</table>
<div class="sources-footer">
  <h3>Registrovani izvori podataka</h3>
  <div class="src-row"><span class="src-name">APR — Agencija za privredne registre</span><a class="src-url" href="https://pretraga.apr.gov.rs" target="_blank">pretraga.apr.gov.rs</a></div>
  <div class="src-row"><span class="src-name">Portal javnih nabavki — UJN</span><a class="src-url" href="https://jnportal.ujn.gov.rs/tender-documents/search" target="_blank">jnportal.ujn.gov.rs</a></div>
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
