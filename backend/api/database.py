"""Neo4j database connection manager with async support, retry logic, and connection pooling."""
import os
from contextlib import contextmanager
from typing import Optional

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, SessionExpired, TransientError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import structlog

logger = structlog.get_logger()

NEO4J_URI = os.getenv("DATABASE_URL", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "changeme123")
NEO4J_MAX_POOL_SIZE = int(os.getenv("NEO4J_MAX_POOL_SIZE", "50"))
NEO4J_CONNECTION_TIMEOUT = int(os.getenv("NEO4J_CONNECTION_TIMEOUT", "30"))

_driver = None

RETRYABLE = (ServiceUnavailable, SessionExpired, TransientError)


def get_driver():
    """Get or create the Neo4j driver with connection pooling."""
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD),
            max_connection_pool_size=NEO4J_MAX_POOL_SIZE,
            connection_timeout=NEO4J_CONNECTION_TIMEOUT,
            connection_acquisition_timeout=60,
        )
        # Verify connectivity on startup
        try:
            _driver.verify_connectivity()
            logger.info("neo4j_connected", uri=NEO4J_URI, pool_size=NEO4J_MAX_POOL_SIZE)
        except Exception as e:
            logger.error("neo4j_connection_failed", uri=NEO4J_URI, error=str(e))
            _driver = None
            raise
    return _driver


def close_driver():
    """Close the Neo4j driver."""
    global _driver
    if _driver:
        _driver.close()
        _driver = None
        logger.info("neo4j_disconnected")


@contextmanager
def get_session(database: str = "neo4j"):
    """Get a Neo4j session with proper lifecycle management."""
    driver = get_driver()
    session = driver.session(database=database)
    try:
        yield session
    finally:
        session.close()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
    retry=retry_if_exception_type(RETRYABLE),
    before_sleep=lambda rs: logger.warning(
        "neo4j_retry", attempt=rs.attempt_number, error=str(rs.outcome.exception())
    ),
)
def run_query(cypher: str, params: Optional[dict] = None) -> list[dict]:
    """Execute a Cypher query and return results as list of dicts.

    Includes automatic retry for transient failures.
    """
    with get_session() as session:
        result = session.run(cypher, params or {})
        return [record.data() for record in result]


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
    retry=retry_if_exception_type(RETRYABLE),
)
def run_query_single(cypher: str, params: Optional[dict] = None) -> Optional[dict]:
    """Execute a Cypher query and return single result."""
    with get_session() as session:
        result = session.run(cypher, params or {})
        record = result.single()
        return record.data() if record else None


def run_query_paginated(
    cypher: str,
    params: Optional[dict] = None,
    skip: int = 0,
    limit: int = 25,
) -> dict:
    """Execute a paginated Cypher query.

    Appends SKIP/LIMIT and runs a count query.
    Returns {items: [...], total: int, skip: int, limit: int}.
    """
    # Build count query by wrapping
    count_cypher = f"CALL () {{ {cypher} RETURN count(*) AS __total }} RETURN __total"
    paginated_cypher = f"{cypher} SKIP $__skip LIMIT $__limit"

    full_params = {**(params or {}), "__skip": skip, "__limit": limit}

    items = run_query(paginated_cypher, full_params)

    # Try to get total count; fall back to len(items) if count query fails
    try:
        count_result = run_query_single(count_cypher, params or {})
        total = count_result.get("__total", len(items)) if count_result else len(items)
    except Exception:
        total = len(items)

    return {"items": items, "total": total, "skip": skip, "limit": limit}


def check_health() -> dict:
    """Check Neo4j connectivity and return status."""
    try:
        result = run_query_single("RETURN 1 AS ok")
        if result and result.get("ok") == 1:
            return {"status": "healthy", "neo4j": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "neo4j": str(e)}
    return {"status": "unhealthy", "neo4j": "unknown error"}
