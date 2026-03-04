"""
Initialize Neo4j schema: constraints and full-text indexes.
Run once after Neo4j starts:
  python -m backend.queries.init_schema
"""
from backend.api.database import run_query
import structlog

logger = structlog.get_logger()

CONSTRAINTS = [
    "CREATE CONSTRAINT person_id IF NOT EXISTS FOR (p:Person) REQUIRE p.person_id IS UNIQUE",
    "CREATE CONSTRAINT company_mb IF NOT EXISTS FOR (c:Company) REQUIRE c.maticni_broj IS UNIQUE",
    "CREATE CONSTRAINT institution_id IF NOT EXISTS FOR (i:Institution) REQUIRE i.institution_id IS UNIQUE",
    "CREATE CONSTRAINT contract_id IF NOT EXISTS FOR (ct:Contract) REQUIRE ct.contract_id IS UNIQUE",
    "CREATE CONSTRAINT address_id IF NOT EXISTS FOR (a:Address) REQUIRE a.address_id IS UNIQUE",
    "CREATE CONSTRAINT party_id IF NOT EXISTS FOR (pp:PoliticalParty) REQUIRE pp.party_id IS UNIQUE",
    "CREATE CONSTRAINT budget_id IF NOT EXISTS FOR (b:BudgetItem) REQUIRE b.budget_id IS UNIQUE",
    "CREATE CONSTRAINT property_id IF NOT EXISTS FOR (p:Property) REQUIRE p.property_id IS UNIQUE",
]

INDEXES = [
    "CREATE INDEX person_role IF NOT EXISTS FOR (p:Person) ON (p.current_role)",
    "CREATE INDEX company_status IF NOT EXISTS FOR (c:Company) ON (c.status)",
    "CREATE INDEX contract_date IF NOT EXISTS FOR (ct:Contract) ON (ct.award_date)",
    "CREATE INDEX contract_value IF NOT EXISTS FOR (ct:Contract) ON (ct.value_rsd)",
    "CREATE INDEX property_city IF NOT EXISTS FOR (p:Property) ON (p.city)",
    "CREATE INDEX budget_year IF NOT EXISTS FOR (b:BudgetItem) ON (b.fiscal_year)",
    "CREATE INDEX person_party IF NOT EXISTS FOR (p:Person) ON (p.party_name)",
]

FULLTEXT_INDEXES = [
    """
    CREATE FULLTEXT INDEX person_ft IF NOT EXISTS
    FOR (p:Person) ON EACH [p.full_name, p.name_normalized]
    OPTIONS { indexConfig: { `fulltext.analyzer`: 'standard' } }
    """,
    """
    CREATE FULLTEXT INDEX company_ft IF NOT EXISTS
    FOR (c:Company) ON EACH [c.name, c.name_normalized]
    OPTIONS { indexConfig: { `fulltext.analyzer`: 'standard' } }
    """,
]


def init_schema():
    logger.info("schema_init_start")

    for cypher in CONSTRAINTS:
        try:
            run_query(cypher)
            logger.info("constraint_created", cypher=cypher.split("FOR")[0].strip())
        except Exception as e:
            logger.warning("constraint_skip", error=str(e))

    for cypher in INDEXES:
        try:
            run_query(cypher)
            logger.info("index_created", cypher=cypher.split("FOR")[0].strip())
        except Exception as e:
            logger.warning("index_skip", error=str(e))

    for cypher in FULLTEXT_INDEXES:
        try:
            run_query(cypher.strip())
            logger.info("fulltext_index_created")
        except Exception as e:
            logger.warning("fulltext_index_skip", error=str(e))

    logger.info("schema_init_complete")


if __name__ == "__main__":
    init_schema()
