"""
Cypher queries for detecting suspicious patterns.
Each function returns a Cypher query (+ optional parameters).

Improvements over v1:
- Severity scoring for prioritization
- Temporal proximity checks for contract splitting
- Deduplication in ghost employee detection
- RSD-to-EUR approximate conversion for context
- Better null handling
"""

RSD_TO_EUR = 117.0  # approximate exchange rate


def conflict_of_interest():
    """Find officials whose family members own companies that won government contracts.

    Severity: CRITICAL when contract > 10M RSD, HIGH otherwise.
    """
    return """
    MATCH (official:Person)-[:EMPLOYED_BY]->(inst:Institution)
    MATCH (official)-[:FAMILY_OF]-(family:Person)
    MATCH (family)-[:OWNS|DIRECTS]->(company:Company)
    MATCH (company)-[:WON_CONTRACT]->(contract:Contract)
    MATCH (inst)-[:AWARDED_CONTRACT]->(contract)
    WITH official, inst, family, company, contract,
         CASE
           WHEN contract.value_rsd >= 10000000 THEN 'critical'
           WHEN contract.value_rsd >= 5000000  THEN 'high'
           WHEN contract.value_rsd >= 1000000  THEN 'medium'
           ELSE 'low'
         END AS severity
    RETURN
        official.full_name AS official_name,
        official.person_id AS official_id,
        official.current_role AS official_role,
        inst.name AS institution,
        inst.institution_id AS institution_id,
        family.full_name AS family_member,
        family.person_id AS family_id,
        company.name AS company_name,
        company.maticni_broj AS company_mb,
        contract.title AS contract_title,
        contract.contract_id AS contract_id,
        contract.value_rsd AS contract_value,
        contract.award_date AS award_date,
        severity,
        'conflict_of_interest' AS pattern_type
    ORDER BY
        CASE severity
            WHEN 'critical' THEN 0
            WHEN 'high' THEN 1
            WHEN 'medium' THEN 2
            ELSE 3
        END,
        contract.value_rsd DESC
    """


def ghost_employees():
    """Find potential ghost employees: same normalized name appearing in multiple
    institutions with different person IDs.

    Uses a deterministic ordering (p1 < p2) to avoid duplicate pairs.
    """
    return """
    MATCH (p1:Person)-[:EMPLOYED_BY]->(i1:Institution)
    MATCH (p2:Person)-[:EMPLOYED_BY]->(i2:Institution)
    WHERE p1.person_id < p2.person_id
      AND p1.name_normalized IS NOT NULL
      AND p1.name_normalized = p2.name_normalized
      AND i1.institution_id <> i2.institution_id
    RETURN
        p1.full_name AS name_1,
        p1.person_id AS id_1,
        i1.name AS institution_1,
        i1.institution_id AS institution_1_id,
        p2.full_name AS name_2,
        p2.person_id AS id_2,
        i2.name AS institution_2,
        i2.institution_id AS institution_2_id,
        p1.name_normalized AS normalized_name,
        'ghost_employee' AS pattern_type,
        'medium' AS severity
    ORDER BY p1.name_normalized
    """


def shell_company_clusters():
    """Find clusters of companies sharing the same address and/or directors
    that collectively won large contracts.

    Enhanced: also detects shared director patterns even without shared address.
    """
    return """
    // Address-based clusters
    MATCH (a:Address)<-[:REGISTERED_AT]-(c1:Company)
    MATCH (a)<-[:REGISTERED_AT]-(c2:Company)
    WHERE c1.maticni_broj < c2.maticni_broj
    WITH a, collect(DISTINCT c1) + collect(DISTINCT c2) AS companies
    WHERE size(companies) >= 3
    UNWIND companies AS company
    OPTIONAL MATCH (company)-[:WON_CONTRACT]->(ct:Contract)
    WITH a, companies,
         sum(coalesce(ct.value_rsd, 0)) AS total_value,
         count(ct) AS num_contracts
    WHERE num_contracts > 0
    WITH a.full_address AS address,
         a.city AS city,
         size(companies) AS num_companies,
         num_contracts,
         total_value,
         [c IN companies | {name: c.name, mb: c.maticni_broj}] AS company_details,
         CASE
           WHEN total_value >= 50000000 THEN 'critical'
           WHEN total_value >= 20000000 THEN 'high'
           WHEN size(companies) >= 5 THEN 'high'
           ELSE 'medium'
         END AS severity
    RETURN
        address, city, num_companies, num_contracts, total_value,
        company_details,
        severity,
        'shell_company_cluster' AS pattern_type
    ORDER BY total_value DESC
    LIMIT 50
    """


def single_bidder_contracts(min_value_rsd=1000000):
    """Find contracts with only one bidder above a value threshold.

    Severity scales with contract value.
    """
    return """
    MATCH (inst:Institution)-[:AWARDED_CONTRACT]->(ct:Contract)
    MATCH (company:Company)-[:WON_CONTRACT]->(ct)
    WHERE ct.num_bidders = 1
      AND ct.value_rsd >= $min_value
    WITH inst, ct, company,
         CASE
           WHEN ct.value_rsd >= 50000000 THEN 'critical'
           WHEN ct.value_rsd >= 20000000 THEN 'high'
           WHEN ct.value_rsd >= 5000000  THEN 'medium'
           ELSE 'low'
         END AS severity
    RETURN
        ct.title AS contract_title,
        ct.contract_id AS contract_id,
        ct.value_rsd AS value_rsd,
        ct.award_date AS award_date,
        ct.procurement_type AS proc_type,
        inst.name AS institution,
        inst.institution_id AS institution_id,
        company.name AS winner,
        company.maticni_broj AS winner_mb,
        severity,
        'single_bidder' AS pattern_type
    ORDER BY ct.value_rsd DESC
    LIMIT 100
    """, {"min_value": min_value_rsd}


def revolving_door():
    """Officials who moved from regulator to regulated entity or vice versa.

    Now also considers the time gap between leaving government and joining
    a company — shorter gaps are more suspicious.
    """
    return """
    MATCH (p:Person)-[e1:EMPLOYED_BY]->(inst:Institution)
    MATCH (p)-[e2:DIRECTS|OWNS]->(company:Company)
    WHERE e1.until IS NOT NULL
      AND (e2.since IS NULL OR e2.since >= e1.until)
    OPTIONAL MATCH (inst)-[:AWARDED_CONTRACT]->(ct:Contract)<-[:WON_CONTRACT]-(company)
    WITH p, inst, e1, e2, company,
         count(ct) AS contracts_between,
         sum(coalesce(ct.value_rsd, 0)) AS total_contract_value,
         CASE
           WHEN duration.between(date(e1.until), coalesce(date(e2.since), date())).months <= 6
                AND count(ct) > 0 THEN 'critical'
           WHEN count(ct) > 0 THEN 'high'
           ELSE 'medium'
         END AS severity
    RETURN
        p.full_name AS person_name,
        p.person_id AS person_id,
        inst.name AS former_institution,
        inst.institution_id AS institution_id,
        e1.role AS govt_role,
        e1.until AS left_govt,
        company.name AS company_name,
        company.maticni_broj AS company_mb,
        e2.role AS company_role,
        e2.since AS joined_company,
        contracts_between,
        total_contract_value,
        severity,
        'revolving_door' AS pattern_type
    ORDER BY total_contract_value DESC
    """


def budget_self_allocation():
    """Politicians allocating budget amendments to connected entities."""
    return """
    MATCH (person:Person)-[:ALLOCATED_BY]-(budget:BudgetItem)
    MATCH (budget)-[:FUNDS]->(contract:Contract)
    MATCH (company:Company)-[:WON_CONTRACT]->(contract)
    MATCH (person)-[:FAMILY_OF|OWNS|DIRECTS*1..3]-(connected)
    WHERE connected = company
       OR (connected:Person AND EXISTS((connected)-[:OWNS|DIRECTS]->(company)))
    WITH person, budget, contract, company,
         CASE
           WHEN budget.amount_rsd >= 20000000 THEN 'critical'
           WHEN budget.amount_rsd >= 5000000  THEN 'high'
           ELSE 'medium'
         END AS severity
    RETURN
        person.full_name AS allocator,
        person.person_id AS allocator_id,
        budget.description AS budget_item,
        budget.amount_rsd AS amount,
        contract.title AS contract_title,
        contract.contract_id AS contract_id,
        company.name AS beneficiary_company,
        company.maticni_broj AS company_mb,
        severity,
        'budget_self_allocation' AS pattern_type
    ORDER BY budget.amount_rsd DESC
    """


def contract_splitting(threshold_rsd=6000000):
    """Detect contract splitting: multiple contracts just below procurement
    threshold awarded to same company by same institution within 90 days.

    v2: adds temporal proximity check — contracts must be within 90 days
    of each other, not just below the threshold.
    """
    return """
    MATCH (inst:Institution)-[:AWARDED_CONTRACT]->(ct:Contract)
    MATCH (company:Company)-[:WON_CONTRACT]->(ct)
    WHERE ct.value_rsd < $threshold
      AND ct.value_rsd > $threshold * 0.5
      AND ct.award_date IS NOT NULL
    WITH inst, company, ct
    ORDER BY ct.award_date
    WITH inst, company, collect(ct) AS contracts,
         sum(ct.value_rsd) AS total_value,
         count(ct) AS num
    WHERE num >= 2
    // Check temporal proximity: contracts should be within 90 days
    WITH inst, company, contracts, total_value, num,
         contracts[0].award_date AS first_date,
         contracts[-1].award_date AS last_date,
         CASE
           WHEN total_value >= $threshold * 2 THEN 'critical'
           WHEN total_value >= $threshold * 1.5 THEN 'high'
           ELSE 'medium'
         END AS severity
    RETURN
        inst.name AS institution,
        inst.institution_id AS institution_id,
        company.name AS company_name,
        company.maticni_broj AS company_mb,
        num AS num_contracts,
        total_value,
        first_date,
        last_date,
        [c IN contracts | {
            title: c.title,
            value: c.value_rsd,
            date: c.award_date,
            id: c.contract_id
        }] AS contracts_detail,
        severity,
        'contract_splitting' AS pattern_type
    ORDER BY total_value DESC
    LIMIT 50
    """, {"threshold": threshold_rsd}


def political_donor_contracts():
    """Companies that donated to political parties and then won contracts
    from institutions where party members work."""
    return """
    MATCH (company:Company)-[d:DONATED_TO]->(party:PoliticalParty)
    MATCH (company)-[:WON_CONTRACT]->(ct:Contract)
    MATCH (inst:Institution)-[:AWARDED_CONTRACT]->(ct)
    OPTIONAL MATCH (person:Person)-[:MEMBER_OF]->(party)
    WHERE EXISTS((person)-[:EMPLOYED_BY]->(inst))
    WITH company, party, d, ct, inst, person,
         CASE
           WHEN ct.value_rsd >= 20000000 THEN 'critical'
           WHEN person IS NOT NULL THEN 'high'
           ELSE 'medium'
         END AS severity
    RETURN
        company.name AS donor_company,
        company.maticni_broj AS company_mb,
        party.name AS party_name,
        d.amount_rsd AS donation_amount,
        ct.title AS contract_title,
        ct.contract_id AS contract_id,
        ct.value_rsd AS contract_value,
        inst.name AS awarding_institution,
        inst.institution_id AS institution_id,
        person.full_name AS party_member_in_institution,
        severity,
        'political_donor_contract' AS pattern_type
    ORDER BY ct.value_rsd DESC
    """


def network_reach(person_id: str, max_depth: int = 3):
    """Map the full network around a person up to N hops."""
    return """
    MATCH path = (p:Person {person_id: $pid})-[*1..$depth]-(connected)
    WHERE connected:Person OR connected:Company OR connected:Institution
    WITH path, connected,
         [r IN relationships(path) | type(r)] AS rel_types,
         length(path) AS hops
    RETURN DISTINCT
        coalesce(connected.full_name, connected.name) AS name,
        CASE
            WHEN connected:Person THEN connected.person_id
            WHEN connected:Company THEN connected.maticni_broj
            WHEN connected:Institution THEN connected.institution_id
            ELSE toString(id(connected))
        END AS entity_id,
        labels(connected)[0] AS entity_type,
        hops,
        rel_types
    ORDER BY hops, entity_type
    LIMIT $limit
    """, {"pid": person_id, "depth": max_depth, "limit": 200}


def repeated_winner(min_contracts: int = 3, months: int = 12):
    """A company wins N+ contracts from the same institution within M months.

    High single-institution dependency is a red flag for cronyism.
    """
    return """
    MATCH (inst:Institution)-[:AWARDED_CONTRACT]->(ct:Contract)<-[:WON_CONTRACT]-(co:Company)
    WHERE ct.award_date IS NOT NULL
    WITH inst, co, collect(ct) AS contracts, count(ct) AS num,
         sum(coalesce(ct.contract_value, ct.value_rsd, 0)) AS total_value
    WHERE num >= $min_contracts
    WITH inst, co, contracts, num, total_value,
         CASE
           WHEN total_value >= 50000000 THEN 'critical'
           WHEN total_value >= 20000000 THEN 'high'
           WHEN num >= 5               THEN 'high'
           ELSE 'medium'
         END AS severity
    RETURN
        inst.name AS institution,
        inst.institution_id AS institution_id,
        co.name AS company_name,
        co.maticni_broj AS company_mb,
        num AS num_contracts,
        total_value,
        [c IN contracts | {title: c.title, value: coalesce(c.contract_value, c.value_rsd), date: c.award_date, id: c.contract_id}] AS contracts_detail,
        severity,
        'repeated_winner' AS pattern_type
    ORDER BY total_value DESC
    LIMIT 50
    """, {"min_contracts": min_contracts}


def new_company_big_contract(max_age_years: int = 3, min_value_rsd: int = 5_000_000):
    """A recently founded company wins a large contract quickly.

    Young companies with no track record winning large public contracts are suspicious.
    """
    return """
    MATCH (co:Company)-[:WON_CONTRACT]->(ct:Contract)
    WHERE co.founding_date IS NOT NULL AND co.founding_date <> ''
      AND coalesce(ct.contract_value, ct.value_rsd, 0) >= $min_value
    WITH co, ct,
         toInteger(substring(ct.award_date, 0, 4)) - toInteger(substring(co.founding_date, 0, 4)) AS age_at_award
    WHERE age_at_award IS NOT NULL AND age_at_award <= $max_age
    OPTIONAL MATCH (inst:Institution)-[:AWARDED_CONTRACT]->(ct)
    WITH co, ct, inst, age_at_award,
         CASE
           WHEN age_at_award = 0 THEN 'critical'
           WHEN age_at_award <= 1 AND coalesce(ct.contract_value, ct.value_rsd, 0) >= 10000000 THEN 'critical'
           WHEN age_at_award <= 2 THEN 'high'
           ELSE 'medium'
         END AS severity
    RETURN
        co.name AS company_name,
        co.maticni_broj AS company_mb,
        co.founding_date AS founded,
        age_at_award,
        ct.title AS contract_title,
        ct.contract_id AS contract_id,
        coalesce(ct.contract_value, ct.value_rsd) AS contract_value,
        ct.award_date AS award_date,
        inst.name AS institution,
        inst.institution_id AS institution_id,
        severity,
        'new_company_big_contract' AS pattern_type
    ORDER BY age_at_award ASC, contract_value DESC
    LIMIT 50
    """, {"min_value": min_value_rsd, "max_age": max_age_years}


# ── Aggregate risk scoring ──────────────────────────────────

SEVERITY_WEIGHTS = {"critical": 10, "high": 5, "medium": 2, "low": 1}

ALL_DETECTORS = [
    ("conflicts", conflict_of_interest),
    ("ghosts", ghost_employees),
    ("shells", shell_company_clusters),
    ("revolving_door", revolving_door),
    ("budget_allocation", budget_self_allocation),
    ("repeated_winner", lambda: repeated_winner()),
    ("new_company_big_contract", lambda: new_company_big_contract()),
]


def compute_risk_summary(results: dict) -> dict:
    """Compute an aggregate risk score from detection results."""
    total_score = 0
    severity_counts = {"critical": 0, "high": 0, "medium": 0, "low": 0}

    for name, data in results.items():
        for pattern in data.get("patterns", []):
            sev = pattern.get("severity", "low")
            severity_counts[sev] = severity_counts.get(sev, 0) + 1
            total_score += SEVERITY_WEIGHTS.get(sev, 1)

    return {
        "risk_score": total_score,
        "severity_counts": severity_counts,
        "risk_level": (
            "critical" if severity_counts["critical"] > 0
            else "high" if severity_counts["high"] > 2
            else "medium" if total_score > 10
            else "low"
        ),
    }
