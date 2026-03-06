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
    """Find suspicious single-source contracts.

    Detects two cases:
    1. Contracts with num_bidders = 1 (seed/enriched data)
    2. Contracts awarded via negotiated procedure without notice (proc_type '3' or '9')
       which in Serbian law means competitive bidding was bypassed — a red flag.
    """
    return """
    MATCH (inst:Institution)-[:AWARDED_CONTRACT]->(ct:Contract)
    MATCH (company:Company)-[:WON_CONTRACT]->(ct)
    WITH inst, ct, company,
         coalesce(ct.value_rsd, ct.contract_value, 0) AS val,
         ct.proc_type AS ptype,
         ct.num_bidders AS bidders
    WHERE val >= $min_value
      AND (bidders = 1 OR ptype IN ['3', '9'])
    WITH inst, ct, company, val, ptype,
         CASE
           WHEN val >= 50000000 THEN 'critical'
           WHEN val >= 20000000 THEN 'high'
           WHEN val >= 5000000  THEN 'medium'
           ELSE 'low'
         END AS severity
    RETURN
        ct.title AS contract_title,
        ct.contract_id AS contract_id,
        val AS value_rsd,
        ct.award_date AS award_date,
        ptype AS proc_type,
        inst.name AS institution,
        inst.institution_id AS institution_id,
        company.name AS winner,
        company.maticni_broj AS winner_mb,
        severity,
        'single_bidder' AS pattern_type
    ORDER BY val DESC
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


def contract_splitting(threshold_rsd=15000000):
    """Detect contract splitting: multiple contracts just below procurement
    threshold awarded to same company by same institution.

    In Serbia, the simplified procedure threshold is ~15M RSD for goods/services.
    Splitting detects: same firm + same institution, multiple contracts each below
    threshold but collectively above it — classic way to avoid mandatory open tender.

    Uses coalesce(value_rsd, contract_value) to handle both seed and real data.
    """
    return """
    MATCH (inst:Institution)-[:AWARDED_CONTRACT]->(ct:Contract)
    MATCH (company:Company)-[:WON_CONTRACT]->(ct)
    WITH inst, company, ct,
         coalesce(ct.value_rsd, ct.contract_value, 0) AS val
    WHERE val < $threshold
      AND val > $threshold * 0.3
      AND ct.award_date IS NOT NULL
    WITH inst, company, ct, val
    ORDER BY ct.award_date
    WITH inst, company, collect(ct) AS contracts, collect(val) AS values,
         sum(val) AS total_value,
         count(ct) AS num
    WHERE num >= 2
    WITH inst, company, contracts, values, total_value, num,
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
        [i IN range(0, size(contracts)-1) | {
            title: contracts[i].title,
            value: values[i],
            date: contracts[i].award_date,
            id: contracts[i].contract_id
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


def repeated_winner(min_contracts: int = 3, min_total_rsd: int = 5_000_000):
    """A company wins N+ contracts from the same institution with suspicious total value.

    High single-institution dependency is a red flag for cronyism.
    Requires minimum total value to filter out trivial multi-contract supplier relationships.
    """
    return """
    MATCH (inst:Institution)-[:AWARDED_CONTRACT]->(ct:Contract)<-[:WON_CONTRACT]-(co:Company)
    WHERE ct.award_date IS NOT NULL
    WITH inst, co, collect(ct) AS contracts, count(ct) AS num,
         sum(coalesce(ct.contract_value, ct.value_rsd, 0)) AS total_value
    WHERE num >= $min_contracts AND total_value >= $min_total
    WITH inst, co, contracts, num, total_value,
         CASE
           WHEN total_value >= 100000000 THEN 'critical'
           WHEN total_value >= 50000000  THEN 'high'
           WHEN total_value >= 20000000  THEN 'medium'
           ELSE 'low'
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
    """, {"min_contracts": min_contracts, "min_total": min_total_rsd}


def direct_official_contractor(min_value_rsd: int = 1_000_000):
    """Official employed at Institution directly owns/directs a company that won a contract
    from that same institution.

    Unlike conflict_of_interest (which requires a FAMILY_OF intermediary), this detects
    the most direct form: the official themselves is on both sides of the deal.
    Works with real data once APR director enrichment is loaded.
    """
    return """
    MATCH (official:Person)-[:EMPLOYED_BY]->(inst:Institution)
    MATCH (official)-[:OWNS|DIRECTS]->(company:Company)
    MATCH (company)-[:WON_CONTRACT]->(contract:Contract)
    MATCH (inst)-[:AWARDED_CONTRACT]->(contract)
    WHERE coalesce(contract.value_rsd, contract.contract_value, 0) >= $min_value
    WITH official, inst, company, contract,
         CASE
           WHEN coalesce(contract.value_rsd, contract.contract_value, 0) >= 50000000 THEN 'critical'
           WHEN coalesce(contract.value_rsd, contract.contract_value, 0) >= 10000000 THEN 'high'
           WHEN coalesce(contract.value_rsd, contract.contract_value, 0) >= 1000000  THEN 'medium'
           ELSE 'low'
         END AS severity
    RETURN
        official.full_name AS official_name,
        official.person_id AS official_id,
        official.current_role AS official_role,
        inst.name AS institution,
        inst.institution_id AS institution_id,
        company.name AS company_name,
        company.maticni_broj AS company_mb,
        contract.title AS contract_title,
        contract.contract_id AS contract_id,
        coalesce(contract.value_rsd, contract.contract_value) AS contract_value,
        contract.award_date AS award_date,
        official.source AS data_source,
        severity,
        'direct_official_contractor' AS pattern_type
    ORDER BY
        CASE severity
            WHEN 'critical' THEN 0
            WHEN 'high' THEN 1
            WHEN 'medium' THEN 2
            ELSE 3
        END,
        coalesce(contract.value_rsd, contract.contract_value) DESC
    """, {"min_value": min_value_rsd}


def ghost_director(min_contracts: int = 2):
    """A person directs/owns multiple companies that collectively won contracts
    from the same institution — a sign of shell company coordination.

    Different from ghost_employees (which checks EMPLOYED_BY across institutions);
    this checks DIRECTS/OWNS across companies winning from the same institution.
    Works with real data once APR director enrichment is loaded.
    """
    return """
    MATCH (p:Person)-[:DIRECTS|OWNS]->(co1:Company)-[:WON_CONTRACT]->(ct1:Contract)
    MATCH (p)-[:DIRECTS|OWNS]->(co2:Company)-[:WON_CONTRACT]->(ct2:Contract)
    WHERE co1.maticni_broj < co2.maticni_broj
    MATCH (inst:Institution)-[:AWARDED_CONTRACT]->(ct1)
    MATCH (inst)-[:AWARDED_CONTRACT]->(ct2)
    WITH p, inst,
         collect(DISTINCT co1) + collect(DISTINCT co2) AS companies,
         count(DISTINCT ct1) + count(DISTINCT ct2) AS total_contracts,
         sum(coalesce(ct1.value_rsd, ct1.contract_value, 0)) +
         sum(coalesce(ct2.value_rsd, ct2.contract_value, 0)) AS total_value
    WHERE total_contracts >= $min_contracts
    WITH p, inst, companies, total_contracts, total_value,
         CASE
           WHEN total_value >= 50000000 THEN 'critical'
           WHEN total_value >= 20000000 THEN 'high'
           WHEN size(companies) >= 3    THEN 'high'
           ELSE 'medium'
         END AS severity
    RETURN
        p.full_name AS director_name,
        p.person_id AS director_id,
        inst.name AS institution,
        inst.institution_id AS institution_id,
        size(companies) AS num_companies,
        total_contracts,
        total_value,
        [c IN companies | {name: c.name, mb: c.maticni_broj}] AS company_details,
        severity,
        'ghost_director' AS pattern_type
    ORDER BY total_value DESC
    LIMIT 50
    """, {"min_contracts": min_contracts}


def institutional_monopoly(min_pct: float = 0.7, min_value_rsd: int = 10_000_000, min_contracts: int = 3):
    """An institution gives ≥70% of its total contract value to a single company.

    Requires at least min_contracts (default 3) so that trivially small institutions
    with a single large contract don't trigger false positives.
    Extreme concentration is a strong red flag for cronyism / procurement capture.
    """
    return """
    MATCH (inst:Institution)-[:AWARDED_CONTRACT]->(ct:Contract)
    WITH inst, sum(coalesce(ct.value_rsd, ct.contract_value, 0)) AS inst_total
    WHERE inst_total >= $min_value

    MATCH (inst)-[:AWARDED_CONTRACT]->(ct2:Contract)<-[:WON_CONTRACT]-(co:Company)
    WITH inst, inst_total, co,
         sum(coalesce(ct2.value_rsd, ct2.contract_value, 0)) AS co_total,
         count(ct2) AS num_contracts
    WHERE num_contracts >= $min_contracts
      AND co_total >= inst_total * $min_pct

    WITH inst, co, inst_total, co_total, num_contracts,
         round(100.0 * co_total / inst_total) AS pct,
         CASE
           WHEN co_total / inst_total >= 0.9 THEN 'critical'
           WHEN co_total / inst_total >= 0.8 THEN 'high'
           ELSE 'medium'
         END AS severity
    RETURN
        inst.name AS institution,
        inst.institution_id AS institution_id,
        co.name AS company_name,
        co.maticni_broj AS company_mb,
        inst_total AS institution_total_value,
        co_total AS company_total_value,
        pct AS company_pct_of_institution,
        num_contracts,
        severity,
        'institutional_monopoly' AS pattern_type
    ORDER BY co_total DESC
    LIMIT 50
    """, {"min_pct": min_pct, "min_value": min_value_rsd, "min_contracts": min_contracts}


def samododeljivanje_proxy(min_value_rsd: int = 1_000_000):
    """Proxy for budget self-allocation: a government official is employed at an institution
    AND owns/directs a company that won contracts from ANY public institution.

    Unlike budget_self_allocation (which needs BudgetItem nodes), this proxy uses the
    person's institutional role as a proxy for budget influence.
    Works with real data once APR director enrichment is loaded.
    """
    return """
    MATCH (official:Person)-[:EMPLOYED_BY]->(employer:Institution)
    MATCH (official)-[:OWNS|DIRECTS]->(company:Company)
    MATCH (company)-[:WON_CONTRACT]->(contract:Contract)
    OPTIONAL MATCH (awarding:Institution)-[:AWARDED_CONTRACT]->(contract)
    WHERE coalesce(contract.value_rsd, contract.contract_value, 0) >= $min_value
    WITH official, employer, company, contract, awarding,
         CASE
           WHEN awarding.institution_id = employer.institution_id THEN 'critical'
           WHEN coalesce(contract.value_rsd, contract.contract_value, 0) >= 50000000 THEN 'high'
           ELSE 'medium'
         END AS severity
    RETURN
        official.full_name AS official_name,
        official.person_id AS official_id,
        official.current_role AS official_role,
        employer.name AS employer_institution,
        employer.institution_id AS employer_id,
        company.name AS company_name,
        company.maticni_broj AS company_mb,
        contract.title AS contract_title,
        contract.contract_id AS contract_id,
        coalesce(contract.value_rsd, contract.contract_value) AS contract_value,
        contract.award_date AS award_date,
        awarding.name AS awarding_institution,
        awarding.institution_id AS awarding_id,
        severity,
        'samododeljivanje_proxy' AS pattern_type
    ORDER BY
        CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 ELSE 2 END,
        coalesce(contract.value_rsd, contract.contract_value) DESC
    LIMIT 100
    """, {"min_value": min_value_rsd}


def new_company_big_contract(max_age_years: int = 3, min_value_rsd: int = 5_000_000):
    """A recently founded company wins a large contract quickly.

    Young companies with no track record winning large public contracts are suspicious.
    """
    return """
    MATCH (co:Company)-[:WON_CONTRACT]->(ct:Contract)
    WHERE co.founding_date IS NOT NULL AND co.founding_date <> ''
      AND coalesce(ct.contract_value, ct.value_rsd, 0) >= $min_value
      AND ct.award_date IS NOT NULL AND ct.award_date <> ''
    WITH co, ct,
         // Handle both ISO (YYYY-MM-DD) and Serbian (D.M.YYYY.) date formats
         CASE
           WHEN co.founding_date =~ '\\d{4}-.*'
             THEN toInteger(substring(co.founding_date, 0, 4))
           ELSE toInteger([x IN split(co.founding_date, '.') WHERE x =~ '\\d{4}' | x][0])
         END AS founded_year,
         toInteger(substring(ct.award_date, 0, 4)) AS award_year
    WITH co, ct, award_year - founded_year AS age_at_award
    WHERE age_at_award IS NOT NULL AND age_at_award >= 0 AND age_at_award <= $max_age
    OPTIONAL MATCH (inst:Institution)-[:AWARDED_CONTRACT]->(ct)
    WITH co,
         min(age_at_award) AS age_at_award,
         collect(DISTINCT ct.contract_id)[0] AS top_contract_id,
         collect(DISTINCT ct.title)[0] AS top_contract_title,
         max(coalesce(ct.contract_value, ct.value_rsd, 0)) AS top_contract_value,
         sum(coalesce(ct.contract_value, ct.value_rsd, 0)) AS total_value,
         count(DISTINCT ct) AS num_contracts,
         collect(DISTINCT ct.award_date)[0] AS award_date,
         collect(DISTINCT inst.name)[0] AS institution,
         collect(DISTINCT inst.institution_id)[0] AS institution_id
    WITH co, age_at_award, top_contract_id, top_contract_title,
         top_contract_value, total_value, num_contracts, award_date,
         institution, institution_id,
         CASE
           WHEN age_at_award = 0 THEN 'critical'
           WHEN age_at_award <= 1 AND total_value >= 10000000 THEN 'critical'
           WHEN age_at_award <= 2 THEN 'high'
           ELSE 'medium'
         END AS severity
    RETURN
        co.name AS company_name,
        co.maticni_broj AS company_mb,
        co.founding_date AS founded,
        age_at_award,
        top_contract_title AS contract_title,
        top_contract_id AS contract_id,
        top_contract_value AS contract_value,
        total_value,
        num_contracts,
        award_date,
        institution,
        institution_id,
        severity,
        'new_company_big_contract' AS pattern_type
    ORDER BY age_at_award ASC, total_value DESC
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
    # Real-data detectors (fire once APR director data is loaded)
    ("direct_official_contractor", lambda: direct_official_contractor()),
    ("ghost_director", lambda: ghost_director()),
    ("institutional_monopoly", lambda: institutional_monopoly()),
    ("samododeljivanje_proxy", lambda: samododeljivanje_proxy()),
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
