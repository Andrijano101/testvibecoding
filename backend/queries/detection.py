"""
Cypher queries for detecting suspicious patterns in public procurement.

v2 changes:
- All monetary thresholds lowered to 2M RSD floor
- 5 new detection patterns
- Better severity scoring tuned to Serbian procurement scale
- Deduplication improvements
- party_linked_contractor uses MP party affiliation data
"""

RSD_TO_EUR = 117.0  # approximate exchange rate

# Serbian procurement law thresholds (RSD)
# Below these values, simplified/direct negotiation is allowed
THRESHOLD_DIRECT    =  1_000_000   # direct agreement (no competition required)
THRESHOLD_SIMPLIFIED = 5_000_000   # simplified open procedure
THRESHOLD_OPEN      = 15_000_000   # full open tender required for goods/services
THRESHOLD_WORKS     = 30_000_000   # full open tender for construction works


def conflict_of_interest():
    """Official's family member owns/directs a company that won contracts from the same institution."""
    return """
    MATCH (official:Person)-[:EMPLOYED_BY]->(inst:Institution)
    MATCH (official)-[:FAMILY_OF]-(family:Person)
    MATCH (family)-[:OWNS|DIRECTS]->(company:Company)
    MATCH (company)-[:WON_CONTRACT]->(contract:Contract)
    MATCH (inst)-[:AWARDED_CONTRACT]->(contract)
    WITH official, inst, family, company, contract,
         coalesce(contract.value_rsd, contract.contract_value, 0) AS val
    WHERE val >= 2000000
    WITH official, inst, family, company, contract, val,
         CASE
           WHEN val >= 10000000 THEN 'critical'
           WHEN val >= 5000000  THEN 'high'
           ELSE 'medium'
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
        val AS contract_value,
        contract.award_date AS award_date,
        contract.verification_url AS verification_url,
        severity,
        'conflict_of_interest' AS pattern_type
    ORDER BY
        CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 ELSE 2 END,
        val DESC
    """


def ghost_employees():
    """Same normalized name appears across multiple institutions with different person IDs."""
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
    """3+ companies sharing the same address collectively winning contracts."""
    return """
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
           WHEN total_value >= 10000000 THEN 'high'
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


def single_bidder_contracts(min_value_rsd: int = 2_000_000):
    """Contracts won through non-competitive procedures (single bid or negotiated without notice).

    Proc types 3 and 9 in Serbian procurement law = negotiated procedure without prior
    publication — competitive bidding was bypassed.
    Threshold lowered to 2M RSD to catch mid-size contracts.
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
           WHEN val >= 10000000 THEN 'high'
           WHEN val >= 2000000  THEN 'medium'
           ELSE 'low'
         END AS severity
    OPTIONAL MATCH (dir:Person)-[:DIRECTS|OWNS]->(company)
    WITH inst, ct, company, val, ptype, severity,
         collect(DISTINCT dir.full_name) AS directors
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
        ct.verification_url AS verification_url,
        directors,
        severity,
        'single_bidder' AS pattern_type
    ORDER BY val DESC
    LIMIT 200
    """, {"min_value": min_value_rsd}


def revolving_door():
    """Former officials who joined companies that then won contracts from their old institution."""
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


def contract_splitting(threshold_rsd: int = 15_000_000):
    """Multiple contracts just below the open-tender threshold, same firm + institution.

    Checks three legal thresholds (2M, 5M, 15M) to catch splitting at every level.
    Lowered minimum individual contract to 400K (was 30% of threshold) to catch
    clusters of small contracts that collectively evade the 2M threshold.
    """
    return """
    MATCH (inst:Institution)-[:AWARDED_CONTRACT]->(ct:Contract)
    MATCH (company:Company)-[:WON_CONTRACT]->(ct)
    WITH inst, company, ct,
         coalesce(ct.value_rsd, ct.contract_value, 0) AS val
    WHERE val < $threshold
      AND val >= 400000
      AND ct.award_date IS NOT NULL
    WITH inst, company, ct, val
    ORDER BY ct.award_date
    WITH inst, company, collect(ct) AS contracts, collect(val) AS values,
         sum(val) AS total_value,
         count(ct) AS num
    WHERE num >= 2 AND total_value >= 2000000
    WITH inst, company, contracts, values, total_value, num,
         contracts[0].award_date AS first_date,
         contracts[-1].award_date AS last_date,
         CASE
           WHEN total_value >= $threshold * 2 THEN 'critical'
           WHEN total_value >= $threshold      THEN 'high'
           WHEN total_value >= 5000000         THEN 'medium'
           ELSE 'low'
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
    LIMIT 100
    """, {"threshold": threshold_rsd}


def political_donor_contracts():
    """Companies that donated to political parties and won contracts from party-linked institutions."""
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
        ct.verification_url AS verification_url,
        ct.value_rsd AS contract_value,
        inst.name AS awarding_institution,
        inst.institution_id AS institution_id,
        person.full_name AS party_member_in_institution,
        severity,
        'political_donor_contract' AS pattern_type
    ORDER BY ct.value_rsd DESC
    """


def repeated_winner(min_contracts: int = 3, min_total_rsd: int = 2_000_000):
    """Company wins 3+ contracts from the same institution with suspicious total value.

    Threshold lowered to 2M RSD total to catch medium-scale cronyism.
    """
    return """
    MATCH (inst:Institution)-[:AWARDED_CONTRACT]->(ct:Contract)<-[:WON_CONTRACT]-(co:Company)
    WHERE ct.award_date IS NOT NULL
    WITH inst, co, collect(ct) AS contracts, count(ct) AS num,
         sum(coalesce(ct.contract_value, ct.value_rsd, 0)) AS total_value
    WHERE num >= $min_contracts AND total_value >= $min_total
    // Also compute institution's total to get share percentage
    WITH inst, co, contracts, num, total_value
    OPTIONAL MATCH (inst)-[:AWARDED_CONTRACT]->(all_ct:Contract)
    WITH inst, co, contracts, num, total_value,
         sum(coalesce(all_ct.contract_value, all_ct.value_rsd, 0)) AS inst_total
    WITH inst, co, contracts, num, total_value, inst_total,
         CASE WHEN inst_total > 0 THEN round(100.0 * total_value / inst_total) ELSE null END AS share_pct,
         CASE
           WHEN total_value >= 100000000 THEN 'critical'
           WHEN total_value >= 20000000  THEN 'high'
           WHEN total_value >= 5000000   THEN 'medium'
           ELSE 'low'
         END AS severity
    OPTIONAL MATCH (dir:Person)-[:DIRECTS|OWNS]->(co)
    WITH inst, co, contracts, num, total_value, inst_total, share_pct, severity,
         collect(DISTINCT dir.full_name) AS directors
    RETURN
        inst.name AS institution,
        inst.institution_id AS institution_id,
        co.name AS company_name,
        co.maticni_broj AS company_mb,
        co.founding_date AS company_founded,
        num AS num_contracts,
        total_value,
        share_pct,
        directors,
        [c IN contracts | {title: c.title, value: coalesce(c.contract_value, c.value_rsd), date: c.award_date, id: c.contract_id}] AS contracts_detail,
        severity,
        'repeated_winner' AS pattern_type
    ORDER BY total_value DESC
    LIMIT 100
    """, {"min_contracts": min_contracts, "min_total": min_total_rsd}


def direct_official_contractor(min_value_rsd: int = 2_000_000):
    """Official employed at Institution directly directs a company that won a contract from that same institution."""
    return """
    MATCH (official:Person)-[:EMPLOYED_BY]->(inst:Institution)
    MATCH (official)-[:OWNS|DIRECTS]->(company:Company)
    MATCH (company)-[:WON_CONTRACT]->(contract:Contract)
    MATCH (inst)-[:AWARDED_CONTRACT]->(contract)
    WITH official, inst, company, contract,
         coalesce(contract.value_rsd, contract.contract_value, 0) AS val
    WHERE val >= $min_value
    WITH official, inst, company, contract, val,
         CASE
           WHEN val >= 50000000 THEN 'critical'
           WHEN val >= 10000000 THEN 'high'
           ELSE 'medium'
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
        val AS contract_value,
        contract.award_date AS award_date,
        contract.verification_url AS verification_url,
        official.source AS data_source,
        severity,
        'direct_official_contractor' AS pattern_type
    ORDER BY
        CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 ELSE 2 END,
        val DESC
    """, {"min_value": min_value_rsd}


def ghost_director(min_contracts: int = 2):
    """One person directs multiple companies all winning contracts from the same institution."""
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
    WHERE total_contracts >= $min_contracts AND total_value >= 2000000
    WITH p, inst, companies, total_contracts, total_value,
         CASE
           WHEN total_value >= 50000000 THEN 'critical'
           WHEN total_value >= 10000000 THEN 'high'
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


def institutional_monopoly(min_pct: float = 0.6, min_value_rsd: int = 2_000_000, min_contracts: int = 2):
    """A company receives ≥60% of an institution's total contract value.

    Threshold lowered to 60% (was 70%) and 2M floor (was 10M).
    min_contracts lowered to 2 to catch institutions with only a few large contracts.
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
           WHEN co_total / inst_total >= 0.75 THEN 'high'
           ELSE 'medium'
         END AS severity
    OPTIONAL MATCH (dir:Person)-[:DIRECTS|OWNS]->(co)
    WITH inst, co, inst_total, co_total, num_contracts, pct, severity,
         collect(DISTINCT dir.full_name) AS directors
    RETURN
        inst.name AS institution,
        inst.institution_id AS institution_id,
        co.name AS company_name,
        co.maticni_broj AS company_mb,
        co.founding_date AS company_founded,
        inst_total AS institution_total_value,
        co_total AS company_total_value,
        pct AS company_pct_of_institution,
        num_contracts,
        directors,
        severity,
        'institutional_monopoly' AS pattern_type
    ORDER BY co_total DESC
    LIMIT 100
    """, {"min_pct": min_pct, "min_value": min_value_rsd, "min_contracts": min_contracts}


def samododeljivanje_proxy(min_value_rsd: int = 2_000_000):
    """Government official or MP directs a company that won public contracts from any institution.

    Threshold raised to 2M RSD to reduce noise while catching real cases.
    """
    return """
    MATCH (official:Person)-[:EMPLOYED_BY]->(employer:Institution)
    MATCH (official)-[:OWNS|DIRECTS]->(company:Company)
    MATCH (company)-[:WON_CONTRACT]->(contract:Contract)
    OPTIONAL MATCH (awarding:Institution)-[:AWARDED_CONTRACT]->(contract)
    WITH official, employer, company, contract, awarding,
         coalesce(contract.value_rsd, contract.contract_value, 0) AS val
    WHERE val >= $min_value
    WITH official, employer, company, contract, awarding, val,
         CASE
           WHEN awarding.institution_id = employer.institution_id THEN 'critical'
           WHEN val >= 50000000 THEN 'high'
           WHEN val >= 10000000 THEN 'medium'
           ELSE 'low'
         END AS severity
    WHERE severity <> 'low'
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
        val AS contract_value,
        contract.award_date AS award_date,
        contract.verification_url AS verification_url,
        awarding.name AS awarding_institution,
        awarding.institution_id AS awarding_id,
        severity,
        'samododeljivanje_proxy' AS pattern_type
    ORDER BY
        CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 ELSE 2 END,
        val DESC
    LIMIT 100
    """, {"min_value": min_value_rsd}


def new_company_big_contract(max_age_years: int = 3, min_value_rsd: int = 2_000_000):
    """Recently founded company wins a contract above 2M RSD.

    Threshold lowered from 5M to 2M. Severity now considers both age and value.
    """
    return """
    MATCH (co:Company)-[:WON_CONTRACT]->(ct:Contract)
    WHERE co.founding_date IS NOT NULL AND co.founding_date <> ''
      AND coalesce(ct.contract_value, ct.value_rsd, 0) >= $min_value
      AND ct.award_date IS NOT NULL AND ct.award_date <> ''
    WITH co, ct,
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
         collect(DISTINCT ct.verification_url)[0] AS verification_url,
         collect(DISTINCT inst.name)[0] AS institution,
         collect(DISTINCT inst.institution_id)[0] AS institution_id
    OPTIONAL MATCH (dir:Person)-[:DIRECTS|OWNS]->(co)
    WITH co, age_at_award, top_contract_id, top_contract_title,
         top_contract_value, total_value, num_contracts, award_date,
         verification_url, institution, institution_id,
         collect(DISTINCT dir.full_name) AS directors
    WITH co, age_at_award, top_contract_id, top_contract_title,
         top_contract_value, total_value, num_contracts, award_date,
         verification_url, institution, institution_id, directors,
         CASE
           WHEN age_at_award = 0                                    THEN 'critical'
           WHEN age_at_award <= 1 AND top_contract_value >= 5000000 THEN 'critical'
           WHEN age_at_award <= 1                                   THEN 'high'
           WHEN age_at_award <= 2 AND top_contract_value >= 5000000 THEN 'high'
           WHEN age_at_award <= 2                                   THEN 'medium'
           ELSE 'low'
         END AS severity
    WHERE severity <> 'low'
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
        verification_url,
        institution,
        institution_id,
        directors,
        severity,
        'new_company_big_contract' AS pattern_type
    ORDER BY age_at_award ASC, total_value DESC
    LIMIT 100
    """, {"min_value": min_value_rsd, "max_age": max_age_years}


# ── NEW PATTERNS ─────────────────────────────────────────────────


def zero_competition_repeat(min_value_rsd: int = 2_000_000, min_contracts: int = 2):
    """Institution repeatedly bypasses competition for the same company.

    Detects: same institution awards 2+ contracts via negotiated/no-bid procedure
    to the same company. Each instance is suspicious; repetition is systemic.
    In Serbian law proc_type 3 = negotiated without prior notice,
    proc_type 9 = direct agreement. Both bypass normal competitive tendering.
    """
    return """
    MATCH (inst:Institution)-[:AWARDED_CONTRACT]->(ct:Contract)
    MATCH (co:Company)-[:WON_CONTRACT]->(ct)
    WITH inst, co, ct,
         coalesce(ct.value_rsd, ct.contract_value, 0) AS val,
         ct.proc_type AS ptype,
         ct.num_bidders AS bidders
    WHERE val >= $min_value
      AND (bidders = 1 OR ptype IN ['3', '9'])
    WITH inst, co,
         count(ct) AS num_nocompetition,
         sum(coalesce(ct.value_rsd, ct.contract_value, 0)) AS total_value,
         collect({title: ct.title, value: coalesce(ct.value_rsd, ct.contract_value, 0),
                  date: ct.award_date, id: ct.contract_id, proc: ct.proc_type}) AS contracts_detail
    WHERE num_nocompetition >= $min_contracts
    WITH inst, co, num_nocompetition, total_value, contracts_detail,
         CASE
           WHEN total_value >= 50000000 OR num_nocompetition >= 5 THEN 'critical'
           WHEN total_value >= 10000000 OR num_nocompetition >= 3 THEN 'high'
           ELSE 'medium'
         END AS severity
    RETURN
        inst.name AS institution,
        inst.institution_id AS institution_id,
        co.name AS company_name,
        co.maticni_broj AS company_mb,
        num_nocompetition AS num_contracts,
        total_value,
        contracts_detail,
        severity,
        'zero_competition_repeat' AS pattern_type
    ORDER BY total_value DESC
    LIMIT 100
    """, {"min_value": min_value_rsd, "min_contracts": min_contracts}


def value_just_below_threshold(tolerance_pct: float = 0.15, min_occurrences: int = 2):
    """Company receives multiple contracts with values clustered just below legal thresholds.

    Serbian law thresholds for goods/services:
      - 1M RSD: direct agreement (no process required)
      - 5M RSD: simplified open procedure
      - 15M RSD: full open tender

    A contract at 4.8M (just below 5M) avoids open tender requirements.
    Two or more such contracts to the same company from the same institution
    in the suspicious zone is a strong signal of deliberate manipulation.
    """
    return """
    MATCH (inst:Institution)-[:AWARDED_CONTRACT]->(ct:Contract)
    MATCH (co:Company)-[:WON_CONTRACT]->(ct)
    WITH inst, co, ct,
         coalesce(ct.value_rsd, ct.contract_value, 0) AS val
    // Flag contracts in the "suspicious zone" just below each threshold
    WHERE (val >= 850000  AND val < 1000000)   // just below 1M threshold
       OR (val >= 4250000 AND val < 5000000)   // just below 5M threshold
       OR (val >= 12750000 AND val < 15000000) // just below 15M threshold
    WITH inst, co,
         count(ct) AS num_suspicious,
         sum(coalesce(ct.value_rsd, ct.contract_value, 0)) AS total_value,
         collect({title: ct.title, value: coalesce(ct.value_rsd, ct.contract_value, 0),
                  date: ct.award_date, id: ct.contract_id}) AS contracts_detail
    WHERE num_suspicious >= $min_occurrences
    WITH inst, co, num_suspicious, total_value, contracts_detail,
         CASE
           WHEN num_suspicious >= 5 OR total_value >= 20000000 THEN 'critical'
           WHEN num_suspicious >= 3 OR total_value >= 5000000  THEN 'high'
           ELSE 'medium'
         END AS severity
    RETURN
        inst.name AS institution,
        inst.institution_id AS institution_id,
        co.name AS company_name,
        co.maticni_broj AS company_mb,
        num_suspicious AS num_contracts,
        total_value,
        contracts_detail,
        severity,
        'value_just_below_threshold' AS pattern_type
    ORDER BY num_suspicious DESC, total_value DESC
    LIMIT 100
    """, {"min_occurrences": min_occurrences}


def distributed_evasion(min_institutions: int = 3, min_total_rsd: int = 2_000_000):
    """Company wins small contracts from many different institutions to stay below detection radar.

    Classic evasion: instead of one large contract (which triggers scrutiny),
    the company accumulates many small contracts from different institutions.
    Each individual contract looks unremarkable; the cumulative total is significant.
    """
    return """
    MATCH (inst:Institution)-[:AWARDED_CONTRACT]->(ct:Contract)<-[:WON_CONTRACT]-(co:Company)
    WITH co, inst,
         sum(coalesce(ct.value_rsd, ct.contract_value, 0)) AS inst_value,
         count(ct) AS inst_contracts
    WITH co,
         count(DISTINCT inst) AS num_institutions,
         sum(inst_value) AS total_value,
         sum(inst_contracts) AS total_contracts,
         avg(inst_value) AS avg_per_institution,
         collect(DISTINCT inst.name)[0..5] AS sample_institutions
    WHERE num_institutions >= $min_institutions
      AND total_value >= $min_total
      // Key signal: many institutions but small average per institution
      AND avg_per_institution < total_value * 0.4
    WITH co, num_institutions, total_value, total_contracts,
         avg_per_institution, sample_institutions,
         CASE
           WHEN total_value >= 50000000 AND num_institutions >= 10 THEN 'critical'
           WHEN total_value >= 20000000 OR num_institutions >= 7   THEN 'high'
           ELSE 'medium'
         END AS severity
    RETURN
        co.name AS company_name,
        co.maticni_broj AS company_mb,
        num_institutions,
        total_contracts,
        total_value,
        round(avg_per_institution) AS avg_per_institution,
        sample_institutions,
        severity,
        'distributed_evasion' AS pattern_type
    ORDER BY total_value DESC
    LIMIT 50
    """, {"min_institutions": min_institutions, "min_total": min_total_rsd}


def party_linked_contractor(min_value_rsd: int = 2_000_000):
    """MP or party member directs a company that wins contracts from institutions
    where fellow party members are employed.

    Pattern: Person A (MP/party member) → DIRECTS → Company → WON_CONTRACT → Contract
             ← AWARDED_CONTRACT ← Institution ← EMPLOYED_BY ← Person B (same party as A)

    This is the 'party capture of procurement' pattern — party controls both sides.
    """
    return """
    MATCH (mp:Person)-[:MEMBER_OF]->(party:PoliticalParty)
    MATCH (mp)-[:OWNS|DIRECTS]->(co:Company)
    MATCH (co)-[:WON_CONTRACT]->(ct:Contract)
    MATCH (inst:Institution)-[:AWARDED_CONTRACT]->(ct)
    MATCH (official:Person)-[:EMPLOYED_BY]->(inst)
    MATCH (official)-[:MEMBER_OF]->(party)
    WHERE mp.person_id <> official.person_id
      AND coalesce(ct.value_rsd, ct.contract_value, 0) >= $min_value
    WITH mp, party, co, ct, inst, official,
         coalesce(ct.value_rsd, ct.contract_value, 0) AS val,
         CASE
           WHEN coalesce(ct.value_rsd, ct.contract_value, 0) >= 20000000 THEN 'critical'
           WHEN coalesce(ct.value_rsd, ct.contract_value, 0) >= 5000000  THEN 'high'
           ELSE 'medium'
         END AS severity
    RETURN
        mp.full_name AS mp_name,
        mp.person_id AS mp_id,
        party.name AS party_name,
        co.name AS company_name,
        co.maticni_broj AS company_mb,
        ct.title AS contract_title,
        ct.contract_id AS contract_id,
        val AS contract_value,
        ct.award_date AS award_date,
        inst.name AS institution,
        inst.institution_id AS institution_id,
        official.full_name AS party_official_at_institution,
        severity,
        'party_linked_contractor' AS pattern_type
    ORDER BY
        CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 ELSE 2 END,
        val DESC
    LIMIT 100
    """, {"min_value": min_value_rsd}


def suspicious_price_concentration(min_value_rsd: int = 2_000_000, min_contracts: int = 3):
    """A company wins the vast majority of contracts in its industry category.

    Uses APR activity_code to group companies by sector.
    If one company wins 60%+ of all contracts in its sector from a single institution,
    it likely has an exclusive arrangement — not a competitive market outcome.
    """
    return """
    MATCH (co:Company)-[:WON_CONTRACT]->(ct:Contract)
    MATCH (inst:Institution)-[:AWARDED_CONTRACT]->(ct)
    WHERE co.activity_code IS NOT NULL AND co.activity_code <> ''
    WITH co, inst, co.activity_code AS sector,
         sum(coalesce(ct.value_rsd, ct.contract_value, 0)) AS co_value,
         count(ct) AS co_contracts
    WHERE co_contracts >= $min_contracts AND co_value >= $min_value

    // Get total sector value at this institution
    WITH co, inst, sector, co_value, co_contracts
    MATCH (other:Company)-[:WON_CONTRACT]->(ct2:Contract)
    MATCH (inst)-[:AWARDED_CONTRACT]->(ct2)
    WHERE other.activity_code = sector
    WITH co, inst, sector, co_value, co_contracts,
         sum(coalesce(ct2.value_rsd, ct2.contract_value, 0)) AS sector_total,
         count(DISTINCT other) AS competitors
    WHERE sector_total > 0 AND competitors >= 2
      AND co_value * 1.0 / sector_total >= 0.6

    WITH co, inst, sector, co_value, co_contracts, sector_total, competitors,
         round(100.0 * co_value / sector_total) AS sector_pct,
         CASE
           WHEN co_value * 1.0 / sector_total >= 0.9 AND co_value >= 10000000 THEN 'critical'
           WHEN co_value * 1.0 / sector_total >= 0.75                          THEN 'high'
           ELSE 'medium'
         END AS severity
    RETURN
        co.name AS company_name,
        co.maticni_broj AS company_mb,
        sector AS activity_code,
        inst.name AS institution,
        inst.institution_id AS institution_id,
        co_value AS company_value,
        sector_total,
        sector_pct,
        co_contracts AS num_contracts,
        competitors AS num_competitors_in_sector,
        severity,
        'suspicious_price_concentration' AS pattern_type
    ORDER BY co_value DESC
    LIMIT 50
    """, {"min_value": min_value_rsd, "min_contracts": min_contracts}


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


# ── Aggregate risk scoring ────────────────────────────────────────

SEVERITY_WEIGHTS = {"critical": 10, "high": 5, "medium": 2, "low": 1}

ALL_DETECTORS = [
    # Core conflict patterns
    ("conflicts",                  conflict_of_interest),
    ("direct_official_contractor", lambda: direct_official_contractor()),
    ("samododeljivanje_proxy",     lambda: samododeljivanje_proxy()),
    ("party_linked_contractor",    lambda: party_linked_contractor()),
    # Procurement manipulation
    ("single_bidder",              lambda: single_bidder_contracts()),
    ("zero_competition_repeat",    lambda: zero_competition_repeat()),
    ("contract_splitting",         lambda: contract_splitting()),
    ("value_just_below_threshold", lambda: value_just_below_threshold()),
    # Market concentration
    ("repeated_winner",            lambda: repeated_winner()),
    ("institutional_monopoly",     lambda: institutional_monopoly()),
    ("suspicious_price_concentration", lambda: suspicious_price_concentration()),
    ("distributed_evasion",        lambda: distributed_evasion()),
    # Company/person patterns
    ("new_company_big_contract",   lambda: new_company_big_contract()),
    ("ghost_director",             lambda: ghost_director()),
    ("ghosts",                     ghost_employees),
    ("shells",                     shell_company_clusters),
    # Data-dependent (require specific relationship types)
    ("revolving_door",             revolving_door),
    ("budget_allocation",          budget_self_allocation),
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
            else "high"    if severity_counts["high"] > 2
            else "medium"  if total_score > 10
            else "low"
        ),
    }
