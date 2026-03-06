"""
Seed Graph: plants synthetic but realistic test data into Neo4j
that exercises all 8 corruption detection patterns.

Run via:  POST /seed
Or directly:  python -m backend.etl.seed_graph
"""
from datetime import datetime, timedelta
from backend.api.database import run_query
import structlog

logger = structlog.get_logger()


def _q(cypher: str, params: dict = None):
    run_query(cypher, params or {})


def clear_seed_data():
    """Remove only seed nodes (tagged with source='seed')."""
    _q("MATCH (n) WHERE n.source = 'seed' DETACH DELETE n")
    logger.info("seed_cleared")


def plant_seed_graph():
    """
    Inserts synthetic data covering all 8 detection patterns.
    All nodes are tagged source='seed' so they can be cleaned up.
    Target: ~80+ seed nodes total.
    """
    logger.info("seed_plant_start")
    now = datetime.utcnow().isoformat()

    # ── Shared address for shell cluster #1 ─────────────────────
    _q("""
        MERGE (a:Address {address_id: 'ADDR-SEED-1'})
        SET a.street = 'Nemanjina 4', a.city = 'Beograd',
            a.full_address = 'Nemanjina 4, Beograd', a.source = 'seed'
    """)

    # ── Shared address for shell cluster #2 (Novi Sad) ──────────
    _q("""
        MERGE (a:Address {address_id: 'ADDR-SEED-2'})
        SET a.street = 'Bulevar Oslobođenja 12', a.city = 'Novi Sad',
            a.full_address = 'Bulevar Oslobođenja 12, Novi Sad', a.source = 'seed'
    """)

    # ── Institutions (original 3) ────────────────────────────────
    institutions = [
        ("INST-SEED-MF",  "Ministarstvo finansija",                    "https://www.mfin.gov.rs"),
        ("INST-SEED-MUP", "Ministarstvo unutrašnjih poslova",          "https://www.mup.gov.rs"),
        ("INST-SEED-APN", "Agencija za privatizaciju",                 "https://www.priv.rs"),
    ]
    for iid, name, vurl in institutions:
        _q("""
            MERGE (i:Institution {institution_id: $iid})
            SET i.name = $name, i.source = 'seed', i.verification_url = $vurl
        """, {"iid": iid, "name": name, "vurl": vurl})

    # ── Institutions (5 new) ────────────────────────────────────
    new_institutions = [
        ("INST-SEED-RATEL", "Republička agencija za elektronske komunikacije", "https://www.ratel.rs"),
        ("INST-SEED-NS",    "Grad Novi Sad",                                   "https://www.novisad.rs"),
        ("INST-SEED-NIS",   "Grad Niš",                                        "https://www.ni.rs"),
        ("INST-SEED-PU",    "Poreska uprava",                                  "https://www.purs.gov.rs"),
        ("INST-SEED-REM",   "Regulatorno telo za elektronske medije",          "https://www.rem.rs"),
    ]
    for iid, name, vurl in new_institutions:
        _q("""
            MERGE (i:Institution {institution_id: $iid})
            SET i.name = $name, i.source = 'seed', i.verification_url = $vurl
        """, {"iid": iid, "name": name, "vurl": vurl})

    # ── Political parties (original 2) ───────────────────────────
    parties = [
        ("PARTY-SEED-SNS",  "Srpska napredna stranka"),
        ("PARTY-SEED-SPS",  "Socijalistička partija Srbije"),
    ]
    for ppid, name in parties:
        _q("""
            MERGE (pp:PoliticalParty {party_id: $ppid})
            SET pp.name = $name, pp.source = 'seed'
        """, {"ppid": ppid, "name": name})

    # ── Political parties (2 new) ────────────────────────────────
    new_parties = [
        ("PARTY-SEED-SSP",  "Stranka slobode i pravde"),
        ("PARTY-SEED-POKS", "Pokret obnove Kraljevine Srbije"),
    ]
    for ppid, name in new_parties:
        _q("""
            MERGE (pp:PoliticalParty {party_id: $ppid})
            SET pp.name = $name, pp.source = 'seed'
        """, {"ppid": ppid, "name": name})

    # ────────────────────────────────────────────────────────────
    # PATTERN 1: Conflict of interest
    # Official Dragan Petrović → family member Milica → Company Alfa → Contract from his institution
    # ────────────────────────────────────────────────────────────
    _q("""
        MERGE (p1:Person {person_id: 'PERSON-SEED-DRAGAN'})
        SET p1.full_name = 'Dragan Petrović', p1.name_normalized = 'dragan petrovic',
            p1.current_role = 'Direktor sektora', p1.source = 'seed',
            p1.verification_url = 'https://www.acas.rs/imovinski-registar/'
        WITH p1
        MATCH (i:Institution {institution_id: 'INST-SEED-MF'})
        MERGE (p1)-[:EMPLOYED_BY {role: 'Direktor sektora', since: '2019-01-01'}]->(i)
    """)
    _q("""
        MERGE (p2:Person {person_id: 'PERSON-SEED-MILICA'})
        SET p2.full_name = 'Milica Petrović', p2.name_normalized = 'milica petrovic',
            p2.source = 'seed',
            p2.verification_url = 'https://www.acas.rs/imovinski-registar/'
        WITH p2
        MATCH (p1:Person {person_id: 'PERSON-SEED-DRAGAN'})
        MERGE (p1)-[:FAMILY_OF]-(p2)
    """)
    _q("""
        MERGE (c1:Company {maticni_broj: 'MB-SEED-001'})
        SET c1.name = 'Alfa Konsalting d.o.o.', c1.name_normalized = 'alfa konsalting',
            c1.status = 'aktivna', c1.pib = 'PIB-SEED-001', c1.source = 'seed',
            c1.verification_url = 'https://pretraga.apr.gov.rs/unifiedsearch?searchTerm=MB-SEED-001'
        WITH c1
        MATCH (p2:Person {person_id: 'PERSON-SEED-MILICA'})
        MERGE (p2)-[:OWNS {role: 'osnivač', ownership_pct: 100.0}]->(c1)
    """)
    _q("""
        MERGE (ct1:Contract {contract_id: 'CT-SEED-001'})
        SET ct1.title = 'Usluge IT konsaltinga - MF 2023',
            ct1.value_rsd = 24000000, ct1.award_date = '2023-06-15',
            ct1.num_bidders = 1, ct1.procurement_type = 'usluge',
            ct1.status = 'zaključen', ct1.source = 'seed',
            ct1.verification_url = 'https://jnportal.ujn.gov.rs/tender-documents/search'
        WITH ct1
        MATCH (c1:Company {maticni_broj: 'MB-SEED-001'})
        MATCH (i:Institution {institution_id: 'INST-SEED-MF'})
        MERGE (c1)-[:WON_CONTRACT]->(ct1)
        MERGE (i)-[:AWARDED_CONTRACT]->(ct1)
    """)

    # ────────────────────────────────────────────────────────────
    # PATTERN 2: Ghost employees
    # "Jovan Nikolić" appears in MF payroll AND MUP payroll with different IDs
    # ────────────────────────────────────────────────────────────
    _q("""
        MERGE (g1:Person {person_id: 'PERSON-SEED-GHOST-A'})
        SET g1.full_name = 'Jovan Nikolić', g1.name_normalized = 'jovan nikolic',
            g1.source = 'seed',
            g1.verification_url = 'https://www.acas.rs/imovinski-registar/'
        WITH g1
        MATCH (i:Institution {institution_id: 'INST-SEED-MF'})
        MERGE (g1)-[:EMPLOYED_BY {role: 'Referent', since: '2021-01-01'}]->(i)
    """)
    _q("""
        MERGE (g2:Person {person_id: 'PERSON-SEED-GHOST-B'})
        SET g2.full_name = 'Jovan Nikolić', g2.name_normalized = 'jovan nikolic',
            g2.source = 'seed',
            g2.verification_url = 'https://www.acas.rs/imovinski-registar/'
        WITH g2
        MATCH (i:Institution {institution_id: 'INST-SEED-MUP'})
        MERGE (g2)-[:EMPLOYED_BY {role: 'Inspektor', since: '2020-03-01'}]->(i)
    """)

    # ────────────────────────────────────────────────────────────
    # PATTERN 3: Shell company cluster #1 (Beograd — Nemanjina 4)
    # 3 companies at the same address, collectively won 45M in contracts
    # ────────────────────────────────────────────────────────────
    shells = [
        ("MB-SEED-010", "Beta Trade d.o.o.",    "beta trade",    "PIB-SEED-010", "CT-SEED-010", "Isporuka kancelarijskog materijala", 14500000),
        ("MB-SEED-011", "Gama Promet d.o.o.",   "gama promet",   "PIB-SEED-011", "CT-SEED-011", "Isporuka prehrambenih artikala",     16000000),
        ("MB-SEED-012", "Delta Servis d.o.o.",  "delta servis",  "PIB-SEED-012", "CT-SEED-012", "Usluge čišćenja i održavanja",       15000000),
    ]
    for mb, name, norm, pib, ct_id, ct_title, ct_val in shells:
        _q("""
            MERGE (c:Company {maticni_broj: $mb})
            SET c.name = $name, c.name_normalized = $norm, c.pib = $pib,
                c.status = 'aktivna', c.source = 'seed',
                c.verification_url = $vurl
            WITH c
            MATCH (a:Address {address_id: 'ADDR-SEED-1'})
            MERGE (c)-[:REGISTERED_AT]->(a)
        """, {"mb": mb, "name": name, "norm": norm, "pib": pib,
              "vurl": f"https://pretraga.apr.gov.rs/unifiedsearch?searchTerm={mb}"})
        _q("""
            MERGE (ct:Contract {contract_id: $ct_id})
            SET ct.title = $title, ct.value_rsd = $val,
                ct.award_date = '2023-09-01', ct.num_bidders = 1,
                ct.procurement_type = 'dobra', ct.status = 'zaključen',
                ct.source = 'seed',
                ct.verification_url = 'https://jnportal.ujn.gov.rs/tender-documents/search'
            WITH ct
            MATCH (c:Company {maticni_broj: $mb})
            MATCH (i:Institution {institution_id: 'INST-SEED-MUP'})
            MERGE (c)-[:WON_CONTRACT]->(ct)
            MERGE (i)-[:AWARDED_CONTRACT]->(ct)
        """, {"ct_id": ct_id, "title": ct_title, "val": ct_val, "mb": mb})

    # ────────────────────────────────────────────────────────────
    # PATTERN 4: Single bidder contracts (above 1M RSD, num_bidders=1)
    # Already covered by CT-SEED-001 (24M) and shell contracts (14-16M)
    # Add a few more explicitly
    # ────────────────────────────────────────────────────────────
    _q("""
        MERGE (c:Company {maticni_broj: 'MB-SEED-020'})
        SET c.name = 'Epsilon IT d.o.o.', c.name_normalized = 'epsilon it',
            c.status = 'aktivna', c.pib = 'PIB-SEED-020', c.source = 'seed',
            c.verification_url = 'https://pretraga.apr.gov.rs/unifiedsearch?searchTerm=MB-SEED-020'
    """)
    for ct_id, title, val, date in [
        ("CT-SEED-020", "Razvoj informacionog sistema - faza 1", 45000000, "2023-03-10"),
        ("CT-SEED-021", "Razvoj informacionog sistema - faza 2", 38000000, "2023-05-22"),
    ]:
        _q("""
            MERGE (ct:Contract {contract_id: $ct_id})
            SET ct.title = $title, ct.value_rsd = $val,
                ct.award_date = $date, ct.num_bidders = 1,
                ct.procurement_type = 'usluge', ct.status = 'zaključen',
                ct.source = 'seed',
                ct.verification_url = 'https://jnportal.ujn.gov.rs/tender-documents/search'
            WITH ct
            MATCH (c:Company {maticni_broj: 'MB-SEED-020'})
            MATCH (inst:Institution {institution_id: 'INST-SEED-APN'})
            MERGE (c)-[:WON_CONTRACT]->(ct)
            MERGE (inst)-[:AWARDED_CONTRACT]->(ct)
        """, {"ct_id": ct_id, "title": title, "val": val, "date": date})

    # ────────────────────────────────────────────────────────────
    # PATTERN 5: Revolving door #1
    # Slobodan Đorđević LEFT MUP in 2022, then DIRECTS Zeta Security which got MUP contracts
    # ────────────────────────────────────────────────────────────
    _q("""
        MERGE (p:Person {person_id: 'PERSON-SEED-SLOBODAN'})
        SET p.full_name = 'Slobodan Đorđević', p.name_normalized = 'slobodan djordjevic',
            p.source = 'seed',
            p.verification_url = 'https://www.acas.rs/imovinski-registar/'
        WITH p
        MATCH (i:Institution {institution_id: 'INST-SEED-MUP'})
        MERGE (p)-[:EMPLOYED_BY {role: 'Pomoćnik ministra', since: '2017-01-01', until: '2022-06-30'}]->(i)
    """)
    _q("""
        MERGE (c:Company {maticni_broj: 'MB-SEED-030'})
        SET c.name = 'Zeta Security d.o.o.', c.name_normalized = 'zeta security',
            c.status = 'aktivna', c.pib = 'PIB-SEED-030', c.source = 'seed',
            c.verification_url = 'https://pretraga.apr.gov.rs/unifiedsearch?searchTerm=MB-SEED-030'
        WITH c
        MATCH (p:Person {person_id: 'PERSON-SEED-SLOBODAN'})
        MERGE (p)-[:DIRECTS {role: 'Direktor', since: '2022-08-01'}]->(c)
    """)
    _q("""
        MERGE (ct:Contract {contract_id: 'CT-SEED-030'})
        SET ct.title = 'Usluge fizičko-tehničke zaštite objekata MUP',
            ct.value_rsd = 31000000, ct.award_date = '2023-01-15',
            ct.num_bidders = 2, ct.procurement_type = 'usluge',
            ct.status = 'zaključen', ct.source = 'seed',
            ct.verification_url = 'https://jnportal.ujn.gov.rs/tender-documents/search'
        WITH ct
        MATCH (c:Company {maticni_broj: 'MB-SEED-030'})
        MATCH (i:Institution {institution_id: 'INST-SEED-MUP'})
        MERGE (c)-[:WON_CONTRACT]->(ct)
        MERGE (i)-[:AWARDED_CONTRACT]->(ct)
    """)

    # ────────────────────────────────────────────────────────────
    # PATTERN 6: Budget self-allocation #1
    # Oficial Dragan Petrović allocated a budget item that funds a contract won by his wife's company
    # ────────────────────────────────────────────────────────────
    _q("""
        MERGE (b:BudgetItem {budget_id: 'BUDGET-SEED-001'})
        SET b.fiscal_year = 2023, b.description = 'Digitalizacija poreske uprave',
            b.amount_rsd = 18000000, b.program_code = 'P402',
            b.program_name = 'E-uprava', b.appropriation_rsd = 18000000,
            b.execution_rsd = 18000000, b.source = 'seed'
        WITH b
        MATCH (p:Person {person_id: 'PERSON-SEED-DRAGAN'})
        MERGE (p)-[:ALLOCATED_BY]-(b)
    """)
    _q("""
        MERGE (ct:Contract {contract_id: 'CT-SEED-040'})
        SET ct.title = 'Digitalizacija poreske uprave - implementacija',
            ct.value_rsd = 18000000, ct.award_date = '2023-04-01',
            ct.num_bidders = 1, ct.procurement_type = 'usluge',
            ct.status = 'zaključen', ct.source = 'seed',
            ct.verification_url = 'https://jnportal.ujn.gov.rs/tender-documents/search'
        WITH ct
        MATCH (b:BudgetItem {budget_id: 'BUDGET-SEED-001'})
        MATCH (c:Company {maticni_broj: 'MB-SEED-001'})
        MATCH (i:Institution {institution_id: 'INST-SEED-MF'})
        MERGE (b)-[:FUNDS]->(ct)
        MERGE (c)-[:WON_CONTRACT]->(ct)
        MERGE (i)-[:AWARDED_CONTRACT]->(ct)
    """)

    # ────────────────────────────────────────────────────────────
    # PATTERN 7: Contract splitting #1
    # Same company, same institution, 3 contracts just below 6M threshold, within 60 days
    # ────────────────────────────────────────────────────────────
    _q("""
        MERGE (c:Company {maticni_broj: 'MB-SEED-040'})
        SET c.name = 'Eta Građevina d.o.o.', c.name_normalized = 'eta gradjevina',
            c.status = 'aktivna', c.pib = 'PIB-SEED-040', c.source = 'seed',
            c.verification_url = 'https://pretraga.apr.gov.rs/unifiedsearch?searchTerm=MB-SEED-040'
    """)
    for ct_id, title, val, date in [
        ("CT-SEED-050", "Radovi na adaptaciji - zgrada A", 5800000, "2023-10-01"),
        ("CT-SEED-051", "Radovi na adaptaciji - zgrada B", 5750000, "2023-10-28"),
        ("CT-SEED-052", "Radovi na adaptaciji - zgrada C", 5900000, "2023-11-20"),
    ]:
        _q("""
            MERGE (ct:Contract {contract_id: $ct_id})
            SET ct.title = $title, ct.value_rsd = $val,
                ct.award_date = $date, ct.num_bidders = 1,
                ct.procurement_type = 'radovi', ct.status = 'zaključen',
                ct.source = 'seed',
                ct.verification_url = 'https://jnportal.ujn.gov.rs/tender-documents/search'
            WITH ct
            MATCH (c:Company {maticni_broj: 'MB-SEED-040'})
            MATCH (i:Institution {institution_id: 'INST-SEED-APN'})
            MERGE (c)-[:WON_CONTRACT]->(ct)
            MERGE (i)-[:AWARDED_CONTRACT]->(ct)
        """, {"ct_id": ct_id, "title": title, "val": val, "date": date})

    # ────────────────────────────────────────────────────────────
    # PATTERN 8: Political donor contracts #1
    # Theta Group donated to SNS → won contract from institution where SNS member works
    # ────────────────────────────────────────────────────────────
    _q("""
        MERGE (c:Company {maticni_broj: 'MB-SEED-050'})
        SET c.name = 'Theta Group d.o.o.', c.name_normalized = 'theta group',
            c.status = 'aktivna', c.pib = 'PIB-SEED-050', c.source = 'seed',
            c.verification_url = 'https://pretraga.apr.gov.rs/unifiedsearch?searchTerm=MB-SEED-050'
        WITH c
        MATCH (pp:PoliticalParty {party_id: 'PARTY-SEED-SNS'})
        MERGE (c)-[:DONATED_TO {amount_rsd: 2500000, year: 2022}]->(pp)
    """)
    _q("""
        MERGE (ct:Contract {contract_id: 'CT-SEED-060'})
        SET ct.title = 'Izgradnja poslovnog centra - APN',
            ct.value_rsd = 85000000, ct.award_date = '2023-07-01',
            ct.num_bidders = 3, ct.procurement_type = 'radovi',
            ct.status = 'zaključen', ct.source = 'seed',
            ct.verification_url = 'https://jnportal.ujn.gov.rs/tender-documents/search'
        WITH ct
        MATCH (c:Company {maticni_broj: 'MB-SEED-050'})
        MATCH (i:Institution {institution_id: 'INST-SEED-APN'})
        MERGE (c)-[:WON_CONTRACT]->(ct)
        MERGE (i)-[:AWARDED_CONTRACT]->(ct)
    """)
    # SNS member working in APN
    _q("""
        MERGE (pm:Person {person_id: 'PERSON-SEED-PM-SNS'})
        SET pm.full_name = 'Miroslav Stanković', pm.name_normalized = 'miroslav stankovic',
            pm.current_role = 'Direktor', pm.party_name = 'Srpska napredna stranka',
            pm.source = 'seed',
            pm.verification_url = 'https://www.acas.rs/imovinski-registar/'
        WITH pm
        MATCH (i:Institution {institution_id: 'INST-SEED-APN'})
        MATCH (pp:PoliticalParty {party_id: 'PARTY-SEED-SNS'})
        MERGE (pm)-[:EMPLOYED_BY {role: 'Direktor', since: '2020-01-01'}]->(i)
        MERGE (pm)-[:MEMBER_OF]->(pp)
    """)

    # ════════════════════════════════════════════════════════════
    #  NEW DATA — 3× expansion
    # ════════════════════════════════════════════════════════════

    # ────────────────────────────────────────────────────────────
    # PATTERN 3 (new): Shell company cluster #2 — Novi Sad, Bulevar Oslobođenja 12
    # 4 companies registered at same address, each wins single-bidder contract from Grad NS
    # ────────────────────────────────────────────────────────────
    ns_shells = [
        ("MB-SEED-060", "Iota Gradnja d.o.o.",    "iota gradnja",    "PIB-SEED-060", "CT-SEED-NS-060", "Rekonstrukcija lokalnih puteva — zona 1", 9800000),
        ("MB-SEED-061", "Kapa Inženjering d.o.o.", "kapa inzenjering","PIB-SEED-061", "CT-SEED-NS-061", "Komunalna infrastruktura — blok 2",       11200000),
        ("MB-SEED-062", "Lambda Promet d.o.o.",    "lambda promet",   "PIB-SEED-062", "CT-SEED-NS-062", "Uređenje zelenih površina — park Šodrošr", 8400000),
        ("MB-SEED-063", "Mi Consulting d.o.o.",    "mi consulting",   "PIB-SEED-063", "CT-SEED-NS-063", "Izrada projektne dokumentacije — NS",     10600000),
    ]
    for mb, name, norm, pib, ct_id, ct_title, ct_val in ns_shells:
        _q("""
            MERGE (c:Company {maticni_broj: $mb})
            SET c.name = $name, c.name_normalized = $norm, c.pib = $pib,
                c.status = 'aktivna', c.source = 'seed',
                c.verification_url = $vurl
            WITH c
            MATCH (a:Address {address_id: 'ADDR-SEED-2'})
            MERGE (c)-[:REGISTERED_AT]->(a)
        """, {"mb": mb, "name": name, "norm": norm, "pib": pib,
              "vurl": f"https://pretraga.apr.gov.rs/unifiedsearch?searchTerm={mb}"})
        _q("""
            MERGE (ct:Contract {contract_id: $ct_id})
            SET ct.title = $title, ct.value_rsd = $val,
                ct.award_date = '2023-11-05', ct.num_bidders = 1,
                ct.procurement_type = 'radovi', ct.status = 'zaključen',
                ct.source = 'seed',
                ct.verification_url = 'https://jnportal.ujn.gov.rs/tender-documents/search'
            WITH ct
            MATCH (c:Company {maticni_broj: $mb})
            MATCH (i:Institution {institution_id: 'INST-SEED-NS'})
            MERGE (c)-[:WON_CONTRACT]->(ct)
            MERGE (i)-[:AWARDED_CONTRACT]->(ct)
        """, {"ct_id": ct_id, "title": ct_title, "val": ct_val, "mb": mb})

    # ────────────────────────────────────────────────────────────
    # PATTERN 1 (new): Conflict of interest #2 — Novi Sad
    # Bojana Savić (Grad NS official) → brother Stefan → Nu Gradnja → contract from Grad NS
    # ────────────────────────────────────────────────────────────
    _q("""
        MERGE (p:Person {person_id: 'PERSON-SEED-BOJANA'})
        SET p.full_name = 'Bojana Savić', p.name_normalized = 'bojana savic',
            p.current_role = 'Pomoćnik gradonačelnika', p.source = 'seed',
            p.verification_url = 'https://www.acas.rs/imovinski-registar/'
        WITH p
        MATCH (i:Institution {institution_id: 'INST-SEED-NS'})
        MERGE (p)-[:EMPLOYED_BY {role: 'Pomoćnik gradonačelnika', since: '2020-06-01'}]->(i)
    """)
    _q("""
        MERGE (p2:Person {person_id: 'PERSON-SEED-STEFAN'})
        SET p2.full_name = 'Stefan Savić', p2.name_normalized = 'stefan savic',
            p2.source = 'seed',
            p2.verification_url = 'https://www.acas.rs/imovinski-registar/'
        WITH p2
        MATCH (p1:Person {person_id: 'PERSON-SEED-BOJANA'})
        MERGE (p1)-[:FAMILY_OF]-(p2)
    """)
    _q("""
        MERGE (c:Company {maticni_broj: 'MB-SEED-070'})
        SET c.name = 'Nu Gradnja d.o.o.', c.name_normalized = 'nu gradnja',
            c.status = 'aktivna', c.pib = 'PIB-SEED-070', c.source = 'seed',
            c.verification_url = 'https://pretraga.apr.gov.rs/unifiedsearch?searchTerm=MB-SEED-070'
        WITH c
        MATCH (p2:Person {person_id: 'PERSON-SEED-STEFAN'})
        MERGE (p2)-[:OWNS {role: 'osnivač', ownership_pct: 100.0}]->(c)
    """)
    _q("""
        MERGE (ct:Contract {contract_id: 'CT-SEED-070'})
        SET ct.title = 'Rekonstrukcija trotoara — centar grada',
            ct.value_rsd = 18500000, ct.award_date = '2023-08-20',
            ct.num_bidders = 1, ct.procurement_type = 'radovi',
            ct.status = 'zaključen', ct.source = 'seed',
            ct.verification_url = 'https://jnportal.ujn.gov.rs/tender-documents/search'
        WITH ct
        MATCH (c:Company {maticni_broj: 'MB-SEED-070'})
        MATCH (i:Institution {institution_id: 'INST-SEED-NS'})
        MERGE (c)-[:WON_CONTRACT]->(ct)
        MERGE (i)-[:AWARDED_CONTRACT]->(ct)
    """)

    # ────────────────────────────────────────────────────────────
    # PATTERN 5 (new): Revolving door #2 — RATEL to telecom
    # Predrag Vasić LEFT RATEL end-2021, then DIRECTS Omega Telecom which got RATEL contract
    # ────────────────────────────────────────────────────────────
    _q("""
        MERGE (p:Person {person_id: 'PERSON-SEED-PREDRAG'})
        SET p.full_name = 'Predrag Vasić', p.name_normalized = 'predrag vasic',
            p.source = 'seed',
            p.verification_url = 'https://www.acas.rs/imovinski-registar/'
        WITH p
        MATCH (i:Institution {institution_id: 'INST-SEED-RATEL'})
        MERGE (p)-[:EMPLOYED_BY {role: 'Zamenik direktora', since: '2016-03-01', until: '2021-12-31'}]->(i)
    """)
    _q("""
        MERGE (c:Company {maticni_broj: 'MB-SEED-080'})
        SET c.name = 'Omega Telecom d.o.o.', c.name_normalized = 'omega telecom',
            c.status = 'aktivna', c.pib = 'PIB-SEED-080', c.source = 'seed',
            c.verification_url = 'https://pretraga.apr.gov.rs/unifiedsearch?searchTerm=MB-SEED-080'
        WITH c
        MATCH (p:Person {person_id: 'PERSON-SEED-PREDRAG'})
        MERGE (p)-[:DIRECTS {role: 'Direktor', since: '2022-02-15'}]->(c)
    """)
    _q("""
        MERGE (ct:Contract {contract_id: 'CT-SEED-080'})
        SET ct.title = 'Usluge tehničke podrške RATEL',
            ct.value_rsd = 22000000, ct.award_date = '2022-09-01',
            ct.num_bidders = 1, ct.procurement_type = 'usluge',
            ct.status = 'zaključen', ct.source = 'seed',
            ct.verification_url = 'https://jnportal.ujn.gov.rs/tender-documents/search'
        WITH ct
        MATCH (c:Company {maticni_broj: 'MB-SEED-080'})
        MATCH (i:Institution {institution_id: 'INST-SEED-RATEL'})
        MERGE (c)-[:WON_CONTRACT]->(ct)
        MERGE (i)-[:AWARDED_CONTRACT]->(ct)
    """)

    # ────────────────────────────────────────────────────────────
    # PATTERN 2 (new): Ghost employees #2 — "Ana Đorđević" in PU and RATEL
    # ────────────────────────────────────────────────────────────
    _q("""
        MERGE (g:Person {person_id: 'PERSON-SEED-GHOST-C'})
        SET g.full_name = 'Ana Đorđević', g.name_normalized = 'ana djordjevic',
            g.source = 'seed',
            g.verification_url = 'https://www.acas.rs/imovinski-registar/'
        WITH g
        MATCH (i:Institution {institution_id: 'INST-SEED-PU'})
        MERGE (g)-[:EMPLOYED_BY {role: 'Poreski inspektor', since: '2020-04-01'}]->(i)
    """)
    _q("""
        MERGE (g:Person {person_id: 'PERSON-SEED-GHOST-D'})
        SET g.full_name = 'Ana Đorđević', g.name_normalized = 'ana djordjevic',
            g.source = 'seed',
            g.verification_url = 'https://www.acas.rs/imovinski-registar/'
        WITH g
        MATCH (i:Institution {institution_id: 'INST-SEED-RATEL'})
        MERGE (g)-[:EMPLOYED_BY {role: 'Savetnik za regulativu', since: '2019-09-01'}]->(i)
    """)

    # ────────────────────────────────────────────────────────────
    # PATTERN 2 (new): Ghost employees #3 — "Zoran Milošević" in NS and Niš
    # ────────────────────────────────────────────────────────────
    _q("""
        MERGE (g:Person {person_id: 'PERSON-SEED-GHOST-E'})
        SET g.full_name = 'Zoran Milošević', g.name_normalized = 'zoran milosevic',
            g.source = 'seed',
            g.verification_url = 'https://www.acas.rs/imovinski-registar/'
        WITH g
        MATCH (i:Institution {institution_id: 'INST-SEED-NS'})
        MERGE (g)-[:EMPLOYED_BY {role: 'Šef odseka', since: '2018-02-01'}]->(i)
    """)
    _q("""
        MERGE (g:Person {person_id: 'PERSON-SEED-GHOST-F'})
        SET g.full_name = 'Zoran Milošević', g.name_normalized = 'zoran milosevic',
            g.source = 'seed',
            g.verification_url = 'https://www.acas.rs/imovinski-registar/'
        WITH g
        MATCH (i:Institution {institution_id: 'INST-SEED-NIS'})
        MERGE (g)-[:EMPLOYED_BY {role: 'Viši savetnik', since: '2019-05-15'}]->(i)
    """)

    # ────────────────────────────────────────────────────────────
    # PATTERN 7 (new): Contract splitting #2 — Niš
    # Pi Elektro wins 4 contracts from Grad Niš, each ~5.5M, within 75 days
    # ────────────────────────────────────────────────────────────
    _q("""
        MERGE (c:Company {maticni_broj: 'MB-SEED-090'})
        SET c.name = 'Pi Elektro d.o.o.', c.name_normalized = 'pi elektro',
            c.status = 'aktivna', c.pib = 'PIB-SEED-090', c.source = 'seed',
            c.verification_url = 'https://pretraga.apr.gov.rs/unifiedsearch?searchTerm=MB-SEED-090'
    """)
    for ct_id, title, val, date in [
        ("CT-SEED-090", "Javna rasveta — zona A", 5500000, "2024-01-15"),
        ("CT-SEED-091", "Javna rasveta — zona B", 5600000, "2024-02-20"),
        ("CT-SEED-092", "Javna rasveta — zona C", 5450000, "2024-03-10"),
        ("CT-SEED-093", "Javna rasveta — zona D", 5700000, "2024-03-28"),
    ]:
        _q("""
            MERGE (ct:Contract {contract_id: $ct_id})
            SET ct.title = $title, ct.value_rsd = $val,
                ct.award_date = $date, ct.num_bidders = 1,
                ct.procurement_type = 'radovi', ct.status = 'zaključen',
                ct.source = 'seed',
                ct.verification_url = 'https://jnportal.ujn.gov.rs/tender-documents/search'
            WITH ct
            MATCH (c:Company {maticni_broj: 'MB-SEED-090'})
            MATCH (i:Institution {institution_id: 'INST-SEED-NIS'})
            MERGE (c)-[:WON_CONTRACT]->(ct)
            MERGE (i)-[:AWARDED_CONTRACT]->(ct)
        """, {"ct_id": ct_id, "title": title, "val": val, "date": date})

    # ────────────────────────────────────────────────────────────
    # PATTERN 8 (new): Political donor contracts #2 — SSP → Grad Niš
    # Ro Consulting donated to SSP → won contract from Grad Niš where SSP member works
    # ────────────────────────────────────────────────────────────
    _q("""
        MERGE (c:Company {maticni_broj: 'MB-SEED-100'})
        SET c.name = 'Ro Consulting d.o.o.', c.name_normalized = 'ro consulting',
            c.status = 'aktivna', c.pib = 'PIB-SEED-100', c.source = 'seed',
            c.verification_url = 'https://pretraga.apr.gov.rs/unifiedsearch?searchTerm=MB-SEED-100'
        WITH c
        MATCH (pp:PoliticalParty {party_id: 'PARTY-SEED-SSP'})
        MERGE (c)-[:DONATED_TO {amount_rsd: 1800000, year: 2023}]->(pp)
    """)
    _q("""
        MERGE (ct:Contract {contract_id: 'CT-SEED-100'})
        SET ct.title = 'Izrada strateškog plana razvoja',
            ct.value_rsd = 12000000, ct.award_date = '2023-10-15',
            ct.num_bidders = 2, ct.procurement_type = 'usluge',
            ct.status = 'zaključen', ct.source = 'seed',
            ct.verification_url = 'https://jnportal.ujn.gov.rs/tender-documents/search'
        WITH ct
        MATCH (c:Company {maticni_broj: 'MB-SEED-100'})
        MATCH (i:Institution {institution_id: 'INST-SEED-NIS'})
        MERGE (c)-[:WON_CONTRACT]->(ct)
        MERGE (i)-[:AWARDED_CONTRACT]->(ct)
    """)
    # SSP member working in Grad Niš
    _q("""
        MERGE (p:Person {person_id: 'PERSON-SEED-VESNA'})
        SET p.full_name = 'Vesna Ilić', p.name_normalized = 'vesna ilic',
            p.current_role = 'Načelnik uprave', p.source = 'seed',
            p.verification_url = 'https://www.acas.rs/imovinski-registar/'
        WITH p
        MATCH (i:Institution {institution_id: 'INST-SEED-NIS'})
        MATCH (pp:PoliticalParty {party_id: 'PARTY-SEED-SSP'})
        MERGE (p)-[:EMPLOYED_BY {role: 'Načelnik uprave', since: '2021-04-01'}]->(i)
        MERGE (p)-[:MEMBER_OF]->(pp)
    """)

    # ────────────────────────────────────────────────────────────
    # PATTERN 6 (new): Budget self-allocation #2
    # Miroslav Stanković (PERSON-SEED-PM-SNS) allocated BUDGET-SEED-002
    # which funds Smart city contract won by Theta Group (MB-SEED-050)
    # ────────────────────────────────────────────────────────────
    _q("""
        MERGE (b:BudgetItem {budget_id: 'BUDGET-SEED-002'})
        SET b.fiscal_year = 2024, b.description = 'Infrastruktura pametnog grada',
            b.amount_rsd = 32000000, b.program_code = 'P510',
            b.program_name = 'Smart City', b.appropriation_rsd = 32000000,
            b.execution_rsd = 32000000, b.source = 'seed'
        WITH b
        MATCH (p:Person {person_id: 'PERSON-SEED-PM-SNS'})
        MERGE (p)-[:ALLOCATED_BY]-(b)
    """)
    _q("""
        MERGE (ct:Contract {contract_id: 'CT-SEED-110'})
        SET ct.title = 'Smart city platforma',
            ct.value_rsd = 32000000, ct.award_date = '2024-02-10',
            ct.num_bidders = 1, ct.procurement_type = 'usluge',
            ct.status = 'zaključen', ct.source = 'seed',
            ct.verification_url = 'https://jnportal.ujn.gov.rs/tender-documents/search'
        WITH ct
        MATCH (b:BudgetItem {budget_id: 'BUDGET-SEED-002'})
        MATCH (c:Company {maticni_broj: 'MB-SEED-050'})
        MATCH (i:Institution {institution_id: 'INST-SEED-APN'})
        MERGE (b)-[:FUNDS]->(ct)
        MERGE (c)-[:WON_CONTRACT]->(ct)
        MERGE (i)-[:AWARDED_CONTRACT]->(ct)
    """)

    # ── Bulk-set verification_url on all seed nodes ──────────────
    _add_verification_urls()

    logger.info("seed_plant_done")
    return {"status": "done", "patterns_seeded": 8}


def _add_verification_urls():
    """
    Bulk SET verification_url on every seed node that is missing one,
    using safe defaults per label so previously-set specific URLs are preserved.
    """
    # Companies: APR lookup by maticni_broj
    _q("""
        MATCH (c:Company)
        WHERE c.source = 'seed' AND c.verification_url IS NULL
        SET c.verification_url =
            'https://pretraga.apr.gov.rs/unifiedsearch?searchTerm=' + c.maticni_broj
    """)

    # Contracts: UJN portal (single canonical search page)
    _q("""
        MATCH (ct:Contract)
        WHERE ct.source = 'seed' AND ct.verification_url IS NULL
        SET ct.verification_url = 'https://jnportal.ujn.gov.rs/tender-documents/search'
    """)

    # Persons: ACAS imovinski registar
    _q("""
        MATCH (p:Person)
        WHERE p.source = 'seed' AND p.verification_url IS NULL
        SET p.verification_url = 'https://www.acas.rs/imovinski-registar/'
    """)

    # Institutions already set individually above; catch any gaps
    _q("""
        MATCH (i:Institution)
        WHERE i.source = 'seed' AND i.verification_url IS NULL
        SET i.verification_url = 'https://www.srbija.gov.rs'
    """)

    logger.info("verification_urls_set")


def summarize():
    """Return counts of seeded nodes."""
    from backend.api.database import run_query_single
    result = run_query_single("""
        CALL () {
            MATCH (n) WHERE n.source = 'seed'
            RETURN count(n) AS seed_nodes
        }
        CALL () {
            MATCH (n:Person) WHERE n.source = 'seed' RETURN count(n) AS persons
        }
        CALL () {
            MATCH (n:Company) WHERE n.source = 'seed' RETURN count(n) AS companies
        }
        CALL () {
            MATCH (n:Contract) WHERE n.source = 'seed' RETURN count(n) AS contracts
        }
        RETURN seed_nodes, persons, companies, contracts
    """)
    return result


if __name__ == "__main__":
    plant_seed_graph()
    print(summarize())
