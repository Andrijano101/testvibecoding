"""
Graph Loader: loads scraped raw data into Neo4j.

Usage:
    loader = GraphLoader()
    loader.load_all()          # load everything in data/raw/
    loader.load_apr_data()     # load only APR data
    loader.load_procurement_data()  # load only procurement data
"""
import os
import json
import glob
from datetime import datetime
from typing import Optional

from backend.api.database import run_query
from backend.etl.entity_resolver import EntityResolver, normalize_company_name
import structlog

logger = structlog.get_logger()

DATA_DIR = os.getenv("DATA_DIR", "./data")
RESOLVER_STATE = os.path.join(DATA_DIR, "resolver_state.json")


class GraphLoader:
    """Loads scraped data files into Neo4j."""

    def __init__(self):
        self.resolver = EntityResolver()
        if os.path.exists(RESOLVER_STATE):
            self.resolver.load(RESOLVER_STATE)

    def load_all(self):
        """Load all available raw data."""
        self.load_apr_data()
        self.load_procurement_data()
        self.load_rik_data()
        self.load_gazette_data()
        self.load_rgz_data()
        self.load_opendata()
        self.load_ujn_institutions()
        self.load_ujn_procurements()
        self.load_parties()
        self.resolver.save(RESOLVER_STATE)
        logger.info("graph_load_complete", stats=self.resolver.stats())

    def load_ujn_institutions(self):
        """Load institution records produced by the UJN bulk scraper."""
        inst_dir = os.path.join(DATA_DIR, "raw", "institutions")
        files = glob.glob(os.path.join(inst_dir, "ujn_*.json"))
        logger.info("ujn_inst_load_start", files=len(files))
        loaded = 0
        for path in files:
            try:
                with open(path, encoding="utf-8") as f:
                    records = json.load(f)
                for rec in records:
                    self._load_ujn_institution(rec)
                    loaded += 1
            except Exception as e:
                logger.error("ujn_inst_load_failed", path=path, error=str(e))
        logger.info("ujn_inst_load_done", loaded=loaded)

    def load_ujn_procurements(self):
        """Load procurement notices produced by the UJN bulk scraper."""
        ujn_dir = os.path.join(DATA_DIR, "raw", "ujn")
        files = glob.glob(os.path.join(ujn_dir, "procurements_*.json"))
        logger.info("ujn_proc_load_start", files=len(files))
        loaded = 0
        for path in files:
            try:
                with open(path, encoding="utf-8") as f:
                    records = json.load(f)
                for rec in records:
                    self._load_ujn_procurement(rec)
                    loaded += 1
            except Exception as e:
                logger.error("ujn_proc_load_failed", path=path, error=str(e))
        logger.info("ujn_proc_load_done", loaded=loaded)

    def load_parties(self):
        """Load political parties from data.gov.rs scraper output."""
        path = os.path.join(DATA_DIR, "raw", "opendata", "parties.json")
        if not os.path.exists(path):
            return
        with open(path, encoding="utf-8") as f:
            records = json.load(f)
        loaded = 0
        for rec in records:
            self._load_party(rec)
            loaded += 1
        logger.info("parties_load_done", loaded=loaded)

    def _load_ujn_institution(self, record: dict):
        """MERGE a real public institution from UJN data."""
        iid = record.get("institution_id")
        name = record.get("name", "")
        mb = record.get("maticni_broj", "")
        if not iid or not name:
            return
        run_query("""
            MERGE (i:Institution {institution_id: $iid})
            SET i.name             = $name,
                i.name_normalized  = $name_norm,
                i.maticni_broj     = $mb,
                i.pib              = $pib,
                i.source           = 'ujn',
                i.verification_url = $vurl,
                i.updated_at       = $now
        """, {
            "iid": iid,
            "name": name,
            "name_norm": record.get("name_normalized", ""),
            "mb": mb,
            "pib": record.get("pib", ""),
            "vurl": f"http://portal.ujn.gov.rs/Pretrage/Narucilac.aspx?mb={mb}" if mb else record.get("source_url", ""),
            "now": datetime.utcnow().isoformat(),
        })

    def _load_ujn_procurement(self, record: dict):
        """MERGE a procurement notice and link to its institution and supplier (if available)."""
        cid = record.get("contract_id")
        title = record.get("title") or record.get("subject", "")
        inst_mb = record.get("institution_mb", "")
        if not cid or not title:
            return

        run_query("""
            MERGE (c:Contract {contract_id: $cid})
            SET c.title            = $title,
                c.subject          = $subject,
                c.proc_type        = $proc_type,
                c.subject_type     = $subject_type,
                c.award_date       = $date,
                c.has_award        = $has_award,
                c.contract_value   = $value,
                c.currency         = $currency,
                c.source           = 'ujn',
                c.verification_url = $vurl,
                c.updated_at       = $now
        """, {
            "cid": cid,
            "title": str(title)[:250],
            "subject": str(record.get("subject", ""))[:250],
            "proc_type": record.get("proc_type", ""),
            "subject_type": record.get("subject_type", ""),
            "date": record.get("date_modified", ""),
            "has_award": bool(record.get("has_award_decision", False)),
            "value": record.get("contract_value"),
            "currency": record.get("currency", "RSD"),
            "vurl": record.get("detail_url", ""),
            "now": datetime.utcnow().isoformat(),
        })

        # Link to institution
        if inst_mb:
            run_query("""
                MATCH (i:Institution {maticni_broj: $mb})
                MATCH (c:Contract {contract_id: $cid})
                MERGE (i)-[:AWARDED_CONTRACT]->(c)
            """, {"mb": inst_mb, "cid": cid})

        # Supplier / Company (if present in OpenData)
        supplier_mb = (record.get("supplier_mb") or "").strip()
        supplier_name = (record.get("supplier_name") or "").strip()

        if supplier_mb or supplier_name:
            # If we have no MB, create a stable pseudo id by hashing name
            mb = supplier_mb if supplier_mb else f"NO_MB_{abs(hash(supplier_name)) % 10**10}"

            run_query("""
                MERGE (co:Company {maticni_broj: $mb})
                SET co.name = coalesce(co.name, $name),
                    co.source = coalesce(co.source, 'ujn_opend'),
                    co.updated_at = $now
            """, {"mb": mb, "name": supplier_name or mb, "now": datetime.utcnow().isoformat()})

            run_query("""
                MATCH (co:Company {maticni_broj: $mb})
                MATCH (c:Contract {contract_id: $cid})
                MERGE (co)-[r:WON_CONTRACT]->(c)
                SET r.source = 'ujn_opend'
            """, {"mb": mb, "cid": cid})

    def _load_party(self, record: dict):
        """MERGE a real political party from data.gov.rs."""
        party_id = record.get("party_id")
        name = record.get("name", "")
        if not party_id or not name:
            return
        run_query("""
            MERGE (pp:PoliticalParty {party_id: $pid})
            SET pp.name            = $name,
                pp.name_normalized = $name_norm,
                pp.abbreviation    = $abbr,
                pp.founded         = $founded,
                pp.address         = $address,
                pp.city            = $city,
                pp.leader          = $leader,
                pp.source          = 'data.gov.rs',
                pp.verification_url = 'https://data.gov.rs/sr/datasets/politichke-stranke/',
                pp.updated_at      = $now
        """, {
            "pid": party_id,
            "name": name,
            "name_norm": record.get("name_normalized", ""),
            "abbr": record.get("abbreviation", ""),
            "founded": record.get("founded", ""),
            "address": record.get("address", ""),
            "city": record.get("city", ""),
            "leader": record.get("leader", ""),
            "now": datetime.utcnow().isoformat(),
        })

    def load_apr_data(self):
        """Load all APR JSON files from data/raw/apr/."""
        apr_dir = os.path.join(DATA_DIR, "raw", "apr")
        files = glob.glob(os.path.join(apr_dir, "*.json"))
        logger.info("apr_load_start", files=len(files))
        loaded = 0
        for path in files:
            try:
                with open(path, encoding="utf-8") as f:
                    record = json.load(f)
                self._load_company(record)
                loaded += 1
            except Exception as e:
                logger.error("apr_load_failed", path=path, error=str(e))
        logger.info("apr_load_done", loaded=loaded, total=len(files))

    def load_procurement_data(self):
        """Load all procurement JSON files from data/raw/procurement/."""
        proc_dir = os.path.join(DATA_DIR, "raw", "procurement")
        files = glob.glob(os.path.join(proc_dir, "*.json"))
        logger.info("procurement_load_start", files=len(files))
        loaded = 0
        for path in files:
            try:
                with open(path, encoding="utf-8") as f:
                    record = json.load(f)
                self._load_contract(record)
                loaded += 1
            except Exception as e:
                logger.error("procurement_load_failed", path=path, error=str(e))
        logger.info("procurement_load_done", loaded=loaded, total=len(files))

    def load_rik_data(self):
        """Load all RIK JSON files from data/raw/rik/."""
        rik_dir = os.path.join(DATA_DIR, "raw", "rik")
        files = glob.glob(os.path.join(rik_dir, "*.json"))
        logger.info("rik_load_start", files=len(files))
        loaded = 0
        for path in files:
            try:
                with open(path, encoding="utf-8") as f:
                    record = json.load(f)
                self._load_official(record)
                loaded += 1
            except Exception as e:
                logger.error("rik_load_failed", path=path, error=str(e))
        logger.info("rik_load_done", loaded=loaded, total=len(files))

    def load_gazette_data(self):
        """Load all gazette JSON files from data/raw/gazette/."""
        gazette_dir = os.path.join(DATA_DIR, "raw", "gazette")
        files = glob.glob(os.path.join(gazette_dir, "*.json"))
        logger.info("gazette_load_start", files=len(files))
        loaded = 0
        for path in files:
            try:
                with open(path, encoding="utf-8") as f:
                    record = json.load(f)
                self._load_gazette_appointment(record)
                loaded += 1
            except Exception as e:
                logger.error("gazette_load_failed", path=path, error=str(e))
        logger.info("gazette_load_done", loaded=loaded, total=len(files))

    def load_rgz_data(self):
        """Load all RGZ JSON files from data/raw/rgz/."""
        rgz_dir = os.path.join(DATA_DIR, "raw", "rgz")
        files = glob.glob(os.path.join(rgz_dir, "*.json"))
        logger.info("rgz_load_start", files=len(files))
        loaded = 0
        for path in files:
            try:
                with open(path, encoding="utf-8") as f:
                    record = json.load(f)
                self._load_property(record)
                loaded += 1
            except Exception as e:
                logger.error("rgz_load_failed", path=path, error=str(e))
        logger.info("rgz_load_done", loaded=loaded, total=len(files))

    def load_opendata(self):
        """Load all OpenData JSON files from data/raw/opendata/."""
        opendata_dir = os.path.join(DATA_DIR, "raw", "opendata")
        files = glob.glob(os.path.join(opendata_dir, "*.json"))
        logger.info("opendata_load_start", files=len(files))
        loaded = 0
        for path in files:
            try:
                filename = os.path.basename(path)
                with open(path, encoding="utf-8") as f:
                    records = json.load(f)
                if not isinstance(records, list):
                    records = [records]
                for record in records:
                    if "budget_id" in record:
                        self._load_budget_item(record)
                        loaded += 1
                    elif "official_id" in record:
                        self._load_official(record)
                        loaded += 1
            except Exception as e:
                logger.error("opendata_load_failed", path=path, error=str(e))
        logger.info("opendata_load_done", loaded=loaded, total=len(files))

    def _load_official(self, record: dict):
        """Merge an elected official or public servant into the graph."""
        # Support both RIK (person_id) and OpenData (official_id) formats
        pid = record.get("person_id") or record.get("official_id")
        name = record.get("full_name", "")
        if not pid or not name:
            return

        resolved = self.resolver.resolve_person(name, source="rik")
        canonical_pid = resolved.canonical_id

        run_query("""
            MERGE (p:Person {person_id: $pid})
            SET p.full_name        = $name,
                p.name_normalized  = $name_norm,
                p.current_role     = $role,
                p.party_name       = $party,
                p.source           = $source,
                p.updated_at       = $now
        """, {
            "pid": canonical_pid,
            "name": name,
            "name_norm": record.get("name_normalized", ""),
            "role": record.get("position_title", record.get("role", "")),
            "party": record.get("party_name", ""),
            "source": "rik" if "person_id" in record else "opendata",
            "now": datetime.utcnow().isoformat(),
        })

        # Link to institution
        inst_name = record.get("institution_name", "")
        inst_id = record.get("institution_id", "")
        if inst_name:
            if not inst_id:
                inst_id = f"INST-{abs(hash(inst_name)) % 10**8}"
            run_query("""
                MERGE (i:Institution {institution_id: $iid})
                SET i.name = $name
                WITH i
                MATCH (p:Person {person_id: $pid})
                MERGE (p)-[r:EMPLOYED_BY]->(i)
                SET r.role  = $role,
                    r.since = $since,
                    r.until = $until
            """, {
                "iid": inst_id,
                "name": inst_name,
                "pid": canonical_pid,
                "role": record.get("position_title", ""),
                "since": record.get("term_start", record.get("appointment_date", "")),
                "until": record.get("term_end", ""),
            })

        # Link to political party
        party_name = record.get("party_name", "")
        party_id = record.get("party_id", "")
        if party_name:
            if not party_id:
                party_id = f"PARTY-{abs(hash(party_name)) % 10**6}"
            run_query("""
                MERGE (pp:PoliticalParty {party_id: $ppid})
                SET pp.name = $party_name
                WITH pp
                MATCH (p:Person {person_id: $pid})
                MERGE (p)-[:MEMBER_OF]->(pp)
            """, {
                "ppid": party_id,
                "party_name": party_name,
                "pid": canonical_pid,
            })

    def _load_gazette_appointment(self, record: dict):
        """Merge a gazette appointment record into the graph."""
        person_name = record.get("person_name", "")
        inst_name = record.get("institution_name", "")
        if not person_name:
            return

        resolved = self.resolver.resolve_person(person_name, source="gazette")
        pid = resolved.canonical_id

        run_query("""
            MERGE (p:Person {person_id: $pid})
            SET p.full_name       = $name,
                p.name_normalized = $name_norm,
                p.source          = 'gazette',
                p.updated_at      = $now
        """, {
            "pid": pid,
            "name": person_name,
            "name_norm": record.get("person_name_normalized", ""),
            "now": datetime.utcnow().isoformat(),
        })

        if inst_name:
            inst_id = record.get("institution_id") or f"INST-{abs(hash(inst_name)) % 10**8}"
            run_query("""
                MERGE (i:Institution {institution_id: $iid})
                SET i.name = $name
                WITH i
                MATCH (p:Person {person_id: $pid})
                MERGE (p)-[r:EMPLOYED_BY]->(i)
                SET r.role  = $role,
                    r.since = $since
            """, {
                "iid": inst_id,
                "name": inst_name,
                "pid": pid,
                "role": record.get("position_title", ""),
                "since": record.get("effective_date", record.get("publication_date", "")),
            })

    def _load_property(self, record: dict):
        """Merge a property record and link to owner Person or Company."""
        prop_id = record.get("property_id", "")
        if not prop_id:
            return

        run_query("""
            MERGE (pr:Property {property_id: $pid})
            SET pr.cadastral_id  = $cadastral,
                pr.address       = $address,
                pr.city          = $city,
                pr.municipality  = $municipality,
                pr.area_sqm      = $area,
                pr.property_type = $ptype,
                pr.source        = 'rgz',
                pr.updated_at    = $now
        """, {
            "pid": prop_id,
            "cadastral": record.get("cadastral_id", ""),
            "address": record.get("address", ""),
            "city": record.get("city", ""),
            "municipality": record.get("municipality", ""),
            "area": record.get("area_sqm"),
            "ptype": record.get("property_type", ""),
            "now": datetime.utcnow().isoformat(),
        })

        owner_name = record.get("owner_name", "")
        owner_type = record.get("owner_type", "person")
        owner_mb = record.get("owner_mb", "")

        if owner_name and owner_type == "person":
            resolved = self.resolver.resolve_person(owner_name, source="rgz")
            owner_pid = resolved.canonical_id
            run_query("""
                MERGE (p:Person {person_id: $pid})
                SET p.full_name       = $name,
                    p.name_normalized = $name_norm
                WITH p
                MATCH (pr:Property {property_id: $prop_id})
                MERGE (p)-[r:OWNS_PROPERTY]->(pr)
                SET r.pct              = $pct,
                    r.acquisition_date = $acq_date
            """, {
                "pid": owner_pid,
                "name": owner_name,
                "name_norm": record.get("owner_name_normalized", ""),
                "prop_id": prop_id,
                "pct": record.get("ownership_pct", 100.0),
                "acq_date": record.get("acquisition_date", ""),
            })
        elif owner_name and owner_type == "company" and owner_mb:
            run_query("""
                MERGE (c:Company {maticni_broj: $mb})
                SET c.name = coalesce(c.name, $name)
                WITH c
                MATCH (pr:Property {property_id: $prop_id})
                MERGE (c)-[r:OWNS_PROPERTY]->(pr)
                SET r.pct              = $pct,
                    r.acquisition_date = $acq_date
            """, {
                "mb": owner_mb,
                "name": owner_name,
                "prop_id": prop_id,
                "pct": record.get("ownership_pct", 100.0),
                "acq_date": record.get("acquisition_date", ""),
            })

    def _load_budget_item(self, record: dict):
        """Merge a budget item and link to its institution."""
        budget_id = record.get("budget_id", "")
        if not budget_id:
            return

        run_query("""
            MERGE (b:BudgetItem {budget_id: $bid})
            SET b.fiscal_year              = $year,
                b.program_code             = $prog_code,
                b.program_name             = $prog_name,
                b.economic_classification  = $econ,
                b.appropriation_rsd        = $approp,
                b.execution_rsd            = $exec,
                b.execution_pct            = $exec_pct,
                b.source                   = 'opendata',
                b.updated_at               = $now
        """, {
            "bid": budget_id,
            "year": record.get("fiscal_year"),
            "prog_code": record.get("program_code", ""),
            "prog_name": record.get("program_name", ""),
            "econ": record.get("economic_classification", ""),
            "approp": record.get("appropriation_rsd"),
            "exec": record.get("execution_rsd"),
            "exec_pct": record.get("execution_pct"),
            "now": datetime.utcnow().isoformat(),
        })

        inst_name = record.get("institution_name", "")
        if inst_name:
            inst_id = record.get("institution_id") or f"INST-{abs(hash(inst_name)) % 10**8}"
            run_query("""
                MERGE (i:Institution {institution_id: $iid})
                SET i.name = $name
                WITH i
                MATCH (b:BudgetItem {budget_id: $bid})
                MERGE (i)-[:HAS_BUDGET]->(b)
            """, {"iid": inst_id, "name": inst_name, "bid": budget_id})

    def _load_company(self, record: dict):
        """Merge a company record into Neo4j."""
        mb = record.get("maticni_broj", "")
        if not mb:
            return

        resolved = self.resolver.resolve_company(
            record.get("name", ""), mb=mb, source="apr"
        )

        run_query("""
            MERGE (c:Company {maticni_broj: $mb})
            SET c.pib               = $pib,
                c.name              = $name,
                c.name_normalized   = $name_norm,
                c.status            = $status,
                c.activity_code     = $act_code,
                c.activity_name     = $act_name,
                c.founding_date     = $founding,
                c.source            = 'apr',
                c.updated_at        = $now
        """, {
            "mb": mb,
            "pib": record.get("pib", ""),
            "name": record.get("name", ""),
            "name_norm": record.get("name_normalized") or normalize_company_name(record.get("name", "")),
            "status": record.get("status", ""),
            "act_code": record.get("activity_code", ""),
            "act_name": record.get("activity_name", ""),
            "founding": record.get("founding_date", ""),
            "now": datetime.utcnow().isoformat(),
        })

        # Load address
        city = record.get("address_city", "")
        street = record.get("address_street", "")
        if city or street:
            address_id = f"ADDR-{mb}"
            run_query("""
                MERGE (a:Address {address_id: $aid})
                SET a.street = $street, a.city = $city, a.full_address = $full
                WITH a
                MATCH (c:Company {maticni_broj: $mb})
                MERGE (c)-[:REGISTERED_AT]->(a)
            """, {
                "aid": address_id,
                "street": street,
                "city": city,
                "full": f"{street}, {city}".strip(", "),
                "mb": mb,
            })

        # Load founders
        for founder in record.get("founders", []):
            self._load_person_company_rel(founder, mb, "OWNS")

        # Load directors
        for director in record.get("directors", []):
            self._load_person_company_rel(director, mb, "DIRECTS")

    def _load_person_company_rel(self, person: dict, company_mb: str, rel_type: str):
        """Create or merge a Person node and its relationship to a Company."""
        name = person.get("name", "")
        if not name:
            return

        resolved = self.resolver.resolve_person(name, source="apr")
        pid = resolved.canonical_id

        run_query(f"""
            MERGE (p:Person {{person_id: $pid}})
            SET p.full_name        = $name,
                p.name_normalized  = $name_norm,
                p.source           = 'apr'
            WITH p
            MATCH (c:Company {{maticni_broj: $mb}})
            MERGE (p)-[r:{rel_type}]->(c)
            SET r.role = $role, r.ownership_pct = $pct
        """, {
            "pid": pid,
            "name": name,
            "name_norm": person.get("name_normalized", ""),
            "mb": company_mb,
            "role": person.get("role", ""),
            "pct": person.get("ownership_pct"),
        })

    def _load_contract(self, record: dict):
        """Merge a procurement contract record into Neo4j."""
        contract_id = record.get("contract_id", "")
        if not contract_id:
            return

        run_query("""
            MERGE (ct:Contract {contract_id: $cid})
            SET ct.title            = $title,
                ct.value_rsd        = $value,
                ct.award_date       = $date,
                ct.procurement_type = $proc_type,
                ct.num_bidders      = $bidders,
                ct.status           = $status,
                ct.source_url       = $url,
                ct.source           = 'procurement',
                ct.updated_at       = $now
        """, {
            "cid": contract_id,
            "title": record.get("title", ""),
            "value": record.get("value_rsd"),
            "date": record.get("award_date", ""),
            "proc_type": record.get("procurement_type", ""),
            "bidders": record.get("num_bidders"),
            "status": record.get("status", ""),
            "url": record.get("source_url", ""),
            "now": datetime.utcnow().isoformat(),
        })

        # Link to institution
        inst_name = record.get("awarding_institution", "")
        if inst_name:
            inst_id = record.get("awarding_institution_id") or \
                      f"INST-{abs(hash(inst_name)) % 10**8}"
            run_query("""
                MERGE (i:Institution {institution_id: $iid})
                SET i.name = $name
                WITH i
                MATCH (ct:Contract {contract_id: $cid})
                MERGE (i)-[:AWARDED_CONTRACT]->(ct)
            """, {"iid": inst_id, "name": inst_name, "cid": contract_id})

        # Link to winning company
        mb = record.get("winning_company_mb", "")
        company_name = record.get("winning_company", "")
        if mb or company_name:
            resolved = self.resolver.resolve_company(company_name, mb=mb, source="procurement")
            if mb:
                run_query("""
                    MERGE (c:Company {maticni_broj: $mb})
                    SET c.name = coalesce(c.name, $name)
                    WITH c
                    MATCH (ct:Contract {contract_id: $cid})
                    MERGE (c)-[:WON_CONTRACT]->(ct)
                """, {"mb": mb, "name": company_name, "cid": contract_id})
