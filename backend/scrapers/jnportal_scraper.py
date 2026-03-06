"""
JN Portal (jnportal.ujn.gov.rs) Contract Scraper

Fetches real contract data from the new Serbian public procurement portal.
The portal requires a session token extracted from the page HTML.

Key fields returned per contract:
  - Id: contract ID
  - CAName: contracting authority (institution) name
  - CAIdentificationNumber: institution PIB
  - TenderName: contract subject/title
  - ContractorName: winning company name
  - ContractorIdentificationNumber: contractor PIB
  - TotalValue: contract value (RSD, excl. VAT)
  - ContractDate: date signed

Outputs:
  - data/raw/ujn/jnportal_contracts.json
  - data/raw/institutions/jnportal_institutions.json
"""

import os
import re
import json
import time
import unicodedata
from datetime import datetime
from typing import Dict, List, Optional, Any

import httpx
import structlog

logger = structlog.get_logger()

DATA_DIR = os.getenv("DATA_DIR", "./data")
SCRAPE_DELAY = float(os.getenv("SCRAPE_DELAY", "1"))
SCRAPER_TIMEOUT = float(os.getenv("SCRAPER_TIMEOUT", "60"))
JNPORTAL_BASE = "https://jnportal.ujn.gov.rs"
JNPORTAL_MAX_ROWS = int(os.getenv("JNPORTAL_MAX_ROWS", "10000"))
# Lower threshold to catch contract-splitting and threshold-manipulation patterns
# TS Serbia research (2024): most manipulation happens in 850K-3M RSD range
JNPORTAL_MIN_VALUE = float(os.getenv("JNPORTAL_MIN_VALUE", "500000"))
JNPORTAL_PAGE_SIZE = int(os.getenv("JNPORTAL_PAGE_SIZE", "100"))


def _ensure_dirs():
    os.makedirs(os.path.join(DATA_DIR, "raw", "ujn"), exist_ok=True)
    os.makedirs(os.path.join(DATA_DIR, "raw", "institutions"), exist_ok=True)
    os.makedirs(os.path.join(DATA_DIR, "raw", "cache"), exist_ok=True)


def _norm(s: str) -> str:
    if not s:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower().strip()


class JNPortalScraper:
    def __init__(self):
        _ensure_dirs()
        self.client = httpx.Client(
            timeout=SCRAPER_TIMEOUT,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; SrpskaTransparentnost/1.0)",
                "Accept": "application/json",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": f"{JNPORTAL_BASE}/contracts",
            },
        )
        self._token: Optional[str] = None

    def _authenticate(self) -> str:
        """Get fresh session cookies and UserToken from the contracts page."""
        logger.info("jnportal_auth", url=f"{JNPORTAL_BASE}/contracts")
        r = self.client.get(f"{JNPORTAL_BASE}/contracts")
        r.raise_for_status()
        m = re.search(r'id="uiUserToken"\s+value="([^"]+)"', r.text)
        if not m:
            raise RuntimeError("Could not find UserToken in contracts page")
        token = m.group(1)
        logger.info("jnportal_token_ok", token=token[:8] + "...")
        return token

    def _get_token(self) -> str:
        if not self._token:
            self._token = self._authenticate()
        return self._token

    def _fetch_page(self, skip: int, take: int, sort_field: str = "TotalValue", sort_desc: bool = True) -> Dict[str, Any]:
        """Fetch one page of contracts with configurable sort field."""
        token = self._get_token()
        sort_param = json.dumps([{"selector": sort_field, "desc": sort_desc}])
        params = {
            "take": take,
            "skip": skip,
            "sort": sort_param,
        }
        url = f"{JNPORTAL_BASE}/api/searchgrid/VContractRegisterPublic/get"
        r = self.client.get(url, params=params, headers={"UserToken": token})
        if r.status_code == 401 or r.status_code == 403:
            # Token expired — re-authenticate
            logger.warning("jnportal_reauth", status=r.status_code)
            self._token = self._authenticate()
            r = self.client.get(url, params=params, headers={"UserToken": self._token})
        r.raise_for_status()
        return r.json()

    def _scrape_phase(
        self,
        sort_field: str,
        max_rows: int,
        min_value: float = 0,
        require_contractor: bool = False,
        seen_ids: Optional[set] = None,
    ) -> List[Dict]:
        """Fetch up to max_rows contracts with a given sort field.

        If require_contractor=True, skips records without a ContractorName.
        If min_value > 0, stops when TotalValue drops below threshold.
        seen_ids: skip contracts already collected in a previous phase.
        """
        if seen_ids is None:
            seen_ids = set()

        contracts: List[Dict] = []
        institutions_map: Dict[str, Dict] = {}
        skip = 0
        page_size = min(JNPORTAL_PAGE_SIZE, max_rows)
        total_available = None
        stopped_early = False

        while len(contracts) < max_rows:
            try:
                page = self._fetch_page(skip, page_size, sort_field=sort_field)
            except Exception as e:
                logger.error("jnportal_page_failed", skip=skip, sort=sort_field, error=str(e))
                break

            if total_available is None:
                total_available = page.get("totalCount", 0)
                logger.info("jnportal_phase_start", sort=sort_field, total=total_available, max_rows=max_rows)

            rows = page.get("data", [])
            if not rows:
                break

            for raw in rows:
                contract_id = f"JNP-{raw.get('Id', '')}"
                if contract_id in seen_ids:
                    continue

                value = raw.get("TotalValue") or 0
                try:
                    value = float(value)
                except (TypeError, ValueError):
                    value = 0.0

                if min_value > 0 and value < min_value:
                    stopped_early = True
                    break

                contractor_name = (raw.get("ContractorName") or "").strip()
                if require_contractor and not contractor_name:
                    continue

                rec = self._parse_contract(raw)
                if rec:
                    contracts.append(rec)
                    seen_ids.add(contract_id)

                # Build institution record
                inst_pib = re.sub(r"\D", "", str(raw.get("CAIdentificationNumber") or ""))
                inst_name = (raw.get("CAName") or "").strip()
                if inst_pib and inst_name and inst_pib not in institutions_map:
                    institutions_map[inst_pib] = {
                        "institution_id": f"JNP-INST-{inst_pib}",
                        "name": inst_name,
                        "name_normalized": _norm(inst_name),
                        "maticni_broj": "",
                        "pib": inst_pib,
                        "source_url": f"{JNPORTAL_BASE}/contracts",
                    }

            if stopped_early or len(rows) < page_size:
                break

            skip += page_size
            if skip >= (total_available or skip + 1):
                break

            time.sleep(SCRAPE_DELAY)

        logger.info("jnportal_phase_done",
                    sort=sort_field,
                    fetched=len(contracts),
                    stopped_early=stopped_early)
        return contracts, institutions_map

    def _parse_contract(self, raw: Dict) -> Optional[Dict[str, Any]]:
        """Convert raw API record to our schema."""
        value = raw.get("TotalValue") or 0
        try:
            value = float(value)
        except (TypeError, ValueError):
            value = 0.0

        contractor_name = (raw.get("ContractorName") or "").strip()
        contractor_pib = re.sub(r"\D", "", str(raw.get("ContractorIdentificationNumber") or ""))
        inst_name = (raw.get("CAName") or "").strip()
        inst_pib = re.sub(r"\D", "", str(raw.get("CAIdentificationNumber") or ""))
        title = (raw.get("TenderName") or "").strip()

        contract_date = raw.get("ContractDate") or ""
        if contract_date:
            # "2026-02-05T00:00:00" -> "2026-02-05"
            contract_date = contract_date[:10]

        return {
            "contract_id": f"JNP-{raw.get('Id', '')}",
            "title": title,
            "subject": title,
            "proc_type": str(raw.get("ProcedureTypeId", "")),
            "subject_type": str(raw.get("ContractTypeId", "")),
            "date_modified": contract_date,
            "has_award_decision": bool(contractor_name),
            "detail_url": f"{JNPORTAL_BASE}/contract-eo/{raw.get('Id', '')}",
            "institution_pib": inst_pib,
            "institution_name": inst_name,
            # Supplier info - the key missing piece in old data
            "supplier_name": contractor_name or None,
            "supplier_pib": contractor_pib or None,
            "supplier_mb": None,  # Not available directly; resolve via APR if needed
            "contract_value": value,
            "currency": "RSD",
            "num_bidders": None,
            "source": "jnportal",
        }

    def _scrape_phase_proc_type(
        self,
        proc_types: List[int],
        max_rows: int,
        seen_ids: Optional[set] = None,
    ) -> tuple:
        """Phase targeting specific procedure types (e.g. 3=negotiated, 9=direct agreement).

        These are the highest-risk non-competitive procedures. We scrape them separately
        sorted by TotalValue DESC to get the most significant ones.
        Since the API filter param doesn't work reliably, we scan pages and filter client-side.
        """
        if seen_ids is None:
            seen_ids = set()

        contracts: List[Dict] = []
        institutions_map: Dict[str, Dict] = {}
        skip = 0
        page_size = JNPORTAL_PAGE_SIZE
        total_scanned = 0
        MAX_SCAN = max_rows * 4  # scan up to 4x to find enough matching rows

        logger.info("jnportal_proc_phase_start", proc_types=proc_types, max_rows=max_rows)

        while len(contracts) < max_rows and total_scanned < MAX_SCAN:
            try:
                # Scan by date (most recent first) — non-competitive contracts
                # are distributed across all years, recent ones are most relevant
                page = self._fetch_page(skip, page_size, sort_field="ContractDate")
            except Exception as e:
                logger.error("jnportal_page_failed", skip=skip, error=str(e))
                break

            rows = page.get("data", [])
            if not rows:
                break

            for raw in rows:
                total_scanned += 1
                pt = raw.get("ProcedureTypeId")
                if pt not in proc_types:
                    continue

                contract_id = f"JNP-{raw.get('Id', '')}"
                if contract_id in seen_ids:
                    continue

                rec = self._parse_contract(raw)
                if rec:
                    contracts.append(rec)
                    seen_ids.add(contract_id)

                    inst_pib = re.sub(r"\D", "", str(raw.get("CAIdentificationNumber") or ""))
                    inst_name = (raw.get("CAName") or "").strip()
                    if inst_pib and inst_name and inst_pib not in institutions_map:
                        institutions_map[inst_pib] = {
                            "institution_id": f"JNP-INST-{inst_pib}",
                            "name": inst_name, "name_normalized": _norm(inst_name),
                            "maticni_broj": "", "pib": inst_pib,
                            "source_url": f"{JNPORTAL_BASE}/contracts",
                        }

                if len(contracts) >= max_rows:
                    break

            if len(rows) < page_size:
                break

            skip += page_size
            time.sleep(SCRAPE_DELAY * 0.5)

        logger.info("jnportal_proc_phase_done",
                    proc_types=proc_types, found=len(contracts), scanned=total_scanned)
        return contracts, institutions_map

    def scrape(
        self,
        max_rows: int = JNPORTAL_MAX_ROWS,
        min_value: float = JNPORTAL_MIN_VALUE,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        """
        Three-phase scrape strategy:
          Phase 1 — sorted by TotalValue DESC, stops at min_value (default 500K RSD).
                    Gets all high-value contracts.
          Phase 2 — sorted by ContractDate DESC, no value filter.
                    Gets the most recent contracts.
          Phase 3 — scan for proc_type 3 (negotiated) + 9 (direct agreement).
                    Targets non-competitive contracts regardless of value.
                    These are the highest-risk for corruption.
        """
        cache_path = os.path.join(DATA_DIR, "raw", "cache", "jnportal_contracts.json")
        if not force_refresh and os.path.exists(cache_path):
            age_hours = (time.time() - os.path.getmtime(cache_path)) / 3600
            if age_hours < 12:
                logger.info("jnportal_cache_hit", age_hours=round(age_hours, 1))
                with open(cache_path) as f:
                    cached = json.load(f)
                return cached.get("summary", {
                    "total_contracts": len(cached.get("contracts", [])),
                    "scraped_at": "cached",
                })

        seen_ids: set = set()

        # Phase 1: high-value contracts sorted by TotalValue DESC
        phase1_quota = max_rows // 2
        phase1_contracts, phase1_insts = self._scrape_phase(
            "TotalValue",
            max_rows=phase1_quota,
            min_value=min_value,
            require_contractor=False,
            seen_ids=seen_ids,
        )
        logger.info("jnportal_phase1_done", count=len(phase1_contracts))

        # Phase 2: recent contracts sorted by ContractDate DESC
        phase2_quota = max_rows // 4
        phase2_contracts, phase2_insts = self._scrape_phase(
            "ContractDate",
            max_rows=phase2_quota,
            min_value=0,
            require_contractor=True,
            seen_ids=seen_ids,
        )
        logger.info("jnportal_phase2_done", count=len(phase2_contracts))

        # Save partial results after Phase 2 so we don't lose data if Phase 3 is slow
        partial_contracts = phase1_contracts + phase2_contracts
        partial_insts = {**phase1_insts, **phase2_insts}
        out_contracts = os.path.join(DATA_DIR, "raw", "ujn", "jnportal_contracts.json")
        out_institutions = os.path.join(DATA_DIR, "raw", "institutions", "jnportal_institutions.json")
        with open(out_contracts, "w", encoding="utf-8") as f:
            json.dump(partial_contracts, f, ensure_ascii=False, indent=2)
        with open(out_institutions, "w", encoding="utf-8") as f:
            json.dump(list(partial_insts.values()), f, ensure_ascii=False, indent=2)
        logger.info("jnportal_partial_saved", count=len(partial_contracts))

        # Phase 3: non-competitive contracts (proc_type 3 and 9) — critical for corruption detection
        # Note: Phase 2 already captured recent contracts; Phase 3 looks further back for non-competitive ones.
        # seen_ids is NOT passed here so Phase 3 scans fresh from ContractDate DESC,
        # deduplication happens below.
        phase3_quota = max_rows // 4
        phase3_contracts, phase3_insts = self._scrape_phase_proc_type(
            proc_types=[3, 9],
            max_rows=phase3_quota,
            seen_ids=None,
        )
        logger.info("jnportal_phase3_done", count=len(phase3_contracts))

        # Deduplicate Phase 3 results against Phase 1+2
        phase3_new = [c for c in phase3_contracts if c["contract_id"] not in seen_ids]
        logger.info("jnportal_phase3_new", new=len(phase3_new), total=len(phase3_contracts))

        contracts = phase1_contracts + phase2_contracts + phase3_new
        institutions_map = {**phase1_insts, **phase2_insts, **phase3_insts}

        # Save final outputs (overwrite partial)
        out_contracts = os.path.join(DATA_DIR, "raw", "ujn", "jnportal_contracts.json")
        out_institutions = os.path.join(DATA_DIR, "raw", "institutions", "jnportal_institutions.json")
        with open(out_contracts, "w", encoding="utf-8") as f:
            json.dump(contracts, f, ensure_ascii=False, indent=2)
        with open(out_institutions, "w", encoding="utf-8") as f:
            json.dump(list(institutions_map.values()), f, ensure_ascii=False, indent=2)

        summary = {
            "total_contracts": len(contracts),
            "total_institutions": len(institutions_map),
            "phase1_by_value": len(phase1_contracts),
            "phase2_by_date": len(phase2_contracts),
            "phase3_noncompetitive": len(phase3_new),
            "min_value_rsd": min_value,
            "max_rows": max_rows,
            "scraped_at": datetime.utcnow().isoformat(),
        }
        with open(cache_path, "w") as f:
            json.dump({"summary": summary, "contracts": contracts[:10]}, f)

        logger.info("jnportal_done",
                    total=len(contracts),
                    phase1=len(phase1_contracts),
                    phase2=len(phase2_contracts),
                    phase3=len(phase3_new),
                    institutions=len(institutions_map))
        return summary
