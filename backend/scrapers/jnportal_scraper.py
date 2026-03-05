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
JNPORTAL_MAX_ROWS = int(os.getenv("JNPORTAL_MAX_ROWS", "2000"))
# Only fetch contracts above this value (RSD) to focus on significant ones
JNPORTAL_MIN_VALUE = float(os.getenv("JNPORTAL_MIN_VALUE", "5000000"))
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

    def _fetch_page(self, skip: int, take: int, sort_desc: bool = True) -> Dict[str, Any]:
        """Fetch one page of contracts sorted by value descending."""
        token = self._get_token()
        sort_param = json.dumps([{"selector": "TotalValue", "desc": sort_desc}])
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

    def scrape(
        self,
        max_rows: int = JNPORTAL_MAX_ROWS,
        min_value: float = JNPORTAL_MIN_VALUE,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        """
        Fetch up to max_rows contracts sorted by value descending.
        Stops early if contract value drops below min_value.
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

        contracts: List[Dict] = []
        institutions_map: Dict[str, Dict] = {}
        skip = 0
        page_size = min(JNPORTAL_PAGE_SIZE, max_rows)
        total_available = None
        stopped_early = False

        while len(contracts) < max_rows:
            try:
                page = self._fetch_page(skip, page_size)
            except Exception as e:
                logger.error("jnportal_page_failed", skip=skip, error=str(e))
                break

            if total_available is None:
                total_available = page.get("totalCount", 0)
                logger.info("jnportal_total_available", total=total_available)

            rows = page.get("data", [])
            if not rows:
                break

            for raw in rows:
                value = raw.get("TotalValue") or 0
                try:
                    value = float(value)
                except (TypeError, ValueError):
                    value = 0.0

                if min_value > 0 and value < min_value:
                    stopped_early = True
                    break

                rec = self._parse_contract(raw)
                if rec:
                    contracts.append(rec)

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

        # Save outputs
        out_contracts = os.path.join(DATA_DIR, "raw", "ujn", "jnportal_contracts.json")
        out_institutions = os.path.join(DATA_DIR, "raw", "institutions", "jnportal_institutions.json")
        with open(out_contracts, "w", encoding="utf-8") as f:
            json.dump(contracts, f, ensure_ascii=False, indent=2)
        with open(out_institutions, "w", encoding="utf-8") as f:
            json.dump(list(institutions_map.values()), f, ensure_ascii=False, indent=2)

        # Cache summary
        summary = {
            "total_contracts": len(contracts),
            "total_institutions": len(institutions_map),
            "stopped_early_below_min_value": stopped_early,
            "min_value_rsd": min_value,
            "max_rows": max_rows,
            "scraped_at": datetime.utcnow().isoformat(),
        }
        with open(cache_path, "w") as f:
            json.dump({"summary": summary, "contracts": contracts[:10]}, f)

        logger.info("jnportal_done",
                    contracts=len(contracts),
                    institutions=len(institutions_map),
                    stopped_early=stopped_early)
        return summary
