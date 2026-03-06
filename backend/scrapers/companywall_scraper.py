"""
CompanyWall Scraper — APR + RGZ data via companywall.rs

companywall.rs aggregates publicly available APR (company registry) and
RGZ (cadastral/property) data. It is publicly accessible without authentication.

For each company (identified by PIB from jnportal contracts):
  1. Search by PIB → get company page URL
  2. Scrape: MB, full name, founding date, activity code, authorized reps (directors)
  3. Scrape: cadastral property parcels (from RGZ integration)

Outputs:
  - data/raw/apr/{pib}.json       — company + director records (source='apr')
  - data/raw/rgz/cw_{pib}.json   — property records per company (source='rgz')
"""

import os
import re
import json
import time
import unicodedata
from datetime import datetime
from typing import Dict, List, Optional

import httpx
from bs4 import BeautifulSoup
import structlog

logger = structlog.get_logger()

DATA_DIR = os.getenv("DATA_DIR", "./data")
SCRAPE_DELAY = float(os.getenv("SCRAPE_DELAY", "1.5"))
CW_MAX_COMPANIES = int(os.getenv("CW_MAX_COMPANIES", "300"))
CW_BASE = "https://www.companywall.rs"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "sr,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Referer": CW_BASE,
}


def _norm(s: str) -> str:
    if not s:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower().strip()


def _ensure_dirs():
    os.makedirs(os.path.join(DATA_DIR, "raw", "apr"), exist_ok=True)
    os.makedirs(os.path.join(DATA_DIR, "raw", "rgz"), exist_ok=True)


class CompanyWallScraper:
    """
    Scrapes company (APR) and property (RGZ) data from companywall.rs.
    Uses publicly available data without authentication.
    """

    def __init__(self):
        _ensure_dirs()
        self.client = httpx.Client(
            timeout=20,
            follow_redirects=True,
            headers=_HEADERS,
        )

    def _search(self, pib: str) -> Optional[str]:
        """Search for a company by PIB. Returns the /firma/ URL or None."""
        try:
            r = self.client.get(f"{CW_BASE}/pretraga", params={"n": pib})
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
            links = [
                a["href"] for a in soup.find_all("a", href=True)
                if "/firma/" in a.get("href", "")
            ]
            if not links:
                return None
            url = links[0]
            return url if url.startswith("http") else CW_BASE + url
        except Exception as e:
            logger.warning("cw_search_failed", pib=pib, error=str(e))
            return None

    def _scrape_company_page(self, url: str, pib: str) -> Optional[Dict]:
        """Scrape a company page and return structured data."""
        try:
            r = self.client.get(url)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")

            company: Dict = {
                "pib": pib,
                "source_url": url,
                "scraped_at": datetime.utcnow().isoformat(),
            }

            # Parse DL sections — company info + directors
            for dl in soup.find_all("dl"):
                dts = dl.find_all("dt")
                dds = dl.find_all("dd")
                for dt, dd in zip(dts, dds):
                    key = dt.get_text(strip=True)
                    val = dd.get_text(strip=True)
                    if not val:
                        continue
                    if key == "MB":
                        company["maticni_broj"] = val
                    elif key == "Naziv":
                        company["name"] = val[:200]
                    elif key == "Datum osnivanja":
                        company["founding_date"] = val
                    elif key == "Delatnost":
                        parts = val.split("-", 1)
                        company["activity_code"] = parts[0].strip()
                        company["activity_name"] = parts[1].strip()[:100] if len(parts) > 1 else ""
                    elif key == "Veličina preduzeća":
                        company["size"] = val
                    elif key == "Zastupnik" and val:
                        if "directors" not in company:
                            company["directors"] = []
                        name_part, _, role = val.partition(",")
                        role = role.strip().lower() or "zastupnik"
                        rel_type = "director" if any(
                            r in role for r in ("direktor", "director")
                        ) else "authorized_rep"
                        company["directors"].append({
                            "person_name": name_part.strip(),
                            "company_pib": pib,
                            "role": rel_type,
                            "role_raw": val,
                        })

            # Normalize name
            if "name" in company:
                company["name_normalized"] = _norm(company["name"])

            # Property/cadastral data
            properties = []
            for table in soup.find_all("table"):
                first_row = table.find("tr")
                if not first_row:
                    continue
                header_text = first_row.get_text()
                if "Katast" not in header_text and "parcele" not in header_text.lower():
                    continue
                for row in table.find_all("tr")[1:]:
                    cells = [td.get_text(strip=True) for td in row.find_all("td")]
                    if not cells or not any(cells):
                        continue
                    mun = re.sub(r"^Katastarska\s+opština\s*", "", cells[0]).strip()
                    parcel = cells[1].strip() if len(cells) > 1 else ""
                    area = cells[2].strip() if len(cells) > 2 else ""
                    if mun or parcel:
                        prop_id = f"PROP-CW-{abs(hash(pib + mun + parcel)) % 10**10}"
                        properties.append({
                            "property_id": prop_id,
                            "cadastral_id": f"{mun}/{parcel}" if parcel else mun,
                            "municipality": mun,
                            "parcel_number": parcel,
                            "area_sqm": self._parse_area(area),
                            "owner_name": company.get("name", ""),
                            "owner_type": "company",
                            "owner_mb": company.get("maticni_broj", pib),
                            "source": "rgz",
                            "source_url": url,
                            "scraped_at": datetime.utcnow().isoformat(),
                        })

            if properties:
                company["properties"] = properties

            return company if "maticni_broj" in company or "name" in company else None

        except Exception as e:
            logger.warning("cw_scrape_failed", url=url, error=str(e))
            return None

    def _parse_area(self, text: str) -> Optional[float]:
        if not text:
            return None
        m = re.search(r"([\d.,]+)", text)
        if m:
            try:
                return float(m.group(1).replace(".", "").replace(",", "."))
            except ValueError:
                pass
        return None

    def _get_top_company_pibs(self, top_n: int) -> List[str]:
        """Load top N company PIBs from jnportal contracts, ranked by total value."""
        path = os.path.join(DATA_DIR, "raw", "ujn", "jnportal_contracts.json")
        if not os.path.exists(path):
            logger.warning("cw_no_contracts", path=path)
            return []

        with open(path, encoding="utf-8") as f:
            contracts = json.load(f)

        totals: Dict[str, float] = {}
        for c in contracts:
            pib = (c.get("supplier_pib") or "").strip()
            val = float(c.get("contract_value") or 0)
            if pib and re.match(r"^\d{9}$", pib):
                totals[pib] = totals.get(pib, 0) + val

        ranked = sorted(totals.items(), key=lambda x: x[1], reverse=True)
        return [pib for pib, _ in ranked[:top_n]]

    def scrape(
        self,
        max_companies: int = CW_MAX_COMPANIES,
        force_refresh: bool = False,
    ) -> Dict:
        """
        Scrape company data from companywall.rs for top jnportal companies.
        Saves APR JSON files to data/raw/apr/{pib}.json
        Saves RGZ JSON files to data/raw/rgz/cw_{pib}.json
        """
        pibs = self._get_top_company_pibs(max_companies)
        logger.info("cw_scrape_start", companies=len(pibs))

        apr_saved = 0
        rgz_saved = 0
        skipped = 0

        for i, pib in enumerate(pibs):
            apr_path = os.path.join(DATA_DIR, "raw", "apr", f"{pib}.json")

            # Skip if fresh cache exists
            if not force_refresh and os.path.exists(apr_path):
                age_h = (time.time() - os.path.getmtime(apr_path)) / 3600
                if age_h < 48:
                    skipped += 1
                    continue

            logger.info("cw_scrape_company", pib=pib, i=i + 1, total=len(pibs))

            firma_url = self._search(pib)
            if not firma_url:
                logger.info("cw_not_found", pib=pib)
                time.sleep(SCRAPE_DELAY * 0.5)
                continue

            time.sleep(SCRAPE_DELAY * 0.5)

            company = self._scrape_company_page(firma_url, pib)
            if not company:
                time.sleep(SCRAPE_DELAY)
                continue

            # Save APR company record
            apr_record = {k: v for k, v in company.items() if k != "properties"}
            apr_record["source"] = "apr"
            if "maticni_broj" not in apr_record:
                apr_record["maticni_broj"] = pib

            with open(apr_path, "w", encoding="utf-8") as f:
                json.dump(apr_record, f, ensure_ascii=False, indent=2)
            apr_saved += 1

            # Save RGZ property records
            props = company.get("properties", [])
            if props:
                rgz_path = os.path.join(DATA_DIR, "raw", "rgz", f"cw_{pib}.json")
                with open(rgz_path, "w", encoding="utf-8") as f:
                    json.dump(props, f, ensure_ascii=False, indent=2)
                rgz_saved += len(props)

            time.sleep(SCRAPE_DELAY)

            if (i + 1) % 20 == 0:
                logger.info("cw_progress", done=i + 1, total=len(pibs), apr=apr_saved, rgz=rgz_saved)

        summary = {
            "total_companies": len(pibs),
            "apr_saved": apr_saved,
            "rgz_properties": rgz_saved,
            "skipped_cached": skipped,
            "scraped_at": datetime.utcnow().isoformat(),
        }
        logger.info("cw_scrape_done", **summary)
        return summary
