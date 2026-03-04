"""
APR Scraper - Agencija za privredne registre
Scrapes company registration data from the Serbian Business Registers Agency.

Target: https://pretraga.apr.gov.rs

Fixed in v2:
- Corrected double-escaped regex patterns (\\\\s -> \\s)
- Added retry logic with tenacity
- Added proper session management
- Improved error handling per field extraction
"""
import os
import re
import time
import json
import hashlib
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, asdict, field

import httpx
from bs4 import BeautifulSoup
from unidecode import unidecode
from tenacity import retry, stop_after_attempt, wait_exponential
import structlog

logger = structlog.get_logger()

APR_BASE = "https://pretraga.apr.gov.rs"
DELAY = float(os.getenv("SCRAPE_DELAY", "2"))
DATA_DIR = os.getenv("DATA_DIR", "./data")
USER_AGENT = os.getenv("USER_AGENT", "SrpskaTransparentnost/1.0")


@dataclass
class CompanyRecord:
    maticni_broj: str
    pib: str = ""
    name: str = ""
    name_normalized: str = ""
    status: str = ""
    activity_code: str = ""
    activity_name: str = ""
    founding_date: str = ""
    address_street: str = ""
    address_city: str = ""
    address_zip: str = ""
    email: str = ""
    founders: list = field(default_factory=list)
    directors: list = field(default_factory=list)
    scraped_at: str = ""
    source_url: str = ""


@dataclass
class PersonRecord:
    name: str
    name_normalized: str = ""
    role: str = ""  # founder, director, authorized_rep
    ownership_pct: Optional[float] = None
    jmbg_hash: str = ""  # SHA-256 of JMBG for privacy


def normalize_name(name: str) -> str:
    """Normalize Serbian names for matching (remove diacritics, lowercase)."""
    if not name:
        return ""
    cleaned = unidecode(name.strip().lower())
    cleaned = re.sub(r"\s+", " ", cleaned)  # Fixed: was \\\\s+
    return cleaned


def normalize_company_name(name: str) -> str:
    """Normalize company names: remove legal suffixes, lowercase."""
    if not name:
        return ""
    name = unidecode(name.strip().lower())
    # Fixed: regex patterns were double-escaped
    suffixes = [
        r"\bd\.?o\.?o\.?\b", r"\ba\.?d\.?\b", r"\bs\.?z\.?r\.?\b",
        r"\bpreduzetnik\b", r"\borganka\b", r"\bz\.?u\.?\b",
        r"\bpr\b", r"\bstr\b",
    ]
    for s in suffixes:
        name = re.sub(s, "", name)
    name = re.sub(r"[^a-z0-9\s]", "", name)  # Fixed: was \\\\s
    name = re.sub(r"\s+", " ", name).strip()  # Fixed: was \\\\s+
    return name


def hash_jmbg(jmbg: str) -> str:
    """Hash JMBG for privacy-preserving matching."""
    if not jmbg or len(jmbg) != 13:
        return ""
    return hashlib.sha256(jmbg.encode()).hexdigest()


class APRScraper:
    """Scraper for APR company registry."""

    def __init__(self):
        self.client = httpx.Client(
            headers={"User-Agent": USER_AGENT},
            timeout=30.0,
            follow_redirects=True,
        )
        self.output_dir = os.path.join(DATA_DIR, "raw", "apr")
        os.makedirs(self.output_dir, exist_ok=True)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def _fetch(self, url: str, **kwargs) -> httpx.Response:
        """Fetch a URL with automatic retry."""
        resp = self.client.get(url, **kwargs)
        resp.raise_for_status()
        return resp

    def search_company(self, query: str) -> list[dict]:
        """Search APR for companies by name or MB."""
        logger.info("apr_search", query=query)
        url = f"{APR_BASE}/unifiedsearch"
        results = []

        try:
            resp = self._fetch(url, params={
                "SearchTerm": query,
                "RegisterIndex": "APRRegisterIndex",
            })
            soup = BeautifulSoup(resp.text, "lxml")

            rows = soup.select("table.results-table tr") or soup.select(".search-result-item")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= 3:
                    link_el = cells[0].find("a", href=True)
                    results.append({
                        "name": cells[0].get_text(strip=True),
                        "maticni_broj": cells[1].get_text(strip=True),
                        "status": cells[2].get_text(strip=True) if len(cells) > 2 else "",
                        "link": link_el["href"] if link_el else "",
                    })

            time.sleep(DELAY)

        except Exception as e:
            logger.error("apr_search_failed", query=query, error=str(e))

        return results

    def scrape_company(self, maticni_broj: str) -> Optional[CompanyRecord]:
        """Scrape full company details from APR by maticni broj."""
        logger.info("apr_scrape_company", mb=maticni_broj)

        try:
            url = f"{APR_BASE}/unifiedsearch/details/{maticni_broj}"
            resp = self._fetch(url)
            soup = BeautifulSoup(resp.text, "lxml")

            record = CompanyRecord(
                maticni_broj=maticni_broj,
                scraped_at=datetime.utcnow().isoformat(),
                source_url=url,
            )

            # Field extraction map
            field_map = {
                "naziv": "name",
                "maticni broj": "maticni_broj",
                "pib": "pib",
                "status": "status",
                "sifra delatnosti": "activity_code",
                "delatnost": "activity_name",
                "datum osnivanja": "founding_date",
                "sediste": "address_street",
                "mesto": "address_city",
            }

            for row in soup.select(".detail-row, .field-row, tr"):
                label_el = row.select_one(".label, .field-label, td:first-child")
                value_el = row.select_one(".value, .field-value, td:last-child")
                if not label_el or not value_el:
                    continue
                label = label_el.get_text(strip=True).lower()
                value = value_el.get_text(strip=True)
                if not value:
                    continue
                for key, attr in field_map.items():
                    if key in label:
                        setattr(record, attr, value)
                        break

            record.name_normalized = normalize_company_name(record.name)

            # Extract founders/owners
            self._extract_persons(soup, record, "founders",
                                  regex=r"osnivac|clan|vlasnik", default_role="founder")

            # Extract directors
            self._extract_persons(soup, record, "directors",
                                  regex=r"direktor|zastupnik", default_role="director")

            # Save raw data
            outfile = os.path.join(self.output_dir, f"{maticni_broj}.json")
            with open(outfile, "w", encoding="utf-8") as f:
                json.dump(asdict(record), f, ensure_ascii=False, indent=2)

            time.sleep(DELAY)
            return record

        except Exception as e:
            logger.error("apr_scrape_failed", mb=maticni_broj, error=str(e))
            return None

    def _extract_persons(self, soup, record: CompanyRecord, target_field: str,
                         regex: str, default_role: str):
        """Extract person records from a section of the APR page."""
        section = soup.find(string=re.compile(regex, re.I))
        if not section:
            return

        parent = section.find_parent("div") or section.find_parent("table")
        if not parent:
            return

        for item in parent.select(".person-item, tr"):
            name_el = item.select_one(".person-name, td:first-child")
            if not name_el:
                continue

            name_text = name_el.get_text(strip=True)
            if not name_text or len(name_text) < 2:
                continue

            person = PersonRecord(name=name_text, role=default_role)
            person.name_normalized = normalize_name(person.name)

            if target_field == "founders":
                pct_el = item.select_one(".ownership-pct, td:nth-child(2)")
                if pct_el:
                    person.ownership_pct = self._parse_pct(pct_el.get_text(strip=True))
            elif target_field == "directors":
                role_el = item.select_one(".role, td:nth-child(2)")
                if role_el:
                    person.role = role_el.get_text(strip=True) or default_role

            getattr(record, target_field).append(asdict(person))

    def _parse_pct(self, text: str) -> Optional[float]:
        """Parse ownership percentage from text."""
        match = re.search(r"(\d+[.,]?\d*)\s*%", text)
        if match:
            return float(match.group(1).replace(",", "."))
        return None

    def scrape_batch(self, maticni_brojevi: list[str]) -> list[CompanyRecord]:
        """Scrape multiple companies with progress logging."""
        results = []
        total = len(maticni_brojevi)
        for i, mb in enumerate(maticni_brojevi):
            logger.info("batch_progress", current=i + 1, total=total, pct=round((i + 1) / total * 100, 1))
            record = self.scrape_company(mb)
            if record:
                results.append(record)
        logger.info("batch_complete", scraped=len(results), total=total)
        return results

    def close(self):
        self.client.close()


if __name__ == "__main__":
    scraper = APRScraper()
    results = scraper.search_company("telecom")
    for r in results[:5]:
        print(f"Found: {r['name']} (MB: {r['maticni_broj']})")
        if r["maticni_broj"]:
            company = scraper.scrape_company(r["maticni_broj"])
            if company:
                print(f"  Founders: {len(company.founders)}")
                print(f"  Directors: {len(company.directors)}")
    scraper.close()
