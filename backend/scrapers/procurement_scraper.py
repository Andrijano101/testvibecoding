"""
Procurement portal scraper stub.
Target: https://jnportal.ujn.gov.rs

Scrapes recent contract awards from the Serbian Public Procurement Portal.
This is a working stub — HTML selectors need tuning once the live site is tested.
"""
import os
import re
import time
import json
from datetime import datetime, timedelta
from typing import Optional
from dataclasses import dataclass, asdict, field

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential
import structlog

logger = structlog.get_logger()

PORTAL_BASE = "https://jnportal.ujn.gov.rs"
DELAY = float(os.getenv("SCRAPE_DELAY", "2"))
DATA_DIR = os.getenv("DATA_DIR", "./data")
USER_AGENT = os.getenv("USER_AGENT", "SrpskaTransparentnost/1.0")


@dataclass
class ContractRecord:
    contract_id: str
    title: str = ""
    value_rsd: Optional[float] = None
    award_date: Optional[str] = None
    procurement_type: str = ""
    num_bidders: Optional[int] = None
    status: str = ""
    awarding_institution: str = ""
    awarding_institution_id: str = ""
    winning_company: str = ""
    winning_company_mb: str = ""
    source_url: str = ""
    scraped_at: str = ""


class ProcurementScraper:
    """Scraper for the Serbian Public Procurement Portal."""

    def __init__(self):
        self.client = httpx.Client(
            headers={"User-Agent": USER_AGENT},
            timeout=30.0,
            follow_redirects=True,
        )
        self.output_dir = os.path.join(DATA_DIR, "raw", "procurement")
        os.makedirs(self.output_dir, exist_ok=True)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def _fetch(self, url: str, **kwargs) -> httpx.Response:
        resp = self.client.get(url, **kwargs)
        resp.raise_for_status()
        return resp

    def scrape_recent(self, days: int = 7) -> list[ContractRecord]:
        """Scrape contracts awarded in the last N days."""
        logger.info("procurement_scrape_start", days=days)
        since = (datetime.utcnow() - timedelta(days=days)).strftime("%d.%m.%Y")
        results = []

        try:
            resp = self._fetch(
                f"{PORTAL_BASE}/pozivi/lista",
                params={"datumObjaveOd": since, "vrstaObavestenja": "24"},
            )
            soup = BeautifulSoup(resp.text, "lxml")

            rows = soup.select("table.procurement-table tbody tr, .result-row")
            for row in rows:
                record = self._parse_row(row)
                if record:
                    results.append(record)
                    self._save(record)
                time.sleep(DELAY)

        except Exception as e:
            logger.error("procurement_scrape_failed", error=str(e))

        logger.info("procurement_scrape_done", count=len(results))
        return results

    def _parse_row(self, row) -> Optional[ContractRecord]:
        """Parse a table row into a ContractRecord."""
        try:
            cells = row.find_all("td")
            if len(cells) < 4:
                return None

            link_el = cells[0].find("a", href=True)
            raw_id = link_el["href"].split("/")[-1] if link_el else ""
            if not raw_id:
                return None

            record = ContractRecord(
                contract_id=f"JN-{raw_id}",
                title=cells[0].get_text(strip=True),
                awarding_institution=cells[1].get_text(strip=True) if len(cells) > 1 else "",
                award_date=cells[2].get_text(strip=True) if len(cells) > 2 else "",
                scraped_at=datetime.utcnow().isoformat(),
                source_url=f"{PORTAL_BASE}{link_el['href']}" if link_el else "",
            )

            # Try to parse value
            value_text = cells[3].get_text(strip=True) if len(cells) > 3 else ""
            match = re.search(r"[\d.,]+", value_text.replace(".", "").replace(",", "."))
            if match:
                record.value_rsd = float(match.group().replace(",", "."))

            return record
        except Exception as e:
            logger.debug("row_parse_failed", error=str(e))
            return None

    def _save(self, record: ContractRecord):
        path = os.path.join(self.output_dir, f"{record.contract_id}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(asdict(record), f, ensure_ascii=False, indent=2)

    def close(self):
        self.client.close()
