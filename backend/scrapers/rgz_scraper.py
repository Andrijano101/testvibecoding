"""
RGZ Scraper - Republički geodetski zavod
Scrapes property ownership records from the Real Estate Agency of Serbia.

Target: https://rgz.gov.rs/usluge/eLine
"""
import os
import re
import time
import json
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, asdict

import httpx
from bs4 import BeautifulSoup
from unidecode import unidecode
from tenacity import retry, stop_after_attempt, wait_exponential
import structlog

logger = structlog.get_logger()

RGZ_BASE = "https://rgz.gov.rs"
RGZ_ELINE = f"{RGZ_BASE}/usluge/eLine"
DELAY = float(os.getenv("SCRAPE_DELAY", "2"))
DATA_DIR = os.getenv("DATA_DIR", "./data")
USER_AGENT = os.getenv("USER_AGENT", "SrpskaTransparentnost/1.0")


def normalize_name(name: str) -> str:
    if not name:
        return ""
    return re.sub(r"\s+", " ", unidecode(name.strip().lower()))


@dataclass
class PropertyRecord:
    property_id: str
    cadastral_id: str = ""
    address: str = ""
    city: str = ""
    municipality: str = ""
    area_sqm: Optional[float] = None
    property_type: str = ""  # residential / commercial / land
    owner_name: str = ""
    owner_name_normalized: str = ""
    owner_type: str = ""  # person / company
    owner_mb: str = ""
    ownership_pct: float = 100.0
    acquisition_date: str = ""
    source_url: str = ""
    scraped_at: str = ""


class RGZScraper:
    """Scraper for RGZ property registry (eLine service)."""

    def __init__(self):
        self.client = httpx.Client(
            headers={"User-Agent": USER_AGENT},
            timeout=30.0,
            follow_redirects=True,
        )
        self.output_dir = os.path.join(DATA_DIR, "raw", "rgz")
        os.makedirs(self.output_dir, exist_ok=True)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def _fetch(self, url: str, **kwargs) -> httpx.Response:
        resp = self.client.get(url, **kwargs)
        resp.raise_for_status()
        return resp

    def search_by_owner(self, name: str) -> list[PropertyRecord]:
        """Search properties by owner name."""
        logger.info("rgz_search_owner", name=name)
        records = []

        try:
            resp = self._fetch(RGZ_ELINE, params={"vlasnik": name, "tip": "ime"})
            soup = BeautifulSoup(resp.text, "lxml")
            rows = soup.select("table.results tr, .property-item, .parcel-row")
            for row in rows:
                record = self._parse_property_row(row, owner_name=name)
                if record:
                    self._save(record)
                    records.append(record)
            time.sleep(DELAY)
        except Exception as e:
            logger.warning("rgz_search_owner_failed", name=name, error=str(e))

        return records

    def search_by_address(self, address: str) -> list[PropertyRecord]:
        """Search properties by address (cross-ref APR companies)."""
        logger.info("rgz_search_address", address=address)
        records = []

        try:
            resp = self._fetch(RGZ_ELINE, params={"adresa": address, "tip": "adresa"})
            soup = BeautifulSoup(resp.text, "lxml")
            rows = soup.select("table.results tr, .property-item, .parcel-row")
            for row in rows:
                record = self._parse_property_row(row)
                if record:
                    self._save(record)
                    records.append(record)
            time.sleep(DELAY)
        except Exception as e:
            logger.warning("rgz_search_address_failed", address=address, error=str(e))

        return records

    def _parse_property_row(self, row, owner_name: str = "") -> Optional[PropertyRecord]:
        """Parse a single property row from RGZ results."""
        cells = row.find_all("td")
        if len(cells) < 3:
            return None

        cadastral_id = cells[0].get_text(strip=True)
        if not cadastral_id or len(cadastral_id) < 2:
            return None

        address = cells[1].get_text(strip=True) if len(cells) > 1 else ""
        area_text = cells[2].get_text(strip=True) if len(cells) > 2 else ""
        owner_text = cells[3].get_text(strip=True) if len(cells) > 3 else owner_name

        # Parse area
        area_sqm = None
        area_match = re.search(r"([\d.,]+)\s*m²?", area_text)
        if area_match:
            area_sqm = float(area_match.group(1).replace(",", "."))

        # Determine property type
        property_type = "residential"
        if re.search(r"poslovn|kancelarij|magacin", address, re.I):
            property_type = "commercial"
        elif re.search(r"njiv|šum|livad|oranica|zemljišt", address, re.I):
            property_type = "land"

        # Determine owner type
        owner_type = "company" if re.search(r"\bd\.?o\.?o\b|\ba\.?d\b|\bpib\b", owner_text, re.I) else "person"

        property_id = f"PROP-{abs(hash(cadastral_id + address)) % 10**10}"

        # Extract city from address
        city = ""
        city_match = re.search(r",\s*([A-ZŠĐČĆŽ][a-zšđčćž\s]+)$", address)
        if city_match:
            city = city_match.group(1).strip()

        return PropertyRecord(
            property_id=property_id,
            cadastral_id=cadastral_id,
            address=address,
            city=city,
            area_sqm=area_sqm,
            property_type=property_type,
            owner_name=owner_text,
            owner_name_normalized=normalize_name(owner_text),
            owner_type=owner_type,
            source_url=RGZ_ELINE,
            scraped_at=datetime.utcnow().isoformat(),
        )

    def _save(self, record: PropertyRecord):
        path = os.path.join(self.output_dir, f"{record.property_id}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(asdict(record), f, ensure_ascii=False, indent=2)

    def close(self):
        self.client.close()
