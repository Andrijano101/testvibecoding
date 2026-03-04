"""
data.gov.rs Open Data Scraper
Uses the udata REST API at https://data.gov.rs/api/1/

NOTE: data.gov.rs runs udata, NOT CKAN.
Correct API base: /api/1/  (not /api/3/action/)

Confirmed working endpoints:
  GET /api/1/datasets/?tag=budzet&page_size=50
  GET /api/1/datasets/politichke-stranke/
  Direct file downloads: https://data.gov.rs/s/resources/...
"""
import os
import json
import io
import time
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Optional

import httpx
import structlog

logger = structlog.get_logger()

UDATA_API = "https://data.gov.rs/api/1"
DELAY = float(os.getenv("SCRAPE_DELAY", "2"))
DATA_DIR = os.getenv("DATA_DIR", "./data")

# Confirmed slugs and file URLs from API inspection
KNOWN_DATASETS = {
    "political_parties": {
        "slug": "politichke-stranke",
        "file_url": "https://data.gov.rs/s/resources/politichke-stranke/20240202-134344/politicke-stranke.xlsx",
        "format": "xlsx",
        "description": "Registar političkih stranaka u Srbiji (MUP)",
    },
    "procurement_notices": {
        "slug": "podatsi-iz-oglasa-javnikh-nabavki-sa-portala-javnikh-nabavki",
        "file_url": "http://portal.ujn.gov.rs/OpenD/OpenData_2020.xlsx",
        "format": "xlsx",
        "description": "Podaci iz oglasa javnih nabavki (UJN 2020)",
    },
}


@dataclass
class PoliticalPartyRecord:
    party_id: str
    name: str
    name_normalized: str
    abbreviation: Optional[str]
    founded: Optional[str]
    address: Optional[str]
    city: Optional[str]
    leader: Optional[str]
    dissolved: Optional[str]
    source: str = "data.gov.rs"
    source_url: str = "https://data.gov.rs/sr/datasets/politichke-stranke/"
    scraped_at: str = ""

    def __post_init__(self):
        if not self.scraped_at:
            self.scraped_at = datetime.utcnow().isoformat()


def _normalize(text: str) -> str:
    import unicodedata
    if not text:
        return ""
    cyrillic_map = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e',
        'ж': 'z', 'з': 'z', 'и': 'i', 'ј': 'j', 'к': 'k', 'л': 'l',
        'љ': 'lj', 'м': 'm', 'н': 'n', 'њ': 'nj', 'о': 'o', 'п': 'p',
        'р': 'r', 'с': 's', 'т': 't', 'ћ': 'c', 'ч': 'c', 'у': 'u',
        'ф': 'f', 'х': 'h', 'ц': 'c', 'џ': 'dz', 'ш': 's', 'ђ': 'dj',
        'А': 'a', 'Б': 'b', 'В': 'v', 'Г': 'g', 'Д': 'd', 'Е': 'e',
        'Ж': 'z', 'З': 'z', 'И': 'i', 'Ј': 'j', 'К': 'k', 'Л': 'l',
        'Љ': 'lj', 'М': 'm', 'Н': 'n', 'Њ': 'nj', 'О': 'o', 'П': 'p',
        'Р': 'r', 'С': 's', 'Т': 't', 'Ћ': 'c', 'Ч': 'c', 'У': 'u',
        'Ф': 'f', 'Х': 'h', 'Ц': 'c', 'Џ': 'dz', 'Ш': 's', 'Ђ': 'dj',
    }
    result = ""
    for ch in text:
        result += cyrillic_map.get(ch, ch)
    result = unicodedata.normalize('NFKD', result)
    result = ''.join(c for c in result if not unicodedata.combining(c))
    return result.lower().strip()


def _slug(text: str) -> str:
    import re
    n = _normalize(text)
    n = re.sub(r'[^a-z0-9]+', '-', n)
    return n.strip('-')[:60]


class OpenDataScraper:
    """Scrapes data.gov.rs using the udata REST API (/api/1/)."""

    def __init__(self):
        self.client = httpx.Client(
            timeout=120.0,
            follow_redirects=True,
            headers={"User-Agent": "SrpskaTransparentnost/1.0 (+https://transparentnost.rs)"},
        )
        os.makedirs(f"{DATA_DIR}/raw/opendata", exist_ok=True)

    def _get_dataset_info(self, slug: str) -> Optional[dict]:
        """Fetch dataset metadata from udata API."""
        try:
            resp = self.client.get(f"{UDATA_API}/datasets/{slug}/")
            if resp.status_code == 200:
                return resp.json()
            logger.warning("udata_dataset_not_found", slug=slug, status=resp.status_code)
            return None
        except Exception as e:
            logger.error("udata_api_error", error=str(e))
            return None

    def _download_file(self, url: str, local_name: str) -> Optional[bytes]:
        """Download a file with caching."""
        cache_path = f"{DATA_DIR}/raw/opendata/{local_name}"
        if os.path.exists(cache_path) and os.path.getsize(cache_path) > 1000:
            logger.info("opendata_cache_hit", file=local_name)
            with open(cache_path, "rb") as f:
                return f.read()
        logger.info("opendata_download", url=url)
        try:
            resp = self.client.get(url)
            if resp.status_code != 200:
                logger.error("opendata_download_failed", url=url, status=resp.status_code)
                return None
            data = resp.content
            with open(cache_path, "wb") as f:
                f.write(data)
            logger.info("opendata_download_done", file=local_name, size=len(data))
            return data
        except Exception as e:
            logger.error("opendata_download_error", error=str(e))
            return None

    def fetch_political_parties(self) -> list[PoliticalPartyRecord]:
        """
        Download political parties register from data.gov.rs.
        Source: MUP register of political parties, updated 2024-02-02.
        URL: https://data.gov.rs/s/resources/politichke-stranke/20240202-134344/politicke-stranke.xlsx
        """
        ds = KNOWN_DATASETS["political_parties"]
        data = self._download_file(ds["file_url"], "politicke_stranke.xlsx")

        if not data:
            logger.warning("parties_download_failed_using_fallback")
            return self._fallback_parties()

        try:
            import openpyxl
            wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True)
            ws = wb.active
            headers = [str(c.value) if c.value else "" for c in next(ws.iter_rows(max_row=1))]
            logger.info("parties_xlsx_headers", headers=headers)

            parties = []
            for i, row in enumerate(ws.iter_rows(min_row=2, values_only=True)):
                rowdict = dict(zip(headers, row))
                name = str(rowdict.get("Странка") or rowdict.get("Stranka") or "").strip()
                if not name or name == "None":
                    continue

                abbr = str(rowdict.get("Скраћени назив") or rowdict.get("Skraceni naziv") or "").strip()
                abbr = abbr if abbr and abbr != "None" else None

                party = PoliticalPartyRecord(
                    party_id=f"PARTY-DGR-{i+1:04d}",
                    name=name,
                    name_normalized=_normalize(name),
                    abbreviation=abbr,
                    founded=str(rowdict.get("Датум оснивања") or rowdict.get("Datum osnivanja") or "").strip() or None,
                    address=str(rowdict.get("Адреса") or rowdict.get("Adresa") or "").strip() or None,
                    city=str(rowdict.get("Место") or rowdict.get("Mesto") or "").strip() or None,
                    leader=str(rowdict.get("Заступник") or rowdict.get("Zastupnik") or "").strip() or None,
                    dissolved=str(rowdict.get("Датум доношења решења о брисању") or "").strip() or None,
                )
                # Skip parties that have been dissolved (non-empty dissolution date)
                if party.dissolved and party.dissolved != "None":
                    continue
                parties.append(party)

            wb.close()
            logger.info("parties_parsed", count=len(parties))

            # Save
            path = f"{DATA_DIR}/raw/opendata/parties.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump([asdict(p) for p in parties], f, ensure_ascii=False, indent=2)

            return parties

        except ImportError:
            logger.error("openpyxl_not_installed")
            return self._fallback_parties()
        except Exception as e:
            logger.error("parties_parse_error", error=str(e))
            return self._fallback_parties()

    def _fallback_parties(self) -> list[PoliticalPartyRecord]:
        """Major active Serbian political parties as fallback."""
        parties_data = [
            ("Srpska napredna stranka", "SNS", "2008", "Beograd", "Aleksandar Vučić"),
            ("Socijalistička partija Srbije", "SPS", "1990", "Beograd", "Ivica Dačić"),
            ("Stranka slobode i pravde", "SSP", "2019", "Beograd", "Dragan Đilas"),
            ("Demokratska stranka", "DS", "1990", "Beograd", "Zoran Lutovac"),
            ("Narodna stranka", "NS", "2014", "Beograd", "Vuk Jeremić"),
            ("Pokret obnove Kraljevine Srbije", "POKS", "1994", "Beograd", "Bogoljub Karić"),
            ("Zeleno-levi front", "ZLF", "2021", "Beograd", "Nebojša Zelić"),
            ("Srbija centar", "SCA", "2022", "Beograd", "Zdravko Ponoš"),
        ]
        result = []
        for i, (name, abbr, founded, city, leader) in enumerate(parties_data):
            result.append(PoliticalPartyRecord(
                party_id=f"PARTY-DGR-{i+1:04d}",
                name=name,
                name_normalized=_normalize(name),
                abbreviation=abbr,
                founded=founded,
                address=None,
                city=city,
                leader=leader,
                dissolved=None,
            ))
        return result

    def discover_datasets(self, tag: str = "budzet", page_size: int = 20) -> list[dict]:
        """
        Browse data.gov.rs udata API by tag.
        Returns list of dataset metadata dicts with resource URLs.
        """
        try:
            resp = self.client.get(
                f"{UDATA_API}/datasets/",
                params={"tag": tag, "page_size": page_size, "sort": "-created"},
            )
            if resp.status_code != 200:
                return []
            data = resp.json()
            datasets = []
            for ds in data.get("data", []):
                for resource in ds.get("resources", []):
                    fmt = resource.get("format", "").lower()
                    if fmt in ("csv", "xlsx", "xls", "json"):
                        datasets.append({
                            "slug": ds.get("slug"),
                            "title": ds.get("title"),
                            "format": fmt,
                            "url": resource.get("url"),
                            "size": resource.get("filesize"),
                        })
            return datasets
        except Exception as e:
            logger.error("udata_discover_error", error=str(e))
            return []

    def close(self):
        self.client.close()
