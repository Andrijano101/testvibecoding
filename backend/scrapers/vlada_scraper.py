"""
Vlada Srbije (Serbian Government) Scraper

Fetches current cabinet members from Wikipedia (sr.wikipedia.org).
Wikipedia is used because:
  - The official government site (srbija.gov.rs) is a React SPA with no static API
  - Wikipedia is consistently up-to-date and machine-readable

For each cabinet member extracts:
  - Full name (Cyrillic)
  - Official role/title
  - Ministry/institution they lead
  - Party affiliation (if available)

Outputs:
  - data/raw/vlada/cabinet.json
"""

import os
import re
import json
import time
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field, asdict

import httpx
from bs4 import BeautifulSoup
from unidecode import unidecode
import structlog

logger = structlog.get_logger()

DATA_DIR = os.getenv("DATA_DIR", "./data")
DELAY = float(os.getenv("SCRAPE_DELAY", "1.0"))
USER_AGENT = "Mozilla/5.0 (compatible; SrpskaTransparentnost/1.0)"

WIKIPEDIA_URL = "https://sr.wikipedia.org/wiki/Влада_Србије"

# Map Wikipedia role text → canonical institution_id + institution name
MINISTRY_MAP = {
    "Председник Владе": ("INST-GOV-PREDSEDNIK", "Влада Републике Србије", "predsednik-vlade"),
    "Потпредседник Владе": ("INST-GOV-PREDSEDNIK", "Влада Републике Србије", "potpredsednik-vlade"),
    "Потпредседници Владе": ("INST-GOV-PREDSEDNIK", "Влада Републике Србије", "potpredsednik-vlade"),
    "Први потпредседник Владе": ("INST-GOV-PREDSEDNIK", "Влада Републике Србије", "potpredsednik-vlade"),
    "Министар спољних послова": ("INST-MFA", "Министарство спољних послова", "ministar"),
    "Министар унутрашњих послова": ("INST-MUP", "Министарство унутрашњих послова", "ministar"),
    "Министар одбране": ("INST-MO", "Министарство одбране", "ministar"),
    "Министар финансија": ("INST-MF", "Министарство финансија", "ministar"),
    "Министар привреде": ("INST-MPRIVREDA", "Министарство привреде", "ministar"),
    "Министарка привреде": ("INST-MPRIVREDA", "Министарство привреде", "ministarka"),
    "Министар просвете": ("INST-MPROSVETA", "Министарство просвете", "ministar"),
    "Министарство просвете": ("INST-MPROSVETA", "Министарство просвете", "ministar"),
    "Министар рударства и енергетике": ("INST-MRUDARSTVO", "Министарство рударства и енергетике", "ministar"),
    "Министарка рударства и енергетике": ("INST-MRUDARSTVO", "Министарство рударства и енергетике", "ministarka"),
    "Министар културе": ("INST-MKULTURA", "Министарство културе", "ministar"),
    "Министар пољопривреде": ("INST-MPOLJOPRIVREDA", "Министарство пољопривреде, шумарства и водопривреде", "ministar"),
    "Министарка трговине": ("INST-MTRGOVINE", "Министарство унутрашње и спољне трговине", "ministarka"),
    "Министарка заштите животне средине": ("INST-MZIVOTNA", "Министарство заштите животне средине", "ministarka"),
    "Министар здравља": ("INST-MZDRAVLJE", "Министарство здравља", "ministar"),
    "Министар за европске интеграције": ("INST-MEI", "Министарство за европске интеграције", "ministar"),
    "Министар правде": ("INST-MPRAVDE", "Министарство правде", "ministar"),
    "Министар за рад": ("INST-MRAD", "Министарство за рад, запошљавање, борачка и социјална питања", "ministar"),
    "Министарка за рад": ("INST-MRAD", "Министарство за рад, запошљавање, борачка и социјална питања", "ministarka"),
    "Министарка државне управе": ("INST-MDRZAVNA", "Министарство државне управе и локалне самоуправе", "ministarka"),
    "Министарка грађевинарства": ("INST-MGRADI", "Министарство грађевинарства, саобраћаја и инфраструктуре", "ministarka"),
    "Министар туризма": ("INST-MTURIZAM", "Министарство туризма и омладине", "ministar"),
    "Министар науке": ("INST-MNAUKA", "Министарство науке, технолошког развоја и иновација", "ministar"),
    "Министар информисања": ("INST-MINFO", "Министарство информисања и телекомуникација", "ministar"),
    "Министар за јавна улагања": ("INST-MJNULAGANJA", "Министарство за јавна улагања", "ministar"),
    "Министар спорта": ("INST-MSPORT", "Министарство спорта", "ministar"),
    "Министар за бригу о селу": ("INST-MSELO", "Министарство за бригу о селу", "ministar"),
    "Министар за људска и мањинска права": ("INST-MLJUDSKA", "Министарство за људска и мањинска права и друштвени дијалог", "ministar"),
    "Министар без портфеља": ("INST-GOV-PREDSEDNIK", "Влада Републике Србије", "ministar-bez-portfelja"),
    "Министарка без портфеља": ("INST-GOV-PREDSEDNIK", "Влада Републике Србије", "ministarka-bez-portfelja"),
}


def normalize_name(name: str) -> str:
    return re.sub(r"\s+", " ", unidecode(name.strip().lower()))


def _role_to_institution(role_text: str) -> tuple:
    """Map a full role string to (institution_id, institution_name, role_short)."""
    role_lower = role_text.lower()
    for key, val in MINISTRY_MAP.items():
        if key.lower() in role_lower:
            return val
    # Fallback — generic ministry
    return ("INST-GOV-UNKNOWN", "Влада Републике Србије", role_text[:60])


@dataclass
class CabinetMember:
    person_id: str
    full_name: str
    name_normalized: str
    role: str
    institution_id: str
    institution_name: str
    party_abbr: str = ""
    source: str = "vlada_wikipedia"
    scraped_at: str = ""


class VladaScraper:
    def __init__(self):
        self.client = httpx.Client(
            headers={"User-Agent": USER_AGENT},
            timeout=20.0,
            verify=False,
            follow_redirects=True,
        )
        self.output_dir = os.path.join(DATA_DIR, "raw", "vlada")
        os.makedirs(self.output_dir, exist_ok=True)

    def scrape(self, force_refresh: bool = False) -> list:
        cache_path = os.path.join(self.output_dir, "cabinet.json")
        if not force_refresh and os.path.exists(cache_path):
            mtime = os.path.getmtime(cache_path)
            age_hours = (time.time() - mtime) / 3600
            if age_hours < 24:
                logger.info("vlada_cache_hit", age_hours=round(age_hours, 1))
                with open(cache_path, encoding="utf-8") as f:
                    return json.load(f)

        logger.info("vlada_scrape_start", url=WIKIPEDIA_URL)
        try:
            resp = self.client.get(WIKIPEDIA_URL)
            resp.raise_for_status()
        except Exception as e:
            logger.error("vlada_fetch_failed", error=str(e))
            return []

        soup = BeautifulSoup(resp.text, "lxml")
        members = self._parse_cabinet(soup)
        logger.info("vlada_scraped", count=len(members))

        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump([asdict(m) for m in members], f, ensure_ascii=False, indent=2)

        return [asdict(m) for m in members]

    def _parse_cabinet(self, soup: BeautifulSoup) -> list:
        tables = soup.find_all("table")
        # Table index 2 consistently has the cabinet (40 rows)
        # Find the table with the most minister-related content
        cabinet_table = None
        for t in tables:
            rows = t.find_all("tr")
            if len(rows) >= 20:
                first_text = t.get_text()
                if "Председник Владе" in first_text or "Председник вла" in first_text:
                    cabinet_table = t
                    break

        if not cabinet_table:
            logger.warning("vlada_no_cabinet_table")
            return []

        members = []
        seen_names = set()
        rows = cabinet_table.find_all("tr")
        # Skip header row
        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 3:
                continue

            role_text = cells[0].get_text(strip=True)
            name_text = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            party_text = cells[4].get_text(strip=True) if len(cells) > 4 else ""

            if not name_text or len(name_text) < 4:
                continue
            if not role_text or any(skip in role_text for skip in ["Функција", "Ime", "Name"]):
                continue

            # Clean name — remove parentheticals, footnotes
            name = re.sub(r"\[.*?\]|\(.*?\)", "", name_text).strip()
            if not name or len(name) < 4:
                continue

            norm = normalize_name(name)
            if norm in seen_names:
                continue
            seen_names.add(norm)

            institution_id, institution_name, role_short = _role_to_institution(role_text)

            # Extract party abbreviation from parentheses
            party_abbr = ""
            m = re.search(r"\(([A-ZŠĐČĆŽА-Ш]{2,8})\)", party_text)
            if m:
                party_abbr = m.group(1)
            elif party_text:
                party_abbr = party_text[:20]

            import hashlib
            pid = f"P-GOV-{hashlib.md5(norm.encode()).hexdigest()[:12]}"

            members.append(CabinetMember(
                person_id=pid,
                full_name=name,
                name_normalized=norm,
                role=role_text,
                institution_id=institution_id,
                institution_name=institution_name,
                party_abbr=party_abbr,
                scraped_at=datetime.utcnow().isoformat(),
            ))

        return members

    def close(self):
        self.client.close()
