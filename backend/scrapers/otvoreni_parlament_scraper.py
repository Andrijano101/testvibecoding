"""
Otvoreni Parlament Scraper
Scrapes Serbian MP data from otvoreniparlament.rs including:
- Name, party/club affiliation
- Company directorships (from ACAS-style asset declarations embedded in profiles)
- Public institutional roles

This is a critical source for detecting politician-company conflicts:
MP directors of companies that win public contracts = direct conflict of interest.
"""
import os
import re
import time
import json
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field, asdict

import httpx
from bs4 import BeautifulSoup
from unidecode import unidecode
from tenacity import retry, stop_after_attempt, wait_exponential
import structlog

logger = structlog.get_logger()

BASE_URL = "https://otvoreniparlament.rs"
DELAY = float(os.getenv("SCRAPE_DELAY", "1.5"))
DATA_DIR = os.getenv("DATA_DIR", "./data")
USER_AGENT = "Mozilla/5.0 (compatible; SrpskaTransparentnost/1.0)"

# Roles that indicate a person directs or controls a company
DIRECTOR_ROLES = {
    "generalni direktor", "direktor", "izvršni direktor", "predsednik odbora direktora",
    "predsednik nadzornog odbora", "član nadzornog odbora", "predsednik upravnog odbora",
    "član upravnog odbora", "generalni sekretar", "zamenik direktora", "pomoćnik direktora",
    "vd direktor", "v.d. direktor", "predsednik", "potpredsednik", "osnivač", "vlasnik",
    "partner", "prokurista", "zastupnik",
}

# Roles that indicate a government/public position (not company)
PUBLIC_ROLES = {
    "narodni poslanik", "narodni poslanica", "poslanik", "ministar", "premijer",
    "predsednik vlade", "potpredsednik vlade", "predsednik opštine", "predsednik grada",
    "gradonačelnik", "zamenik gradonačelnika", "savetnik predsednika", "ambasador",
    "državni sekretar", "pomoćnik ministra", "načelnik", "direktor agencije",
}

# Keywords in company name that indicate it's a public institution, not a private company
PUBLIC_ENTITY_KEYWORDS = {
    "skupština", "skupstina", "grad ", "gradska", "gradske", "gradski",
    "ministarstvo", "opština", "opshtina", "narodna skupština", "vlada",
    "republika srbija", "srbije", "poreska uprava", "fond", "agencija za",
    "komisija za", "zavod za", "javno preduzeće jp ", "klinički centar",
    "klinicki centar", "bolnica", "dom zdravlja",
}


def normalize_name(name: str) -> str:
    if not name:
        return ""
    cleaned = unidecode(name.strip().lower())
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def extract_party_from_klub(klub_text: str) -> str:
    """Extract party short name from 'IVICA DAČIĆ - Socijalistička partija Srbije (SPS)'."""
    if not klub_text:
        return ""
    # Try to extract content in parentheses as abbreviation
    m = re.search(r'\(([A-ZŠĐČĆŽ]{2,6})\)', klub_text)
    if m:
        return m.group(1)
    # Extract after dash
    if " - " in klub_text:
        return klub_text.split(" - ", 1)[1].strip()
    return klub_text.strip()


@dataclass
class MPCompanyRole:
    role: str
    company_name: str
    company_name_normalized: str = ""
    income_rsd: Optional[float] = None
    period_start: str = ""


@dataclass
class MPRecord:
    op_id: str                          # Otvoreni Parlament ID
    full_name: str
    name_normalized: str = ""
    party_name: str = ""
    party_abbr: str = ""
    club: str = ""                      # Poslanički klub (may differ from party)
    birth_date: str = ""
    city: str = ""
    profession: str = ""
    public_role: str = "Narodni poslanik/Narodni poslanica"
    institution: str = "Narodna skupština Republike Srbije"
    company_roles: list = field(default_factory=list)
    source_url: str = ""
    scraped_at: str = ""


class OtvoreniParlamentScraper:
    def __init__(self):
        self.client = httpx.Client(
            headers={"User-Agent": USER_AGENT},
            timeout=20.0,
            verify=False,
            follow_redirects=True,
        )
        self.output_dir = os.path.join(DATA_DIR, "raw", "op")
        os.makedirs(self.output_dir, exist_ok=True)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8))
    def _fetch(self, url: str) -> httpx.Response:
        resp = self.client.get(url)
        resp.raise_for_status()
        return resp

    def scrape_all(self, force_refresh: bool = False) -> list[MPRecord]:
        """Scrape all current MPs and their company roles."""
        logger.info("op_scrape_start", url=BASE_URL + "/poslanik")

        # Get MP listing
        mp_links = self._get_mp_links()
        logger.info("op_mp_links_found", count=len(mp_links))

        records = []
        for i, (name, url) in enumerate(mp_links):
            op_id = url.rstrip("/").split("/")[-1]
            cache_path = os.path.join(self.output_dir, f"{op_id}.json")

            if not force_refresh and os.path.exists(cache_path):
                try:
                    with open(cache_path, encoding="utf-8") as f:
                        rec_data = json.load(f)
                    records.append(MPRecord(**{k: v for k, v in rec_data.items() if k in MPRecord.__dataclass_fields__}))
                    logger.info("op_cache_hit", i=i, op_id=op_id)
                    continue
                except Exception:
                    pass

            logger.info("op_scrape_mp", i=i, op_id=op_id, name=name)
            try:
                record = self._scrape_profile(url, op_id)
                if record:
                    self._save(record)
                    records.append(record)
            except Exception as e:
                logger.error("op_scrape_failed", op_id=op_id, name=name, error=str(e))

            time.sleep(DELAY)

        logger.info("op_scrape_done", total=len(records),
                    with_company_roles=sum(1 for r in records if r.company_roles))
        return records

    def _get_mp_links(self) -> list[tuple[str, str]]:
        """Get all MP name+URL pairs from the listing page."""
        try:
            resp = self._fetch(f"{BASE_URL}/poslanik")
            soup = BeautifulSoup(resp.text, "lxml")
            seen = set()
            links = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/poslanik/" in href:
                    parts = href.rstrip("/").split("/")
                    if parts and parts[-1].isdigit():
                        full_url = href if href.startswith("http") else BASE_URL + href
                        if full_url not in seen:
                            seen.add(full_url)
                            links.append((a.get_text(strip=True), full_url))
            return links
        except Exception as e:
            logger.error("op_listing_failed", error=str(e))
            return []

    def _scrape_profile(self, url: str, op_id: str) -> Optional[MPRecord]:
        """Scrape a single MP profile page."""
        resp = self._fetch(url)
        soup = BeautifulSoup(resp.text, "lxml")

        # Name
        name_el = soup.find("h2")
        if not name_el:
            return None
        full_name = re.sub(r"\s+", " ", name_el.get_text(strip=True))
        if not full_name or len(full_name) < 3:
            return None

        record = MPRecord(
            op_id=op_id,
            full_name=full_name,
            name_normalized=normalize_name(full_name),
            source_url=url,
            scraped_at=datetime.utcnow().isoformat(),
        )

        # Basic info section
        basic_h = soup.find("h3", string=re.compile("Osnovne informacije", re.I))
        if basic_h:
            container = basic_h.find_parent(["div", "section", "article"])
            if container:
                text = container.get_text(separator="|", strip=True)
                # Extract club
                klub_m = re.search(r"Poslanički klub\s*[:\|]\s*([^\|]+)", text)
                if klub_m:
                    record.club = klub_m.group(1).strip()
                    record.party_name = extract_party_from_klub(record.club)
                    # Extract abbreviation
                    m = re.search(r'\(([A-ZŠĐČĆŽ]{2,6})\)', record.club)
                    record.party_abbr = m.group(1) if m else ""

                # Birthdate
                bdate_m = re.search(r"Datum rođenja\s*[:\|]\s*(\d{2}\.\d{2}\.\d{4})", text)
                if bdate_m:
                    record.birth_date = bdate_m.group(1)

                # City
                city_m = re.search(r"Mesto prebivališta\s*[:\|]\s*([^\|]+)", text)
                if city_m:
                    record.city = city_m.group(1).strip()

                # Profession
                prof_m = re.search(r"Zanimanje\s*[:\|]\s*([^\|]+)", text)
                if prof_m:
                    record.profession = prof_m.group(1).strip()

        # Company roles from "Funkcija" section
        record.company_roles = self._extract_company_roles(soup)

        return record

    def _extract_company_roles(self, soup: BeautifulSoup) -> list[dict]:
        """Extract company director/board roles from the asset declaration section."""
        roles = []

        # Find the "Funkcija" table
        func_el = soup.find(string=re.compile(r"^Funkcija$", re.I))
        if not func_el:
            # Try alternative: look for the table after "Državni organ, javno preduzeće" header
            func_el = soup.find(string=re.compile("Državni organ.*javno preduzeće", re.I))

        if not func_el:
            return roles

        # Get the parent row/element and traverse sibling rows
        parent = func_el.find_parent(["tr", "div", "p", "th"])
        if not parent:
            return roles

        # Get the table containing this element
        table = parent.find_parent("table")
        if not table:
            # Try getting next sibling rows directly
            container = parent.find_parent(["div", "section"])
            if container:
                rows = container.find_all("tr")
            else:
                return roles
        else:
            rows = table.find_all("tr")

        seen_companies = set()
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            role_text = cells[0].get_text(strip=True).lower()
            company_text = cells[1].get_text(strip=True) if len(cells) > 1 else ""

            # Skip header rows and empty rows
            if not role_text or not company_text:
                continue
            if role_text in ("funkcija", "vrsta", "naziv i sedište pravnog lica"):
                continue

            # Skip public/government roles
            is_public = any(pr in role_text for pr in PUBLIC_ROLES)
            if is_public:
                continue

            # Skip property entries (parcela, njiva, vikendica, etc.)
            if any(w in role_text for w in ("parcela", "njiva", "vinograd", "pašnjak", "šuma", "automobil", "vrsta")):
                continue
            if any(w in company_text.lower() for w in ("klase", "m2", "audi", "john deere", "talijanski")):
                continue

            # This should be a company role
            is_director = any(dr in role_text for dr in DIRECTOR_ROLES)
            if not is_director and len(role_text) > 50:
                continue  # Skip long non-role strings

            company_name = company_text.strip()
            # Skip public institutions masquerading as companies
            co_lower = company_name.lower()
            if any(kw in co_lower for kw in PUBLIC_ENTITY_KEYWORDS):
                continue

            company_key = normalize_name(company_name)

            # Extract income if available
            income = None
            if len(cells) >= 5:
                income_text = cells[4].get_text(strip=True).replace(",", ".") if len(cells) > 4 else ""
                try:
                    income = float(income_text) if income_text else None
                except ValueError:
                    pass

            # Period
            period = ""
            if len(cells) >= 7:
                period = cells[6].get_text(strip=True) if len(cells) > 6 else ""

            if company_key and company_key not in seen_companies:
                seen_companies.add(company_key)
                roles.append({
                    "role": cells[0].get_text(strip=True),
                    "company_name": company_name,
                    "company_name_normalized": company_key,
                    "income_rsd": income,
                    "period_start": period,
                })

        return roles

    def _save(self, record: MPRecord):
        path = os.path.join(self.output_dir, f"{record.op_id}.json")
        data = asdict(record)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def close(self):
        self.client.close()
