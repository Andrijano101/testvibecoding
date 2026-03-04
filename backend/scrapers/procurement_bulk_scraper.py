"""
UJN Procurement Bulk Scraper
Downloads real procurement data from portal.ujn.gov.rs open data files.

Working URLs (confirmed):
  http://portal.ujn.gov.rs/OpenD/OpenData_2020.xlsx  (36k rows, 10.4 MB)
  http://portal.ujn.gov.rs/OpenD/OpenData_2019.xlsx  (13.4 MB)
  http://portal.ujn.gov.rs/OpenD/OpenData_2018.csv   (38.2 MB, 61k rows)

Data structure: procurement notices with real institution names + maticni_broj.
Contract award decisions (OdlukaODodeliUgovora=1) have linked pages.
"""
import os
import json
import csv
import io
import time
import unicodedata
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Optional

import httpx
import structlog

logger = structlog.get_logger()

DELAY = float(os.getenv("SCRAPE_DELAY", "2"))
DATA_DIR = os.getenv("DATA_DIR", "./data")

# Confirmed working open data files
OPEN_DATA_URLS = [
    ("2020", "http://portal.ujn.gov.rs/OpenD/OpenData_2020.xlsx", "xlsx"),
    ("2019", "http://portal.ujn.gov.rs/OpenD/OpenData_2019.xlsx", "xlsx"),
    ("2018", "http://portal.ujn.gov.rs/OpenD/OpenData_2018.csv", "csv"),
]

# Only download 2020 by default (fastest, still recent)
DEFAULT_YEAR = "2020"


@dataclass
class InstitutionRecord:
    institution_id: str
    name: str
    name_normalized: str
    maticni_broj: str
    pib: Optional[str]
    city: Optional[str]
    source: str = "ujn"
    source_url: str = "http://portal.ujn.gov.rs/OpenD/"
    scraped_at: str = ""

    def __post_init__(self):
        if not self.scraped_at:
            self.scraped_at = datetime.utcnow().isoformat()


@dataclass
class ProcurementRecord:
    contract_id: str
    title: str
    subject: str
    institution_mb: str
    institution_name: str
    proc_type: Optional[str]
    subject_type: Optional[str]
    date_modified: Optional[str]
    detail_url: str
    has_award_decision: bool
    source: str = "ujn"
    scraped_at: str = ""

    def __post_init__(self):
        if not self.scraped_at:
            self.scraped_at = datetime.utcnow().isoformat()


def _normalize(text: str) -> str:
    """Normalize Serbian text to ASCII for matching."""
    if not text:
        return ""
    # Replace Cyrillic
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
    # Strip diacritics
    result = unicodedata.normalize('NFKD', result)
    result = ''.join(c for c in result if not unicodedata.combining(c))
    return result.lower().strip()


class ProcurementBulkScraper:
    """Downloads UJN open data files and extracts institution + procurement data."""

    def __init__(self):
        self.client = httpx.Client(
            timeout=180.0,
            follow_redirects=True,
            headers={"User-Agent": "SrpskaTransparentnost/1.0 (+https://transparentnost.rs)"},
        )
        os.makedirs(f"{DATA_DIR}/raw/ujn", exist_ok=True)
        os.makedirs(f"{DATA_DIR}/raw/institutions", exist_ok=True)

    def _download_xlsx(self, url: str, year: str) -> Optional[bytes]:
        cache_path = f"{DATA_DIR}/raw/ujn/opendata_{year}.xlsx"
        if os.path.exists(cache_path) and os.path.getsize(cache_path) > 100_000:
            logger.info("ujn_cache_hit", year=year)
            with open(cache_path, "rb") as f:
                return f.read()
        logger.info("ujn_download_start", url=url, year=year)
        try:
            resp = self.client.get(url)
            if resp.status_code != 200:
                logger.error("ujn_download_failed", status=resp.status_code, url=url)
                return None
            data = resp.content
            with open(cache_path, "wb") as f:
                f.write(data)
            logger.info("ujn_download_done", year=year, size_mb=round(len(data) / 1e6, 1))
            return data
        except Exception as e:
            logger.error("ujn_download_error", error=str(e))
            return None

    def _download_csv(self, url: str, year: str) -> Optional[str]:
        cache_path = f"{DATA_DIR}/raw/ujn/opendata_{year}.csv"
        if os.path.exists(cache_path) and os.path.getsize(cache_path) > 100_000:
            logger.info("ujn_csv_cache_hit", year=year)
            with open(cache_path, "r", encoding="utf-8-sig") as f:
                return f.read()
        logger.info("ujn_csv_download_start", url=url, year=year)
        try:
            resp = self.client.get(url)
            if resp.status_code != 200:
                logger.error("ujn_csv_download_failed", status=resp.status_code)
                return None
            # Try UTF-8 with BOM first, fall back to windows-1250
            try:
                text = resp.content.decode("utf-8-sig")
            except UnicodeDecodeError:
                text = resp.content.decode("windows-1250", errors="replace")
            with open(cache_path, "w", encoding="utf-8") as f:
                f.write(text)
            logger.info("ujn_csv_download_done", year=year, size_mb=round(len(resp.content) / 1e6, 1))
            return text
        except Exception as e:
            logger.error("ujn_csv_download_error", error=str(e))
            return None

    def _parse_xlsx(self, data: bytes, year: str):
        """Parse XLSX and yield row dicts."""
        try:
            import openpyxl
            from io import BytesIO
            wb = openpyxl.load_workbook(BytesIO(data), read_only=True)
            ws = wb.active
            headers = [str(c.value) if c.value else "" for c in next(ws.iter_rows(max_row=1))]
            for row in ws.iter_rows(min_row=2, values_only=True):
                yield dict(zip(headers, row))
            wb.close()
        except ImportError:
            logger.error("openpyxl_not_installed")

    def _parse_csv_rows(self, text: str):
        """Parse CSV and yield row dicts."""
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            yield row

    def _row_to_records(self, row: dict, year: str):
        """Convert a UJN data row to InstitutionRecord + ProcurementRecord."""
        # Institution
        mb = str(row.get("MaticniBroj") or row.get("MaticniBroj") or "").strip()
        name = str(row.get("Naziv") or row.get("NazivNarucioca") or "").strip()
        if not mb or not name or mb == "None":
            return None, None

        inst = InstitutionRecord(
            institution_id=f"INST-UJN-{mb}",
            name=name,
            name_normalized=_normalize(name),
            maticni_broj=mb,
            pib=str(row.get("PIB") or "").strip() or None,
            city=None,
            source_url=f"http://portal.ujn.gov.rs/OpenD/OpenData_{year}.xlsx",
        )

        # Procurement notice
        doc_id = str(row.get("ID_Dokument") or row.get("SifraNabavke") or "").strip()
        title = str(row.get("NazivDokumenta") or "").strip()
        subject = str(row.get("PredmetNabavke") or "").strip()
        link = str(row.get("Link") or "").strip()

        has_award = str(row.get("OdlukaODodeliUgovora") or "0").strip() == "1"

        if not doc_id:
            return inst, None

        proc = ProcurementRecord(
            contract_id=f"PROC-UJN-{doc_id}",
            title=title[:200] if title else subject[:200],
            subject=subject[:300] if subject else "",
            institution_mb=mb,
            institution_name=name,
            proc_type=str(row.get("IdVrstaPostupka") or "").strip() or None,
            subject_type=str(row.get("VrstaPredmeta") or "").strip() or None,
            date_modified=str(row.get("DatumPoslednjeIzmene") or "").strip() or None,
            detail_url=link,
            has_award_decision=has_award,
        )
        return inst, proc

    def scrape(self, year: str = DEFAULT_YEAR, max_rows: int = 5000) -> tuple[list, list]:
        """
        Download UJN open data for a given year and extract institutions + procurements.
        Returns (institutions, procurements).
        max_rows: cap on how many procurement rows to load (institutions are always deduped).
        """
        # Find URL for year
        url, fmt = None, None
        for y, u, f in OPEN_DATA_URLS:
            if y == year:
                url, fmt = u, f
                break
        if not url:
            logger.error("ujn_unknown_year", year=year)
            return [], []

        # Download
        if fmt == "xlsx":
            data = self._download_xlsx(url, year)
            if not data:
                return [], []
            rows = self._parse_xlsx(data, year)
        else:
            text = self._download_csv(url, year)
            if not text:
                return [], []
            rows = self._parse_csv_rows(text)

        institutions: dict[str, InstitutionRecord] = {}
        procurements: list[ProcurementRecord] = []
        award_only_procs = []
        total = 0

        for row in rows:
            total += 1
            inst, proc = self._row_to_records(row, year)
            if inst and inst.maticni_broj not in institutions:
                institutions[inst.maticni_broj] = inst
            if proc:
                if proc.has_award_decision:
                    award_only_procs.append(proc)
                elif len(procurements) < max_rows // 2:
                    procurements.append(proc)

        # Prioritize award decisions
        procurements = award_only_procs[:max_rows] + procurements[:max(0, max_rows - len(award_only_procs))]

        logger.info("ujn_parse_done",
                    year=year,
                    total_rows=total,
                    unique_institutions=len(institutions),
                    procurements=len(procurements),
                    award_decisions=len(award_only_procs))

        # Save institutions
        inst_list = list(institutions.values())
        inst_path = f"{DATA_DIR}/raw/institutions/ujn_{year}.json"
        with open(inst_path, "w", encoding="utf-8") as f:
            json.dump([asdict(i) for i in inst_list], f, ensure_ascii=False, indent=2)

        # Save procurements
        proc_path = f"{DATA_DIR}/raw/ujn/procurements_{year}.json"
        with open(proc_path, "w", encoding="utf-8") as f:
            json.dump([asdict(p) for p in procurements], f, ensure_ascii=False, indent=2)

        return inst_list, procurements

    def close(self):
        self.client.close()
