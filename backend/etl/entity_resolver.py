"""
Entity Resolution Engine v2

Matches and deduplicates persons and companies across data sources.
Uses fuzzy matching, JMBG hashing, and company registration numbers.

Improvements:
- Indexed name lookups for O(1) exact match before O(n) fuzzy scan
- Persistence support (save/load resolver state)
- Better Cyrillic transliteration (ћ→ć, ч→č, etc.)
- Match audit trail
"""
import re
import json
import hashlib
from typing import Optional
from dataclasses import dataclass, field, asdict
from collections import defaultdict

from unidecode import unidecode
from fuzzywuzzy import fuzz
import structlog

logger = structlog.get_logger()

# Thresholds
NAME_MATCH_THRESHOLD = 88
COMPANY_NAME_THRESHOLD = 85

# Serbian Cyrillic -> Latin (proper diacritics first, then ASCII fallback via unidecode)
CYRILLIC_MAP = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "ђ": "đ",
    "е": "e", "ж": "ž", "з": "z", "и": "i", "ј": "j", "к": "k",
    "л": "l", "љ": "lj", "м": "m", "н": "n", "њ": "nj", "о": "o",
    "п": "p", "р": "r", "с": "s", "т": "t", "ћ": "ć", "у": "u",
    "ф": "f", "х": "h", "ц": "c", "ч": "č", "џ": "dž", "ш": "š",
    # Uppercase
    "А": "A", "Б": "B", "В": "V", "Г": "G", "Д": "D", "Ђ": "Đ",
    "Е": "E", "Ж": "Ž", "З": "Z", "И": "I", "Ј": "J", "К": "K",
    "Л": "L", "Љ": "Lj", "М": "M", "Н": "N", "Њ": "Nj", "О": "O",
    "П": "P", "Р": "R", "С": "S", "Т": "T", "Ћ": "Ć", "У": "U",
    "Ф": "F", "Х": "H", "Ц": "C", "Ч": "Č", "Џ": "Dž", "Ш": "Š",
}


@dataclass
class ResolvedEntity:
    canonical_id: str
    entity_type: str  # person, company
    canonical_name: str
    source_ids: list = field(default_factory=list)
    confidence: float = 1.0
    match_method: str = "new"


def cyrillic_to_latin(text: str) -> str:
    """Convert Serbian Cyrillic to Latin script."""
    result = []
    for char in text:
        result.append(CYRILLIC_MAP.get(char, char))
    return "".join(result)


def normalize_serbian_name(name: str) -> str:
    """Normalize Serbian personal name for matching.

    Pipeline: Cyrillic->Latin -> remove diacritics -> lowercase -> clean whitespace.
    """
    if not name:
        return ""

    # Cyrillic to Latin
    name = cyrillic_to_latin(name)

    # Remove diacritics
    name = unidecode(name).lower()

    # Remove non-alpha chars except spaces
    name = re.sub(r"[^a-z\s]", "", name)
    name = re.sub(r"\s+", " ", name).strip()

    return name


def normalize_company_name(name: str) -> str:
    """Normalize company name for matching."""
    if not name:
        return ""

    name = normalize_serbian_name(name)

    # Remove common legal form suffixes
    legal_forms = [
        r"\bdoo\b", r"\bd\.o\.o\b", r"\bad\b", r"\ba\.d\b",
        r"\bszr\b", r"\bstr\b", r"\bpr\b",
        r"\bpreduzetnik\b", r"\bortacka\b", r"\bkomanditno\b",
        r"\bakcionarsko\b",
        r"\bdrustvo sa ogranicenom odgovornoscu\b",
        r"\bakcionarsko drustvo\b",
    ]
    for form in legal_forms:
        name = re.sub(form, "", name)

    name = re.sub(r"\s+", " ", name).strip()
    return name


def generate_person_id(name: str, jmbg: str = "", source: str = "") -> str:
    """Generate a stable person ID. Priority: JMBG hash > name+source hash."""
    if jmbg and len(jmbg) == 13:
        return f"P-{hashlib.sha256(jmbg.encode()).hexdigest()[:16]}"
    normalized = normalize_serbian_name(name)
    seed = f"{normalized}:{source}"
    return f"P-{hashlib.sha256(seed.encode()).hexdigest()[:16]}"


def match_persons(name1: str, name2: str, jmbg1: str = "", jmbg2: str = "") -> tuple[bool, float, str]:
    """Determine if two person records refer to the same individual."""
    # JMBG match is definitive
    if jmbg1 and jmbg2 and len(jmbg1) == 13 and len(jmbg2) == 13:
        if jmbg1 == jmbg2:
            return True, 1.0, "jmbg_exact"
        else:
            return False, 0.0, "jmbg_mismatch"

    n1 = normalize_serbian_name(name1)
    n2 = normalize_serbian_name(name2)

    if not n1 or not n2:
        return False, 0.0, "empty_name"

    # Exact normalized match
    if n1 == n2:
        return True, 0.95, "name_exact"

    # Token sort ratio (handles name reordering: "Petar Marko" == "Marko Petar")
    score = fuzz.token_sort_ratio(n1, n2)
    if score >= NAME_MATCH_THRESHOLD:
        return True, score / 100.0, "name_fuzzy"

    # Partial ratio for substring matching (handles missing middle names)
    partial = fuzz.partial_ratio(n1, n2)
    if partial >= 95 and min(len(n1), len(n2)) > 5:
        return True, partial / 100.0 * 0.8, "name_partial"

    return False, score / 100.0, "no_match"


def match_companies(name1: str, name2: str, mb1: str = "", mb2: str = "") -> tuple[bool, float, str]:
    """Determine if two company records refer to the same entity."""
    # Maticni broj match is definitive
    if mb1 and mb2:
        return (True, 1.0, "mb_exact") if mb1 == mb2 else (False, 0.0, "mb_mismatch")

    n1 = normalize_company_name(name1)
    n2 = normalize_company_name(name2)

    if not n1 or not n2:
        return False, 0.0, "empty_name"

    if n1 == n2:
        return True, 0.95, "name_exact"

    score = fuzz.token_sort_ratio(n1, n2)
    if score >= COMPANY_NAME_THRESHOLD:
        return True, score / 100.0, "name_fuzzy"

    return False, score / 100.0, "no_match"


class EntityResolver:
    """Batch entity resolution with indexed lookups.

    Uses a normalized-name index to get O(1) exact matches before
    falling back to O(n) fuzzy scanning.
    """

    def __init__(self):
        self.person_registry: dict[str, ResolvedEntity] = {}
        self.company_registry: dict[str, ResolvedEntity] = {}
        # Index: normalized_name -> list of canonical_ids (for fast exact lookup)
        self._person_name_index: dict[str, list[str]] = defaultdict(list)
        self._company_name_index: dict[str, list[str]] = defaultdict(list)

    def resolve_person(self, name: str, jmbg: str = "", source: str = "") -> ResolvedEntity:
        """Resolve a person record to a canonical entity.

        1. Try JMBG exact match
        2. Try normalized name index (exact)
        3. Fuzzy scan remaining entries
        """
        normalized = normalize_serbian_name(name)

        # 1. JMBG-based lookup
        if jmbg and len(jmbg) == 13:
            jmbg_id = f"P-{hashlib.sha256(jmbg.encode()).hexdigest()[:16]}"
            if jmbg_id in self.person_registry:
                entity = self.person_registry[jmbg_id]
                entity.source_ids.append(f"{source}:{name}")
                entity.confidence = 1.0
                return entity

        # 2. Exact name index lookup
        if normalized in self._person_name_index:
            for cid in self._person_name_index[normalized]:
                entity = self.person_registry[cid]
                entity.source_ids.append(f"{source}:{name}")
                entity.confidence = max(entity.confidence, 0.95)
                logger.debug("person_matched", name=name, canonical=entity.canonical_name, method="name_index")
                return entity

        # 3. Fuzzy scan (only against entities NOT already matched by index)
        for cid, entity in self.person_registry.items():
            is_match, confidence, method = match_persons(name, entity.canonical_name, jmbg)
            if is_match:
                entity.source_ids.append(f"{source}:{name}")
                entity.confidence = max(entity.confidence, confidence)
                # Add to index for future fast lookups
                self._person_name_index[normalized].append(cid)
                logger.debug("person_matched", name=name, canonical=entity.canonical_name, method=method)
                return entity

        # 4. New entity
        pid = generate_person_id(name, jmbg, source)
        entity = ResolvedEntity(
            canonical_id=pid,
            entity_type="person",
            canonical_name=name,
            source_ids=[f"{source}:{name}"],
            confidence=1.0,
            match_method="new",
        )
        self.person_registry[pid] = entity
        self._person_name_index[normalized].append(pid)
        return entity

    def resolve_company(self, name: str, mb: str = "", source: str = "") -> ResolvedEntity:
        """Resolve a company record to a canonical entity."""
        # MB is definitive
        if mb and mb in self.company_registry:
            entity = self.company_registry[mb]
            entity.source_ids.append(f"{source}:{name}")
            return entity

        # Check name index
        normalized = normalize_company_name(name)
        if normalized in self._company_name_index:
            for cid in self._company_name_index[normalized]:
                entity = self.company_registry.get(cid)
                if entity:
                    entity.source_ids.append(f"{source}:{name}")
                    return entity

        cid = f"C-{mb}" if mb else f"C-{hashlib.sha256(normalized.encode()).hexdigest()[:16]}"
        entity = ResolvedEntity(
            canonical_id=cid,
            entity_type="company",
            canonical_name=name,
            source_ids=[f"{source}:{name}"],
            confidence=1.0 if mb else 0.8,
            match_method="mb_exact" if mb else "name_only",
        )
        if mb:
            self.company_registry[mb] = entity
        else:
            self.company_registry[cid] = entity
        self._company_name_index[normalized].append(cid)
        return entity

    def save(self, filepath: str):
        """Persist resolver state to JSON for reuse across runs."""
        data = {
            "persons": {k: asdict(v) for k, v in self.person_registry.items()},
            "companies": {k: asdict(v) for k, v in self.company_registry.items()},
        }
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info("resolver_saved", persons=len(self.person_registry), companies=len(self.company_registry))

    def load(self, filepath: str):
        """Load resolver state from a previous run."""
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)

            for pid, pdata in data.get("persons", {}).items():
                entity = ResolvedEntity(**pdata)
                self.person_registry[pid] = entity
                normalized = normalize_serbian_name(entity.canonical_name)
                self._person_name_index[normalized].append(pid)

            for cid, cdata in data.get("companies", {}).items():
                entity = ResolvedEntity(**cdata)
                self.company_registry[cid] = entity
                normalized = normalize_company_name(entity.canonical_name)
                self._company_name_index[normalized].append(cid)

            logger.info("resolver_loaded", persons=len(self.person_registry), companies=len(self.company_registry))
        except FileNotFoundError:
            logger.info("resolver_no_previous_state", file=filepath)
        except Exception as e:
            logger.error("resolver_load_failed", error=str(e))

    def stats(self) -> dict:
        """Return resolution statistics."""
        return {
            "total_persons": len(self.person_registry),
            "total_companies": len(self.company_registry),
            "multi_source_persons": sum(
                1 for e in self.person_registry.values() if len(e.source_ids) > 1
            ),
            "multi_source_companies": sum(
                1 for e in self.company_registry.values() if len(e.source_ids) > 1
            ),
        }
