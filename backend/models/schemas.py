"""Pydantic models for the API and graph entities.

v2: Added RiskSummary, improved validation, added docstrings.
"""
from pydantic import BaseModel, Field
from typing import Optional
from datetime import date, datetime
from enum import Enum


class EntityType(str, Enum):
    PERSON = "Person"
    COMPANY = "Company"
    INSTITUTION = "Institution"
    CONTRACT = "Contract"
    PARTY = "PoliticalParty"


class SeverityLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class PersonCreate(BaseModel):
    person_id: str
    full_name: str = Field(..., min_length=2, max_length=200)
    name_normalized: Optional[str] = None
    date_of_birth: Optional[date] = None
    jmbg_hash: Optional[str] = Field(None, description="SHA-256 of JMBG for privacy")
    current_role: Optional[str] = None
    source: str = "manual"


class CompanyCreate(BaseModel):
    maticni_broj: str = Field(..., description="Company registration number (matični broj)")
    pib: Optional[str] = Field(None, description="Tax identification number (PIB)")
    name: str = Field(..., min_length=1, max_length=500)
    name_normalized: Optional[str] = None
    status: Optional[str] = None
    activity_code: Optional[str] = None
    founding_date: Optional[date] = None
    address: Optional[str] = None
    city: Optional[str] = None
    source: str = "apr"


class ContractCreate(BaseModel):
    contract_id: str
    title: str = Field(..., min_length=1)
    description: Optional[str] = None
    value_rsd: Optional[float] = Field(None, ge=0)
    value_eur: Optional[float] = Field(None, ge=0)
    award_date: Optional[date] = None
    procurement_type: Optional[str] = Field(None, description="open, restricted, negotiated")
    num_bidders: Optional[int] = Field(None, ge=0)
    status: Optional[str] = None
    source_url: Optional[str] = None
    awarding_institution: Optional[str] = None
    winning_company_mb: Optional[str] = None


class RelationshipCreate(BaseModel):
    from_id: str
    from_type: EntityType
    to_id: str
    to_type: EntityType
    relationship: str = Field(..., min_length=1)
    properties: dict = {}


class SearchRequest(BaseModel):
    query: str = Field(..., min_length=2)
    entity_type: Optional[EntityType] = None
    limit: int = Field(25, ge=1, le=100)


class GraphNeighborhood(BaseModel):
    center_id: str
    center_type: EntityType
    depth: int = Field(2, ge=1, le=4)
    limit: int = Field(100, ge=1, le=500)


class SuspiciousPattern(BaseModel):
    pattern_type: str
    severity: SeverityLevel
    description: str
    entities: list[dict]
    evidence: list[str]
    detected_at: datetime


class RiskSummary(BaseModel):
    """Aggregate risk score from all detection patterns."""
    risk_score: int = 0
    risk_level: SeverityLevel = SeverityLevel.LOW
    severity_counts: dict = Field(
        default_factory=lambda: {"critical": 0, "high": 0, "medium": 0, "low": 0}
    )


class DashboardStats(BaseModel):
    total_persons: int = 0
    total_companies: int = 0
    total_contracts: int = 0
    total_institutions: int = 0
    total_relationships: int = 0
    suspicious_patterns: int = 0
    last_scrape: Optional[datetime] = None
    data_sources_active: int = 0
    risk_summary: Optional[RiskSummary] = None
