"""
Shared data models for the IHK exam date scraper.
"""

from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional


@dataclass
class ExamEvent:
    """A single exam event (one exam date or exam pair)."""
    dates: list  # All dates associated with this event
    type: str = "unknown"  # combined | schriftlich | muendlich | exam_date | unknown
    schriftlich: Optional[str] = None  # DD.MM.YYYY
    muendlich: Optional[str] = None
    anmeldeschluss: Optional[str] = None
    status: str = "unknown"  # anmeldung_moeglich | ausgebucht | warteliste | unknown
    source: str = ""  # table | section | list | text_block | pdf | browser | llm | manual
    evidence: str = ""  # Raw text snippet that proves this date exists
    confidence: float = 1.0  # 1.0 for deterministic, 0.0-1.0 for LLM

    def to_dict(self):
        return {
            "dates": self.dates,
            "type": self.type,
            "schriftlich": self.schriftlich,
            "muendlich": self.muendlich,
            "anmeldeschluss": self.anmeldeschluss,
            "status": self.status,
            "source": self.source,
            "evidence": self.evidence[:300],
            "confidence": self.confidence,
        }


@dataclass
class ScrapeResult:
    """Result from any parser stage."""
    ihk_id: str
    stage: str  # parser_a | parser_b | parser_c | llm | source_discovery | manual | cache
    success: bool = False
    exam_events: list = field(default_factory=list)
    raw_dates_2026: list = field(default_factory=list)
    fees: list = field(default_factory=list)
    keyword_score: str = ""
    strategies_used: list = field(default_factory=list)
    error: Optional[str] = None
    http_status: Optional[int] = None
    content_length: int = 0
    url_used: str = ""
    timestamp: str = ""

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now().isoformat()

    def to_dict(self):
        return {
            "ihk_id": self.ihk_id,
            "stage": self.stage,
            "success": self.success,
            "exam_events": [e.to_dict() if hasattr(e, 'to_dict') else e for e in self.exam_events],
            "raw_dates_2026": self.raw_dates_2026,
            "fees": self.fees,
            "keyword_score": self.keyword_score,
            "strategies_used": self.strategies_used,
            "error": self.error,
            "http_status": self.http_status,
            "url_used": self.url_used,
            "timestamp": self.timestamp,
        }


@dataclass
class ValidationResult:
    """Result from the validator."""
    valid: bool = True
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)
    cleaned_events: list = field(default_factory=list)
