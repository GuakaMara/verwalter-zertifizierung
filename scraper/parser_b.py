"""
Parser B — PDF Extraction (pdfplumber)

Für IHKs die Prüfungstermine als PDF veröffentlichen (z.B. München, Saarland).
- Lädt PDF herunter
- Extrahiert Text und Tabellen
- Wendet gleiche Datum-Erkennung an wie Parser A
"""

import re
import tempfile
import os
from .models import ScrapeResult, ExamEvent
from .parser_a import extract_all_dates, CTX_SCHRIFTLICH, CTX_MUENDLICH, CTX_DEADLINE, CTX_FEE, CTX_FEE_REV, CTX_AUSGEBUCHT, EXAM_KEYWORDS

try:
    import requests
except ImportError:
    requests = None

HEADERS = {
    "User-Agent": "Verwalter-Zertifizierung.de/1.0 (+https://verwalter-zertifizierung.de)",
    "Accept": "application/pdf,*/*",
}


def parse(url: str, ihk_id: str) -> ScrapeResult:
    """
    Download PDF and extract exam dates.
    Returns ScrapeResult.
    """
    result = ScrapeResult(ihk_id=ihk_id, stage="parser_b", url_used=url)

    # Check dependencies
    try:
        import pdfplumber
    except ImportError:
        result.error = "pdfplumber nicht installiert (pip3 install pdfplumber)"
        return result

    if not requests:
        result.error = "requests nicht installiert"
        return result

    # Download PDF
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            result.error = f"HTTP {resp.status_code}"
            result.http_status = resp.status_code
            return result
        result.http_status = resp.status_code

        content_type = resp.headers.get("Content-Type", "")
        if "pdf" not in content_type.lower() and not url.lower().endswith(".pdf"):
            result.error = f"Kein PDF: Content-Type={content_type}"
            return result

    except Exception as e:
        result.error = f"Download-Fehler: {str(e)[:100]}"
        return result

    # Save to temp file and parse
    events = []
    all_text = ""

    try:
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(resp.content)
            tmp_path = tmp.name

        with pdfplumber.open(tmp_path) as pdf:
            for page in pdf.pages:
                # Extract text
                page_text = page.extract_text() or ""
                all_text += page_text + "\n"

                # Extract tables
                for table in page.extract_tables():
                    if not table:
                        continue

                    # Try to detect header row
                    header = table[0] if table else []
                    header_lower = [str(h).lower() if h else "" for h in header]

                    for row in table[1:] if len(table) > 1 else table:
                        row_text = " ".join(str(cell) if cell else "" for cell in row)
                        row_dates = extract_all_dates(row_text)
                        dates_2026 = [d for d in row_dates if d["year"] == "2026"]

                        if dates_2026:
                            event = _build_pdf_event(row, header_lower, row_text, dates_2026)
                            events.append(event)

                # Also extract dates from plain text (for non-table PDFs)
                page_dates = extract_all_dates(page_text)
                # Process text paragraphs
                for para in page_text.split("\n\n"):
                    para_dates = extract_all_dates(para)
                    dates_2026 = [d for d in para_dates if d["year"] == "2026"]
                    if dates_2026 and any(kw in para.lower() for kw in ["prüfung", "termin", "schriftlich", "mündlich"]):
                        new_events = _build_text_events(para, dates_2026)
                        for event in new_events:
                            if not _is_dup(event, events):
                                events.append(event)

    except Exception as e:
        result.error = f"PDF-Parse-Fehler: {str(e)[:100]}"
        return result
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass

    # Collect results
    all_dates = extract_all_dates(all_text)
    result.raw_dates_2026 = sorted(set(d["date"] for d in all_dates if d["year"] == "2026"))
    result.exam_events = events
    result.success = len(result.raw_dates_2026) > 0
    result.content_length = len(all_text)

    # Fees (both "430 €" and "€ 430" patterns)
    fees = list(set(CTX_FEE.findall(all_text) + CTX_FEE_REV.findall(all_text)))
    result.fees = [f"{f} €" for f in fees if int(f.split(",")[0].split(".")[0]) >= 50]

    # Keywords
    text_lower = all_text.lower()
    kw_hits = sum(1 for kw in EXAM_KEYWORDS if kw in text_lower)
    result.keyword_score = f"{kw_hits}/{len(EXAM_KEYWORDS)}"

    if events:
        result.strategies_used.append("pdf_table" if any(e.source == "pdf_table" for e in events) else "pdf_text")

    return result


def _build_pdf_event(row, header_lower, row_text, dates_2026):
    """Build event from a PDF table row."""
    event = ExamEvent(
        dates=[d["date"] for d in dates_2026],
        source="pdf_table",
        evidence=row_text[:300],
    )

    # Try column-based mapping
    for i, h in enumerate(header_lower):
        if i >= len(row) or not row[i]:
            continue
        cell_text = str(row[i])
        cell_dates = extract_all_dates(cell_text)
        cell_2026 = [d for d in cell_dates if d["year"] == "2026"]

        if "schriftlich" in h and cell_2026:
            event.schriftlich = cell_2026[0]["date"]
        elif "mündlich" in h and cell_2026:
            event.muendlich = cell_2026[0]["date"]
        elif ("frist" in h or "anmeld" in h) and cell_2026:
            event.anmeldeschluss = cell_2026[0]["date"]
        # München-style: "Termin" column = schriftlich date
        elif ("termin" in h or "datum" in h or "prüfung" in h) and cell_2026:
            if not event.schriftlich:
                event.schriftlich = cell_2026[0]["date"]
        # "Bemerkung" column may contain "mündliche Prüfung am DD.MM.YYYY"
        elif ("bemerkung" in h or "hinweis" in h or "anmerkung" in h):
            if cell_2026 and not event.muendlich:
                # Check if text mentions mündlich
                if re.search(r"m[üu]ndlich", cell_text, re.IGNORECASE):
                    event.muendlich = cell_2026[0]["date"]

    # If no column mapping found mündlich, check row text for pattern
    # "mündliche Prüfung voraussichtlich am DD.MM.YYYY"
    if not event.muendlich:
        m = re.search(
            r"m[üu]ndlich\w*\s+(?:Prüfung\s+)?(?:voraussichtlich\s+)?(?:am\s+)?(\d{1,2}\.\d{1,2}\.\d{4})",
            row_text, re.IGNORECASE
        )
        if m:
            mdl_date = m.group(1)
            parts = mdl_date.split(".")
            if len(parts) == 3 and parts[2] == "2026":
                from .parser_a import normalize_date
                event.muendlich = normalize_date(parts[0], parts[1], parts[2])
                if event.muendlich not in event.dates:
                    event.dates.append(event.muendlich)

    # Set type
    if event.schriftlich and event.muendlich:
        event.type = "combined"
    elif event.schriftlich:
        event.type = "schriftlich"
    elif event.muendlich:
        event.type = "muendlich"
    else:
        # Fallback: if row has mündlich context + dates, first date is likely schriftlich
        has_muend_ctx = bool(CTX_MUENDLICH.search(row_text))
        has_schrift_ctx = bool(CTX_SCHRIFTLICH.search(row_text))
        if has_schrift_ctx and has_muend_ctx:
            event.type = "combined"
        elif has_muend_ctx and len(dates_2026) >= 2:
            # Typical pattern: schriftlich date first, then Bemerkung with mündlich
            # If we have 2+ dates and only mündlich context, first = schriftlich
            sorted_d = sorted(dates_2026, key=lambda d: d["date"])
            event.schriftlich = sorted_d[0]["date"]
            event.muendlich = sorted_d[-1]["date"]
            event.type = "combined" if event.schriftlich != event.muendlich else "exam_date"
        elif has_schrift_ctx:
            event.type = "schriftlich"
        elif has_muend_ctx:
            event.type = "muendlich"
        else:
            event.type = "exam_date"

    # Status
    if CTX_AUSGEBUCHT.search(row_text):
        event.status = "ausgebucht"

    return event


def _build_text_events(text, dates_2026):
    """Build event(s) from plain text paragraph. Returns list of events."""
    has_schrift = bool(CTX_SCHRIFTLICH.search(text))
    has_muend = bool(CTX_MUENDLICH.search(text))

    # Multiple dates, same type → separate events
    if len(dates_2026) >= 2 and has_schrift and not has_muend:
        events = []
        for d in dates_2026:
            ev = ExamEvent(
                dates=[d["date"]], source="pdf_text",
                evidence=text[:300], type="schriftlich",
                schriftlich=d["date"],
            )
            events.append(ev)
        return events

    # Single event (default)
    event = ExamEvent(
        dates=[d["date"] for d in dates_2026],
        source="pdf_text",
        evidence=text[:300],
    )

    if has_schrift and has_muend:
        event.type = "combined"
        if len(dates_2026) >= 2:
            event.schriftlich = dates_2026[0]["date"]
            event.muendlich = dates_2026[-1]["date"]
    elif has_schrift:
        event.type = "schriftlich"
        event.schriftlich = dates_2026[0]["date"]
    elif has_muend:
        event.type = "muendlich"
        event.muendlich = dates_2026[0]["date"]

    return [event]


def _is_dup(new_event, existing):
    new_dates = set(new_event.dates)
    for ev in existing:
        if new_dates.issubset(set(ev.dates)):
            return True
    return False
