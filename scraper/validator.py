"""
Validator — prüft JEDES Ergebnis, egal aus welcher Stufe.

Regeln:
1. Jedes Datum muss ein echtes, gültiges Datum sein
2. Prüfungstermine müssen in der Zukunft liegen (oder max 30 Tage vergangen)
3. Anmeldefrist muss VOR dem Prüfungsdatum liegen (1-180 Tage)
4. schriftlich muss VOR mündlich liegen
4b. schriftlich↔mündlich max 30 Tage auseinander (sonst gemischte Daten)
5. Typ aus Whitelist
6. Status aus Whitelist
7. LLM-Ergebnisse brauchen Evidence
8. LLM-Confidence >= 0.5
9. Gebühren im plausiblen Bereich (50-1000€)
10. Keine Duplikate (Tabelle > Section > Text > LLM)
"""

from datetime import datetime, timedelta
from .models import ValidationResult, ExamEvent


VALID_STATUSES = {"anmeldung_moeglich", "ausgebucht", "warteliste", "unknown"}
VALID_TYPES = {"combined", "schriftlich", "muendlich", "exam_date", "unknown"}
MIN_FEE = 50
MAX_FEE = 1000
MAX_PAST_DAYS = 30
MAX_EXAM_GAP_DAYS = 30  # Max Tage zwischen schriftlich und mündlich

# Source priority: higher = more trustworthy
SOURCE_PRIORITY = {
    "table": 10,
    "line": 10,  # Line-based extraction is very reliable
    "pdf_table": 9,
    "browser_table": 8,
    "manual": 10,
    "list": 6,
    "section": 5,
    "text_block": 4,
    "pdf_text": 4,
    "browser_section": 5,
    "browser_text_block": 4,
    "browser_line": 10,
    "llm": 3,
}


def validate_scrape_result(result, previous_result=None) -> ValidationResult:
    """
    Validate a ScrapeResult. Returns ValidationResult with cleaned events.
    """
    vr = ValidationResult()
    cleaned = []

    for event in result.exam_events:
        ev = event if isinstance(event, dict) else event.to_dict()
        event_errors = []
        event_warnings = []

        # Extract fields
        frist = ev.get("anmeldeschluss")
        schrift = ev.get("schriftlich")
        muend = ev.get("muendlich")

        # ── Rule 1: All dates must be valid ──
        valid_dates = []
        for d in ev.get("dates", []):
            parsed = _parse_date(d)
            if parsed is None:
                event_errors.append(f"Ungültiges Datum: {d}")
            else:
                valid_dates.append(d)

        if not valid_dates:
            event_errors.append("Kein gültiges Datum im Event")

        # ── Rule 2: Dates should be in the future (or recent past) ──
        cutoff = datetime.now() - timedelta(days=MAX_PAST_DAYS)
        future_dates = []
        for d in valid_dates:
            parsed = _parse_date(d)
            if parsed and parsed >= cutoff:
                future_dates.append(d)
            else:
                event_warnings.append(f"Datum in der Vergangenheit: {d}")

        # ── Rule 3: Anmeldefrist plausibility ──
        if frist and (schrift or muend):
            frist_dt = _parse_date(frist)
            exam_dt = _parse_date(schrift or muend)
            if frist_dt and exam_dt:
                gap = (exam_dt - frist_dt).days
                if gap < 0:
                    event_errors.append(
                        f"Anmeldefrist ({frist}) liegt nach Prüfung ({schrift or muend}) — falsche Zuordnung"
                    )
                elif gap > 180:
                    event_warnings.append(
                        f"Anmeldefrist ({frist}) liegt {gap} Tage vor Prüfung — ungewöhnlich"
                    )

        # ── Rule 4: schriftlich before mündlich ──
        if schrift and muend:
            s_dt = _parse_date(schrift)
            m_dt = _parse_date(muend)
            if s_dt and m_dt:
                if s_dt > m_dt:
                    event_errors.append(
                        f"Schriftlich ({schrift}) liegt nach mündlich ({muend}) — falsche Zuordnung"
                    )
                # Rule 4b: gap check
                gap = abs((m_dt - s_dt).days)
                if gap > MAX_EXAM_GAP_DAYS:
                    event_errors.append(
                        f"Schriftlich ({schrift}) und mündlich ({muend}) liegen {gap} Tage auseinander — gemischte Daten"
                    )

        # ── Rule 5: Type must be valid ──
        if ev.get("type") not in VALID_TYPES:
            event_warnings.append(f"Unbekannter Typ: {ev.get('type')}")

        # ── Rule 6: Status must be from whitelist ──
        if ev.get("status") not in VALID_STATUSES:
            event_warnings.append(f"Unbekannter Status: {ev.get('status')}")

        # ── Rule 7: Evidence for LLM ──
        if ev.get("source") == "llm" and not ev.get("evidence"):
            event_warnings.append("LLM-Ergebnis ohne Evidence-Snippet")

        # ── Rule 8: LLM confidence ──
        if ev.get("source") == "llm" and ev.get("confidence", 1.0) < 0.5:
            event_errors.append(f"LLM-Confidence zu niedrig: {ev.get('confidence')}")

        # ── Collect ──
        if event_errors:
            vr.errors.extend(event_errors)
        else:
            cleaned_event = dict(ev)
            cleaned_event["dates"] = future_dates if future_dates else valid_dates
            cleaned.append(cleaned_event)

        if event_warnings:
            vr.warnings.extend(event_warnings)

    # ── Fee validation ──
    for fee_str in result.fees if hasattr(result, 'fees') else []:
        fee_num = _extract_fee_number(fee_str)
        if fee_num is not None and (fee_num < MIN_FEE or fee_num > MAX_FEE):
            vr.warnings.append(f"Gebühr außerhalb Plausibilitätsbereich: {fee_str}")

    # ── Priority-based duplicate removal ──
    cleaned = _remove_duplicates_by_priority(cleaned)

    # ── Drop events where ALL dates are in the past ──
    cutoff = datetime.now() - timedelta(days=MAX_PAST_DAYS)
    final_cleaned = []
    for ev in cleaned:
        dates = ev.get("dates", [])
        if not dates:
            continue
        has_future = any(_parse_date(d) and _parse_date(d) >= cutoff for d in dates)
        if has_future:
            # Keep only future dates in the event
            ev["dates"] = [d for d in dates if _parse_date(d) and _parse_date(d) >= cutoff]
            final_cleaned.append(ev)
        else:
            vr.warnings.append(f"Event komplett vergangen, entfernt: {', '.join(dates)}")
    cleaned = final_cleaned

    # ── Change detection ──
    if previous_result:
        changes = _detect_changes(cleaned, previous_result)
        if changes:
            vr.warnings.extend(changes)

    vr.cleaned_events = cleaned
    vr.valid = len(vr.errors) == 0

    return vr


def validate_fees(fees: list) -> list:
    """Validate and clean fee list. Returns only plausible fees."""
    valid = []
    for fee_str in fees:
        num = _extract_fee_number(fee_str)
        if num is not None and MIN_FEE <= num <= MAX_FEE:
            valid.append(fee_str)
    return valid


def _parse_date(date_str: str):
    """Parse DD.MM.YYYY to datetime. Returns None if invalid."""
    try:
        return datetime.strptime(date_str, "%d.%m.%Y")
    except (ValueError, TypeError):
        return None


def _extract_fee_number(fee_str: str):
    import re
    match = re.search(r"(\d+)", str(fee_str))
    if match:
        return int(match.group(1))
    return None


def _get_source_priority(event: dict) -> int:
    """Get priority score for an event's source."""
    source = event.get("source", "")
    return SOURCE_PRIORITY.get(source, 0)


def _remove_duplicates_by_priority(events: list) -> list:
    """
    Remove duplicate events, keeping the highest-priority source.
    
    Aggressive dedup: an event is a duplicate if ANY of its dates
    already belongs to a kept event. This works because each real exam date
    should only appear in exactly one event.
    
    Table > PDF > Browser > List > Section > Text > LLM
    """
    if not events:
        return []

    # Sort by priority (highest first), then by info quality (more fields = better)
    def quality_score(ev):
        score = _get_source_priority(ev)
        # Bonus for having classified fields (these are valuable)
        if ev.get("schriftlich"): score += 2
        if ev.get("muendlich"): score += 2
        if ev.get("anmeldeschluss"): score += 2
        if ev.get("type") == "combined": score += 3
        # Penalty for unclassified events
        if ev.get("type") in ("unknown", None): score -= 3
        if ev.get("type") == "exam_date": score -= 1
        return score

    events_sorted = sorted(events, key=quality_score, reverse=True)

    kept = []
    used_dates = set()  # ALL individual dates already claimed

    for ev in events_sorted:
        ev_dates = set(ev.get("dates", []))
        if not ev_dates:
            continue

        # Core rule: if ANY date in this event is already claimed → skip
        overlap = ev_dates & used_dates
        if overlap:
            continue

        kept.append(ev)
        used_dates.update(ev_dates)

    # Re-sort chronologically
    def sort_key(e):
        try:
            return datetime.strptime(e["dates"][0], "%d.%m.%Y")
        except Exception:
            return datetime.max

    kept.sort(key=sort_key)
    return kept


def _detect_changes(new_events: list, previous_result) -> list:
    """Detect changes between new and previous results."""
    changes = []

    prev_events = []
    if hasattr(previous_result, 'exam_events'):
        prev_events = previous_result.exam_events
    elif isinstance(previous_result, dict):
        prev_events = previous_result.get("exam_events", [])

    new_dates = set()
    for ev in new_events:
        new_dates.update(ev.get("dates", []))

    old_dates = set()
    for ev in prev_events:
        if isinstance(ev, dict):
            old_dates.update(ev.get("dates", []))

    added = new_dates - old_dates
    removed = old_dates - new_dates

    if added:
        changes.append(f"NEUE Termine: {', '.join(sorted(added))}")
    if removed:
        changes.append(f"ENTFERNTE Termine: {', '.join(sorted(removed))}")

    return changes
