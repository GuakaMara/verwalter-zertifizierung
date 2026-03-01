"""
Parser A — Deterministic HTML Extraction (BeautifulSoup) v2

Core insight: A single exam event has MAX 3 dates:
  - schriftlich (written exam)
  - mündlich (oral exam)
  - Anmeldeschluss (registration deadline)

If a text block has >3 dates, it contains MULTIPLE events.
We group dates by temporal proximity (dates within 30 days = same exam cycle).

Multi-Strategy:
1. Tabellen mit Spaltenüberschriften-Erkennung (most reliable)
2. Sektionen unter relevanten Überschriften
3. Listen-Elemente mit Daten
4. Textblöcke mit Keyword + Datum
"""

import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from .models import ScrapeResult, ExamEvent


# ─── Date Patterns ──────────────────────────────────────────────────────────

PAT_NUMERIC = re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b")

MONTHS_DE = {
    "januar": "01", "februar": "02", "märz": "03", "maerz": "03",
    "april": "04", "mai": "05", "juni": "06",
    "juli": "07", "august": "08", "september": "09",
    "oktober": "10", "november": "11", "dezember": "12",
}

PAT_GERMAN = re.compile(
    r"\b(\d{1,2})\.\s*(" + "|".join(MONTHS_DE.keys()) + r")\s+(\d{4})\b",
    re.IGNORECASE
)

# German date range: "28. - 29. April 2026" (schr day 1, mdl day 2)
PAT_GERMAN_RANGE = re.compile(
    r"\b(\d{1,2})\.\s*[-–—]\s*(\d{1,2})\.\s*(" + "|".join(MONTHS_DE.keys()) + r")\s+(\d{4})\b",
    re.IGNORECASE
)

# Context patterns
CTX_SCHRIFTLICH = re.compile(r"schriftlich", re.IGNORECASE)
CTX_MUENDLICH = re.compile(r"m[üu]ndlich", re.IGNORECASE)
CTX_SAME_DAY = re.compile(r"schriftlich\w*/m[üu]ndlich|schriftlich\w?\s+und\s+m[üu]ndlich|schriftlich\w?\s+(?:sowie|&)\s+m[üu]ndlich", re.IGNORECASE)
CTX_DEADLINE = re.compile(r"anmelde(?:schluss|frist)|anmeldung\s+bis|(?:bis\s+(?:zum\s+)?)\d{1,2}\.", re.IGNORECASE)
CTX_ANMELDUNG_AB = re.compile(r"anmeldung\s+ab\s+(\d{1,2}\.\d{1,2}\.\d{4})", re.IGNORECASE)
CTX_FEE = re.compile(r"(\d{2,4}(?:[,.]\d{2})?)\s*(?:€|EUR|Euro)", re.IGNORECASE)
CTX_FEE_REV = re.compile(r"(?:€|EUR|Euro)\s*(\d{2,4}(?:[,.]\d{2})?)", re.IGNORECASE)
CTX_AUSGEBUCHT = re.compile(r"ausgebucht|belegt|voll|keine\s+(?:Anmeldung|Plätze)", re.IGNORECASE)
CTX_WARTELISTE = re.compile(r"warteliste|nachrücker", re.IGNORECASE)
CTX_ANMELDUNG_MOEGLICH = re.compile(r"anmeldung\s+(?:ab\s+\S+\s+)?möglich|plätze\s+frei|freie\s+plätze", re.IGNORECASE)

# Line-based pattern: "DD.MM.YYYY – schriftliche/mündliche Prüfung"
PAT_LINE_EXAM = re.compile(
    r"(\d{1,2}\.\d{1,2}\.\d{4})\s*[–—-]\s*(?:schriftlich|Prüfung|Gesamtprüfung|mündlich)",
    re.IGNORECASE
)

# Negative context: dates preceded by these are NOT exam dates
PAT_NOT_EXAM_DATE = re.compile(
    r"(?:Stand|Gültig\s+(?:ab|seit)|seit\s+dem|ab\s+dem|bis\s+zum|vom|Fassung|Version"
    r"|Anmeldung\s+ab|Anmeldung\s+(?:ist\s+)?ab"
    r"|gültig\s+seit|in\s+Kraft\s+seit|Dezember\s+20[12]\d|Juni\s+20[12]\d"
    r"|1\.\s+Dezember|1\.\s+Juni)"
    r"\s*:?\s*(\d{1,2}\.\d{1,2}\.\d{4})",
    re.IGNORECASE
)

EXAM_KEYWORDS = [
    "prüfung", "prüfungstermin", "termin", "schriftlich",
    "mündlich", "anmeldung", "anmeldefrist", "gebühr",
    "zertifizierter verwalter", "zertifizierter wohnimmobilienverwalter",
    "§ 26a", "sachkundenachweis"
]

# Column header patterns for table structure detection
COL_SCHRIFTLICH = re.compile(r"schriftlich|schriftl|written", re.IGNORECASE)
COL_MUENDLICH = re.compile(r"m[üu]ndlich|oral", re.IGNORECASE)
COL_DEADLINE = re.compile(r"anmelde|frist|deadline|schluss", re.IGNORECASE)
COL_DATE = re.compile(r"termin|datum|date|pr[üu]fung", re.IGNORECASE)
COL_STATUS = re.compile(r"status|verf[üu]gbar|plätze|anmerkung|hinweis", re.IGNORECASE)

# Max days between dates in the same exam cycle
MAX_CYCLE_GAP = 30


def normalize_date(day, month, year):
    return f"{int(day):02d}.{int(month):02d}.{year}"


def parse_date(date_str):
    """Parse DD.MM.YYYY to datetime."""
    try:
        return datetime.strptime(date_str, "%d.%m.%Y")
    except (ValueError, TypeError):
        return None


def extract_all_dates(text, filter_non_exam=False):
    dates = []
    seen = set()

    # Build set of non-exam dates if filtering is on
    non_exam_dates = set()
    if filter_non_exam:
        non_exam_dates = _find_non_exam_dates(text)

    for m in PAT_NUMERIC.finditer(text):
        day, month, year = m.group(1), m.group(2), m.group(3)
        if 1 <= int(month) <= 12 and 1 <= int(day) <= 31:
            d = normalize_date(day, month, year)
            if d not in seen:
                if filter_non_exam and d in non_exam_dates:
                    continue
                dates.append({"date": d, "year": year, "pos": m.start()})
                seen.add(d)

    for m in PAT_GERMAN.finditer(text):
        day = m.group(1)
        month_name = m.group(2).lower()
        year = m.group(3)
        month_num = MONTHS_DE.get(month_name)
        if month_num:
            d = normalize_date(day, month_num, year)
            if d not in seen:
                if filter_non_exam and d in non_exam_dates:
                    continue
                dates.append({"date": d, "year": year, "pos": m.start()})
                seen.add(d)

    # German date ranges: "28. - 29. April 2026" → two dates
    for m in PAT_GERMAN_RANGE.finditer(text):
        day1, day2 = m.group(1), m.group(2)
        month_name = m.group(3).lower()
        year = m.group(4)
        month_num = MONTHS_DE.get(month_name)
        if month_num:
            d1 = normalize_date(day1, month_num, year)
            d2 = normalize_date(day2, month_num, year)
            for d in (d1, d2):
                if d not in seen:
                    if filter_non_exam and d in non_exam_dates:
                        continue
                    dates.append({"date": d, "year": year, "pos": m.start()})
                    seen.add(d)

    dates.sort(key=lambda x: x["pos"])
    return dates


def _find_non_exam_dates(text):
    """
    Identify dates that appear in non-exam contexts:
    - "Stand: 19.01.2026" (page update date)
    - "Anmeldung ab 07.04.2026" (registration opens)
    - "seit dem 1. Dezember 2020" (law effective date)
    """
    non_exam = set()
    for m in PAT_NOT_EXAM_DATE.finditer(text):
        date_str = m.group(1)
        parts = date_str.split(".")
        if len(parts) == 3:
            try:
                non_exam.add(normalize_date(parts[0], parts[1], parts[2]))
            except (ValueError, IndexError):
                pass

    # Also catch "Anmeldung ab DD.MM.YYYY" via the specific pattern
    for m in CTX_ANMELDUNG_AB.finditer(text):
        date_str = m.group(1)
        parts = date_str.split(".")
        if len(parts) == 3:
            try:
                non_exam.add(normalize_date(parts[0], parts[1], parts[2]))
            except (ValueError, IndexError):
                pass

    return non_exam


def parse(html: str, ihk_id: str, url: str = "") -> ScrapeResult:
    """Parser A: Extract exam dates from static HTML."""
    soup = BeautifulSoup(html, "html.parser")
    full_text = soup.get_text(separator=" ", strip=True)

    result = ScrapeResult(ihk_id=ihk_id, stage="parser_a", url_used=url)

    # Keyword analysis
    text_lower = full_text.lower()
    kw_hits = sum(1 for kw in EXAM_KEYWORDS if kw in text_lower)
    result.keyword_score = f"{kw_hits}/{len(EXAM_KEYWORDS)}"

    # Fee extraction (both "430 €" and "€ 430" patterns)
    fees = list(set(CTX_FEE.findall(full_text) + CTX_FEE_REV.findall(full_text)))
    result.fees = [f"{f} €" for f in fees if int(f.split(",")[0].split(".")[0]) >= 50]

    # All dates
    all_dates = extract_all_dates(full_text)
    result.raw_dates_2026 = sorted(set(d["date"] for d in all_dates if d["year"] == "2026"))

    events = []

    # ── Strategy 0: Line-based extraction (for structured lists like Karlsruhe) ──
    line_events = _extract_from_lines(soup)
    events.extend(line_events)
    if line_events:
        result.strategies_used.append("line")

    # ── Strategy 1: Smart Table Extraction (highest priority) ──
    table_events = _extract_from_tables(soup)
    events.extend(table_events)
    if table_events:
        result.strategies_used.append("table")

    # ── Strategy 2: Section-based extraction ──
    section_events = _extract_from_sections(soup)
    for ev in section_events:
        if not _is_duplicate(ev, events):
            events.append(ev)
    if section_events:
        result.strategies_used.append("section")

    # ── Strategy 3: List items ──
    list_events = _extract_from_lists(soup)
    for ev in list_events:
        if not _is_duplicate(ev, events):
            events.append(ev)
    if list_events:
        result.strategies_used.append("list")

    # ── Strategy 4: Text blocks ──
    text_events = _extract_from_text_blocks(soup)
    for ev in text_events:
        if not _is_duplicate(ev, events):
            events.append(ev)
    if text_events:
        result.strategies_used.append("text_block")

    # Sort chronologically
    def sort_key(e):
        try:
            return parse_date(e.dates[0])
        except Exception:
            return datetime.max

    events.sort(key=sort_key)

    result.exam_events = events
    result.success = len(result.raw_dates_2026) > 0
    result.content_length = len(html)

    return result


# ─── Strategy 0: Line-based extraction ───────────────────────────────────────

def _extract_from_lines(soup):
    """
    Extract events from structured line-by-line text.

    Handles formats like:
      25.02.2026 – schriftliche/mündliche Prüfung - ausgebucht
      20.04.2026 – schriftliche/mündliche Prüfung
      Anmeldung ab 02.02.2026 möglich

    Each line with "DD.MM.YYYY – Prüfung" = one exam event.
    "Anmeldung ab" on the next line = registration opens for THAT event.
    """
    events = []
    full_text = soup.get_text(separator="\n", strip=True)
    lines = full_text.split("\n")

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Look for pattern: "DD.MM.YYYY – schriftliche/mündliche Prüfung"
        match = PAT_LINE_EXAM.search(line)
        if match:
            exam_date_str = match.group(1)
            # Normalize date
            date_parts = exam_date_str.split(".")
            if len(date_parts) == 3:
                exam_date = normalize_date(date_parts[0], date_parts[1], date_parts[2])
                year = date_parts[2]

                if year == "2026":
                    event = ExamEvent(dates=[exam_date])
                    event.source = "line"
                    event.evidence = line[:300]

                    # Is this same-day combined? (schriftliche/mündliche)
                    if CTX_SAME_DAY.search(line):
                        event.type = "combined"
                        event.schriftlich = exam_date
                        event.muendlich = exam_date
                    elif CTX_SCHRIFTLICH.search(line) and not CTX_MUENDLICH.search(line):
                        event.type = "schriftlich"
                        event.schriftlich = exam_date
                    elif CTX_MUENDLICH.search(line) and not CTX_SCHRIFTLICH.search(line):
                        event.type = "muendlich"
                        event.muendlich = exam_date
                    else:
                        event.type = "exam_date"

                    # Status from this line
                    _detect_status(event, line)

                    # Look ahead for "Anmeldung ab" on next line(s)
                    for j in range(1, 4):  # Check next 3 lines
                        if i + j < len(lines):
                            next_line = lines[i + j].strip()
                            ab_match = CTX_ANMELDUNG_AB.search(next_line)
                            if ab_match:
                                # "Anmeldung ab" = registration opens, store as metadata
                                # Not a deadline — it's when you CAN start registering
                                event.evidence += f" | {next_line}"
                                # Check for status in registration line
                                if CTX_ANMELDUNG_MOEGLICH.search(next_line):
                                    event.status = "anmeldung_moeglich"
                                break
                            # Stop lookahead if we hit another exam date
                            if PAT_LINE_EXAM.search(next_line):
                                break

                    events.append(event)

        i += 1

    return events


# ─── Strategy 1: Smart Table Extraction ─────────────────────────────────────

def _extract_from_tables(soup):
    """Extract events from HTML tables with column header detection."""
    events = []

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue

        col_map = _detect_table_columns(rows[0])

        for row in rows[1:] if col_map else rows:
            cells = row.find_all(["td", "th"])
            row_text = row.get_text(separator=" ", strip=True)
            row_dates = extract_all_dates(row_text)
            dates_2026 = [d for d in row_dates if d["year"] == "2026"]

            if not dates_2026:
                continue

            if col_map:
                event = _extract_with_columns(cells, col_map, row_text)
            else:
                # Table row = usually one event, even with >3 dates use proximity
                event = _build_single_event(row_text, dates_2026)

            event.source = "table"
            event.evidence = row_text[:300]
            _detect_status(event, row_text)
            events.append(event)

    return events


def _detect_table_columns(header_row):
    cells = header_row.find_all(["th", "td"])
    if not cells:
        return None

    col_map = {}
    for i, cell in enumerate(cells):
        text = cell.get_text(strip=True)
        if not text:
            continue

        if COL_SCHRIFTLICH.search(text):
            col_map["schriftlich"] = i
        elif COL_MUENDLICH.search(text):
            col_map["muendlich"] = i
        elif COL_DEADLINE.search(text):
            col_map["deadline"] = i
        elif COL_DATE.search(text) and "date" not in col_map:
            col_map["date"] = i
        elif COL_STATUS.search(text):
            col_map["status"] = i

    return col_map if col_map else None


def _extract_with_columns(cells, col_map, row_text):
    """Extract event data using known column positions."""
    event = ExamEvent(dates=[])
    all_dates = []

    for role, idx in col_map.items():
        if idx >= len(cells):
            continue
        cell_text = cells[idx].get_text(separator=" ", strip=True)
        cell_dates = extract_all_dates(cell_text)
        dates_2026 = [d for d in cell_dates if d["year"] == "2026"]

        if role == "schriftlich" and dates_2026:
            event.schriftlich = dates_2026[0]["date"]
            all_dates.extend(d["date"] for d in dates_2026)
        elif role == "muendlich" and dates_2026:
            event.muendlich = dates_2026[0]["date"]
            all_dates.extend(d["date"] for d in dates_2026)
        elif role == "deadline" and dates_2026:
            event.anmeldeschluss = dates_2026[0]["date"]
            all_dates.extend(d["date"] for d in dates_2026)
        elif role == "date" and dates_2026:
            all_dates.extend(d["date"] for d in dates_2026)
        elif role == "status":
            _detect_status(event, cell_text)

    event.dates = list(dict.fromkeys(all_dates))

    if event.schriftlich and event.muendlich:
        event.type = "combined"
    elif event.schriftlich:
        event.type = "schriftlich"
    elif event.muendlich:
        event.type = "muendlich"
    elif event.dates:
        event.type = "exam_date"

    return event


# ─── Strategy 2: Section-based extraction ────────────────────────────────────

def _extract_from_sections(soup):
    events = []
    keywords = ["termin", "prüfung", "datum", "nächst", "übersicht",
                "zeitplan", "veranstaltung", "sachkunde"]

    for heading in soup.find_all(["h1", "h2", "h3", "h4", "h5", "strong", "b"]):
        heading_text = heading.get_text(strip=True).lower()
        if not any(kw in heading_text for kw in keywords):
            continue

        content_parts = []
        sibling = heading.find_next_sibling()
        count = 0
        while sibling and count < 30:
            if sibling.name in ["h1", "h2", "h3", "h4"]:
                break
            content_parts.append(str(sibling))
            sibling = sibling.find_next_sibling()
            count += 1

        if not content_parts:
            # Even without siblings, heading itself may contain dates
            heading_full = heading.get_text(separator=" ", strip=True)
            heading_dates = extract_all_dates(heading_full, filter_non_exam=True)
            heading_dates_2026 = [d for d in heading_dates if d["year"] == "2026"]
            if heading_dates_2026:
                event = _build_single_event(heading_full, heading_dates_2026)
                event.source = "section"
                events.append(event)
            continue

        combined_html = "\n".join(content_parts)
        container = BeautifulSoup(combined_html, "html.parser")
        section_text = heading.get_text(separator=" ", strip=True) + " " + container.get_text(separator=" ", strip=True)
        section_dates = extract_all_dates(section_text, filter_non_exam=True)
        dates_2026 = [d for d in section_dates if d["year"] == "2026"]

        if not dates_2026:
            continue

        # KEY FIX: If section has >3 dates, group into exam cycles first
        if len(dates_2026) > 3:
            grouped = _group_dates_into_cycles(dates_2026)
            for group in grouped:
                group_text = _find_context_for_dates(section_text, group)
                event = _build_single_event(group_text, group)
                event.source = "section"
                events.append(event)
        else:
            # Small section = likely one event
            event = _build_single_event(section_text, dates_2026)
            event.source = "section"
            events.append(event)

    # Also look for inline patterns
    for pattern in [
        re.compile(r"schriftliche?\s+Prüfung", re.IGNORECASE),
        re.compile(r"Prüfungstermin", re.IGNORECASE),
        re.compile(r"nächste[rn]?\s+Termin", re.IGNORECASE),
    ]:
        for elem in soup.find_all(string=pattern):
            parent = elem.parent
            for _ in range(4):
                if parent and parent.name in ["div", "section", "article", "td", "li"]:
                    parent_text = parent.get_text(separator=" ", strip=True)
                    parent_dates = extract_all_dates(parent_text, filter_non_exam=True)
                    dates_2026 = [d for d in parent_dates if d["year"] == "2026"]
                    if dates_2026:
                        if len(dates_2026) > 3:
                            grouped = _group_dates_into_cycles(dates_2026)
                            for group in grouped:
                                ctx = _find_context_for_dates(parent_text, group)
                                event = _build_single_event(ctx, group)
                                event.source = "section"
                                if not _is_duplicate(event, events):
                                    events.append(event)
                        else:
                            event = _build_single_event(parent_text, dates_2026)
                            event.source = "section"
                            if not _is_duplicate(event, events):
                                events.append(event)
                    break
                if parent:
                    parent = parent.parent

    return events


# ─── Strategy 3: List items ──────────────────────────────────────────────────

def _extract_from_lists(soup):
    events = []
    for li in soup.find_all("li"):
        li_text = li.get_text(separator=" ", strip=True)
        li_dates = extract_all_dates(li_text, filter_non_exam=True)
        dates_2026 = [d for d in li_dates if d["year"] == "2026"]

        if dates_2026:
            if len(dates_2026) > 3:
                for group in _group_dates_into_cycles(dates_2026):
                    event = _build_single_event(li_text, group)
                    event.source = "list"
                    events.append(event)
            else:
                event = _build_single_event(li_text, dates_2026)
                event.source = "list"
                events.append(event)

    return events


# ─── Strategy 4: Text blocks ─────────────────────────────────────────────────

def _extract_from_text_blocks(soup):
    events = []
    for tag in soup.find_all(["p", "div", "span", "strong", "em", "b"]):
        if len(list(tag.children)) > 15:
            continue
        tag_text = tag.get_text(separator=" ", strip=True)
        if len(tag_text) > 1500 or len(tag_text) < 8:
            continue

        tag_dates = extract_all_dates(tag_text, filter_non_exam=True)
        dates_2026 = [d for d in tag_dates if d["year"] == "2026"]

        if dates_2026:
            tag_lower = tag_text.lower()
            if any(kw in tag_lower for kw in ["prüfung", "termin", "schriftlich",
                                               "mündlich", "anmelde", "sachkunde", "verwalter"]):
                if len(dates_2026) > 3:
                    for group in _group_dates_into_cycles(dates_2026):
                        ctx = _find_context_for_dates(tag_text, group)
                        event = _build_single_event(ctx, group)
                        event.source = "text_block"
                        events.append(event)
                else:
                    event = _build_single_event(tag_text, dates_2026)
                    event.source = "text_block"
                    events.append(event)

    return events


# ─── Core: Date Grouping ─────────────────────────────────────────────────────

def _group_dates_into_cycles(dates_2026):
    """
    Group dates into exam cycles. Dates within MAX_CYCLE_GAP days
    of each other belong to the same cycle.

    Key constraint: An exam cycle has MAX 3 dates (Frist + schriftlich + mündlich).
    If a group gets bigger, split at the largest internal gap.

    Returns list of groups, each group is a list of date dicts.
    """
    if not dates_2026:
        return []

    # Sort by actual date
    sorted_dates = sorted(dates_2026, key=lambda d: parse_date(d["date"]) or datetime.max)

    # Step 1: Initial grouping by temporal proximity
    groups = []
    current_group = [sorted_dates[0]]

    for i in range(1, len(sorted_dates)):
        prev_dt = parse_date(sorted_dates[i - 1]["date"])
        curr_dt = parse_date(sorted_dates[i]["date"])

        if prev_dt and curr_dt and (curr_dt - prev_dt).days <= MAX_CYCLE_GAP:
            current_group.append(sorted_dates[i])
        else:
            groups.append(current_group)
            current_group = [sorted_dates[i]]

    if current_group:
        groups.append(current_group)

    # Step 2: Split oversized groups (>3 dates) at largest internal gap
    final_groups = []
    for group in groups:
        if len(group) <= 3:
            final_groups.append(group)
        else:
            final_groups.extend(_split_oversized_group(group))

    return final_groups


def _split_oversized_group(group):
    """
    Split a group with >3 dates into sub-groups of max 3.
    Split at the largest date gap within the group.
    """
    if len(group) <= 3:
        return [group]

    # Find the largest gap
    gaps = []
    for i in range(1, len(group)):
        d1 = parse_date(group[i - 1]["date"])
        d2 = parse_date(group[i]["date"])
        if d1 and d2:
            gaps.append((i, (d2 - d1).days))
        else:
            gaps.append((i, 0))

    # Sort by gap size, split at largest
    gaps.sort(key=lambda x: x[1], reverse=True)
    split_idx = gaps[0][0]

    left = group[:split_idx]
    right = group[split_idx:]

    # Recursively split if still too big
    result = []
    result.extend(_split_oversized_group(left) if len(left) > 3 else [left])
    result.extend(_split_oversized_group(right) if len(right) > 3 else [right])

    return result


def _find_context_for_dates(full_text, date_group):
    """
    Extract the relevant text context around a group of dates.
    Returns a substring of full_text centered around the dates.
    """
    if not date_group:
        return full_text

    # Find positions of dates in text
    positions = [d["pos"] for d in date_group if "pos" in d]
    if not positions:
        return full_text

    start = max(0, min(positions) - 100)
    end = min(len(full_text), max(positions) + 100)

    return full_text[start:end]


# ─── Core: Smart Event Builder ───────────────────────────────────────────────

def _build_single_event(text, dates_2026):
    """
    Build a single ExamEvent from text and dates.
    
    Order of operations:
    1. Identify deadline date first (remove from pool)
    2. Assign schriftlich/mündlich from remaining dates by proximity
    """
    event = ExamEvent(
        dates=[d["date"] for d in dates_2026],
        evidence=text[:300],
    )

    # Find keyword positions in text
    same_day = bool(CTX_SAME_DAY.search(text))
    schrift_pos = _find_keyword_pos(text, CTX_SCHRIFTLICH)
    muend_pos = _find_keyword_pos(text, CTX_MUENDLICH)
    deadline_pos = _find_keyword_pos(text, CTX_DEADLINE)
    anmeldung_ab = CTX_ANMELDUNG_AB.search(text)

    has_schrift = schrift_pos is not None
    has_muend = muend_pos is not None
    has_deadline = deadline_pos is not None

    # Step 0a: "mündliche Prüfung findet am Folgetag statt" + 2 dates = combined
    folgetag = bool(re.search(r"m[üu]ndlich\w*\s+(?:Prüfung\s+)?(?:findet\s+)?(?:am\s+)?Folgetag", text, re.IGNORECASE))
    if folgetag and len(dates_2026) >= 2:
        sorted_d = sorted(dates_2026, key=lambda d: parse_date(d["date"]) or datetime.max)
        d1 = parse_date(sorted_d[0]["date"])
        d2 = parse_date(sorted_d[1]["date"])
        if d1 and d2 and (d2 - d1).days == 1:
            event.type = "combined"
            event.schriftlich = sorted_d[0]["date"]
            event.muendlich = sorted_d[1]["date"]
            event.dates = [sorted_d[0]["date"], sorted_d[1]["date"]]
            _detect_status(event, text)
            return event

    # Step 0b: Check for same-day combined (e.g. "schriftliche/mündliche Prüfung")
    if same_day and len(dates_2026) >= 1:
        # Both exam parts on the same day
        # Filter out "Anmeldung ab" dates from exam dates
        exam_dates = dates_2026
        if anmeldung_ab:
            ab_date = anmeldung_ab.group(1)
            ab_normalized = normalize_date(*ab_date.split("."))
            exam_dates = [d for d in dates_2026 if d["date"] != ab_normalized]
            if not exam_dates:
                exam_dates = dates_2026

        event.type = "combined"
        event.schriftlich = exam_dates[0]["date"]
        event.muendlich = exam_dates[0]["date"]
        event.dates = [exam_dates[0]["date"]]
        _detect_status(event, text)
        return event

    # Step 1: Identify deadline date first and remove from pool
    remaining_dates = list(dates_2026)
    
    if has_deadline and len(dates_2026) >= 2:
        deadline_date_str = _closest_date_to_pos(dates_2026, deadline_pos)
        if deadline_date_str:
            # Verify it's chronologically before other dates (deadlines are always earlier)
            deadline_dt = parse_date(deadline_date_str)
            other_dates = [d for d in dates_2026 if d["date"] != deadline_date_str]
            if deadline_dt and other_dates:
                other_min = min(parse_date(d["date"]) or datetime.max for d in other_dates)
                if deadline_dt < other_min:
                    event.anmeldeschluss = deadline_date_str
                    remaining_dates = [d for d in dates_2026 if d["date"] != deadline_date_str]

    # Step 2: Assign schriftlich/mündlich from remaining dates
    if has_schrift and has_muend and len(remaining_dates) >= 2:
        event.type = "combined"
        event.schriftlich = _closest_date_to_pos(remaining_dates, schrift_pos)
        event.muendlich = _closest_date_to_pos(remaining_dates, muend_pos)

        # If they got the same date, use chronological order
        if event.schriftlich == event.muendlich:
            sorted_d = sorted(remaining_dates, key=lambda d: parse_date(d["date"]) or datetime.max)
            if len(sorted_d) >= 2:
                event.schriftlich = sorted_d[0]["date"]
                event.muendlich = sorted_d[1]["date"]
            else:
                event.muendlich = None
                event.type = "schriftlich"

    elif has_schrift:
        event.type = "schriftlich"
        event.schriftlich = _closest_date_to_pos(remaining_dates, schrift_pos)

    elif has_muend:
        event.type = "muendlich"
        event.muendlich = _closest_date_to_pos(remaining_dates, muend_pos)

    elif len(remaining_dates) == 2:
        # No keywords but 2 dates — likely schriftlich + mündlich in order
        sorted_d = sorted(remaining_dates, key=lambda d: parse_date(d["date"]) or datetime.max)
        d1 = parse_date(sorted_d[0]["date"])
        d2 = parse_date(sorted_d[1]["date"])
        if d1 and d2 and 0 < (d2 - d1).days <= MAX_CYCLE_GAP:
            event.type = "combined"
            event.schriftlich = sorted_d[0]["date"]
            event.muendlich = sorted_d[1]["date"]
        else:
            event.type = "exam_date"

    elif len(remaining_dates) == 1 and not has_schrift and not has_muend:
        event.type = "exam_date"

    elif "termin" in text.lower() or "prüfung" in text.lower():
        event.type = "exam_date"
    else:
        event.type = "unknown"

    # Status detection
    _detect_status(event, text)

    return event


def _find_keyword_pos(text, pattern):
    """Find the position of the first match of a pattern."""
    m = pattern.search(text)
    return m.start() if m else None


def _closest_date_to_pos(dates, pos):
    """Find the date closest to a text position."""
    if pos is None or not dates:
        return None
    closest = min(dates, key=lambda d: abs(d.get("pos", 9999) - pos))
    return closest["date"]


def _detect_status(event, text):
    """Detect booking status from text."""
    if CTX_AUSGEBUCHT.search(text):
        event.status = "ausgebucht"
    elif CTX_WARTELISTE.search(text):
        event.status = "warteliste"
    elif CTX_ANMELDUNG_MOEGLICH.search(text):
        event.status = "anmeldung_moeglich"


def _is_duplicate(new_event, existing_events):
    """Check if new event overlaps significantly with existing ones."""
    new_dates = set(new_event.dates if isinstance(new_event, ExamEvent) else new_event.get("dates", []))
    if not new_dates:
        return True
    for existing in existing_events:
        existing_dates = set(existing.dates if isinstance(existing, ExamEvent) else existing.get("dates", []))
        if new_dates.issubset(existing_dates):
            return True
        if len(new_dates) > 0 and len(new_dates & existing_dates) / len(new_dates) >= 0.7:
            return True
    return False
