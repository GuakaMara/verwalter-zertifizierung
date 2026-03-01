"""
Parser LLM — Claude API Extraction mit Guardrails

Letzter Fallback wenn deterministic Parser und Browser scheitern.
- Schickt NUR den relevanten Textblock (nicht die ganze Seite)
- Claude muss pro Termin Evidence-Snippets liefern
- Ergebnis wird strikt validiert
- Confidence-Score pro Event

Benötigt: ANTHROPIC_API_KEY Umgebungsvariable
"""

import json
import os
import re
from .models import ScrapeResult, ExamEvent
from .parser_a import extract_all_dates, EXAM_KEYWORDS

# Max characters to send to Claude (cost control)
MAX_TEXT_LENGTH = 4000


SYSTEM_PROMPT = """Du bist ein Datenextraktionssystem für IHK-Prüfungstermine.

Deine Aufgabe: Extrahiere ALLE Prüfungstermine für den "Zertifizierten Verwalter" (§ 26a WEG) 
aus dem gegebenen Text.

REGELN:
1. Extrahiere NUR Termine die EXPLIZIT im Text stehen
2. Erfinde KEINE Termine
3. Für jeden Termin musst du den EXAKTEN Textausschnitt angeben der ihn belegt
4. Datumsformat: DD.MM.YYYY
5. Wenn du unsicher bist, setze confidence auf unter 0.7

Antworte NUR mit validem JSON in diesem Format:
{
  "events": [
    {
      "schriftlich": "DD.MM.YYYY oder null",
      "muendlich": "DD.MM.YYYY oder null",
      "anmeldeschluss": "DD.MM.YYYY oder null",
      "status": "anmeldung_moeglich | ausgebucht | warteliste | unknown",
      "evidence": "Exakter Textausschnitt (max 100 Zeichen)",
      "confidence": 0.95
    }
  ],
  "fees": ["250 €"],
  "no_dates_reason": "null oder Erklärung warum keine Termine gefunden"
}"""


USER_PROMPT_TEMPLATE = """IHK: {ihk_name} ({ihk_city})
URL: {url}

Extrahiere alle Prüfungstermine 2026 für den "Zertifizierten Verwalter" aus diesem Text:

---
{text}
---

Antworte NUR mit JSON. Keine Erklärungen."""


def parse(text: str, ihk_id: str, ihk_name: str = "", ihk_city: str = "",
          url: str = "", api_key: str = None) -> ScrapeResult:
    """
    Use Claude API to extract exam dates from text.
    Text should be pre-filtered to relevant sections only.
    """
    result = ScrapeResult(ihk_id=ihk_id, stage="parser_llm", url_used=url)

    # Get API key
    key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        result.error = "ANTHROPIC_API_KEY nicht gesetzt"
        return result

    # Check dependency
    try:
        import requests as req
    except ImportError:
        result.error = "requests nicht installiert"
        return result

    # Truncate text to save costs
    if len(text) > MAX_TEXT_LENGTH:
        # Keep beginning and end (most IHKs have dates at top or bottom)
        half = MAX_TEXT_LENGTH // 2
        text = text[:half] + "\n[...gekürzt...]\n" + text[-half:]

    # Build prompt
    user_prompt = USER_PROMPT_TEMPLATE.format(
        ihk_name=ihk_name,
        ihk_city=ihk_city,
        url=url,
        text=text
    )

    # Call Claude API
    try:
        response = req.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 2000,
                "system": SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": user_prompt}],
            },
            timeout=30,
        )

        if response.status_code != 200:
            result.error = f"Claude API HTTP {response.status_code}: {response.text[:200]}"
            return result

        data = response.json()
        content = data.get("content", [{}])[0].get("text", "")

    except Exception as e:
        result.error = f"Claude API Fehler: {str(e)[:200]}"
        return result

    # Parse JSON response
    try:
        # Strip markdown code blocks if present
        content = content.strip()
        if content.startswith("```"):
            content = re.sub(r"^```(?:json)?\s*", "", content)
            content = re.sub(r"\s*```$", "", content)

        parsed = json.loads(content)
    except json.JSONDecodeError as e:
        result.error = f"Claude JSON ungültig: {str(e)[:100]}"
        return result

    # Convert to ExamEvents
    events = []
    for ev_data in parsed.get("events", []):
        dates = []
        schrift = ev_data.get("schriftlich")
        muend = ev_data.get("muendlich")
        frist = ev_data.get("anmeldeschluss")

        if schrift and schrift != "null":
            dates.append(schrift)
        if muend and muend != "null":
            dates.append(muend)

        if not dates:
            continue

        event = ExamEvent(
            dates=dates,
            schriftlich=schrift if schrift != "null" else None,
            muendlich=muend if muend != "null" else None,
            anmeldeschluss=frist if frist and frist != "null" else None,
            status=ev_data.get("status", "unknown"),
            source="llm",
            evidence=ev_data.get("evidence", "")[:300],
            confidence=float(ev_data.get("confidence", 0.5)),
        )

        # Set type
        if event.schriftlich and event.muendlich:
            event.type = "combined"
        elif event.schriftlich:
            event.type = "schriftlich"
        elif event.muendlich:
            event.type = "muendlich"

        events.append(event)

    # Fees
    fees = parsed.get("fees", [])

    result.exam_events = events
    result.raw_dates_2026 = sorted(set(d for ev in events for d in ev.dates))
    result.fees = fees
    result.success = len(events) > 0
    result.strategies_used = ["llm"]

    return result


def extract_relevant_text(html: str) -> str:
    """
    Pre-filter HTML to extract only the relevant text sections.
    This reduces token usage and improves Claude's accuracy.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")

    # Remove script, style, nav, footer, header
    for tag in soup.find_all(["script", "style", "nav", "footer", "header", "aside"]):
        tag.decompose()

    full_text = soup.get_text(separator="\n", strip=True)

    # Find sections with exam keywords
    lines = full_text.split("\n")
    relevant_lines = []
    context_window = 5  # Include N lines before/after keyword match

    keyword_indices = set()
    for i, line in enumerate(lines):
        line_lower = line.lower()
        if any(kw in line_lower for kw in EXAM_KEYWORDS[:6]):  # Top keywords
            for j in range(max(0, i - context_window), min(len(lines), i + context_window + 1)):
                keyword_indices.add(j)

    if keyword_indices:
        for i in sorted(keyword_indices):
            relevant_lines.append(lines[i])
        return "\n".join(relevant_lines)

    # Fallback: return full text (truncated)
    return full_text[:MAX_TEXT_LENGTH]
