"""
Parser C — Playwright Browser Rendering

Für IHKs mit JavaScript-Formularen (z.B. Stuttgart, Freiburg "Lux"-Portal).
- Startet headless Browser
- Wartet auf network idle
- Extrahiert gerenderten DOM
- Lässt dann Parser A über den gerenderten HTML laufen

Benötigt: pip3 install playwright && python3 -m playwright install chromium
"""

from .models import ScrapeResult
from . import parser_a


def parse(url: str, ihk_id: str, wait_seconds: int = 5) -> ScrapeResult:
    """
    Render page with Playwright, then run Parser A on rendered DOM.
    """
    result = ScrapeResult(ihk_id=ihk_id, stage="parser_c", url_used=url)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        result.error = "playwright nicht installiert (pip3 install playwright && python3 -m playwright install chromium)"
        return result

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Verwalter-Zertifizierung.de/1.0 (+https://verwalter-zertifizierung.de)",
                locale="de-DE",
            )
            page = context.new_page()

            # Navigate and wait for content
            page.goto(url, wait_until="networkidle", timeout=30000)

            # Extra wait for JS frameworks to render
            page.wait_for_timeout(wait_seconds * 1000)

            # Try clicking through form steps if it's a Lux portal
            rendered_html = _try_lux_portal(page, url)

            if not rendered_html:
                rendered_html = page.content()

            result.content_length = len(rendered_html)

            browser.close()

    except Exception as e:
        result.error = f"Playwright-Fehler: {str(e)[:200]}"
        return result

    # Run Parser A on the rendered HTML
    parser_a_result = parser_a.parse(rendered_html, ihk_id, url)

    # Transfer results
    result.exam_events = parser_a_result.exam_events
    result.raw_dates_2026 = parser_a_result.raw_dates_2026
    result.fees = parser_a_result.fees
    result.keyword_score = parser_a_result.keyword_score
    result.strategies_used = [f"browser+{s}" for s in parser_a_result.strategies_used]
    result.success = parser_a_result.success

    # Mark events as browser-sourced
    for event in result.exam_events:
        if hasattr(event, 'source'):
            event.source = f"browser_{event.source}"

    return result


def _try_lux_portal(page, url):
    """
    Try to navigate through a Lux booking portal to extract available dates.
    These portals have a multi-step form: Variantenauswahl → Terminauswahl → ...
    """
    try:
        # Check if this is a Lux portal
        if not page.query_selector("[class*='lux'], [class*='variant'], [data-step]"):
            return None

        # Step 1: Look for exam type selection (Gesamtprüfung)
        gesamt_option = page.query_selector(
            "text=Gesamtprüfung, text=schriftlich + mündlich, "
            "input[value*='gesamt'], label:has-text('Gesamtprüfung')"
        )
        if gesamt_option:
            gesamt_option.click()
            page.wait_for_timeout(2000)

        # Step 2: Look for "Weiter" / "Next" button
        weiter_btn = page.query_selector(
            "button:has-text('Weiter'), button:has-text('weiter'), "
            "a:has-text('Weiter'), input[value='Weiter']"
        )
        if weiter_btn:
            weiter_btn.click()
            page.wait_for_timeout(3000)

        # Now we should be on Terminauswahl — get the full page
        return page.content()

    except Exception:
        # If Lux-specific navigation fails, just return regular content
        return page.content()


def is_available() -> bool:
    """Check if Playwright is installed and ready."""
    try:
        from playwright.sync_api import sync_playwright
        return True
    except ImportError:
        return False
