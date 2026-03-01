"""
Parser C — Playwright Browser Rendering

Für IHKs mit JavaScript-Formularen (z.B. Freiburg "Lux"-Portal, Hamburg, Kassel).
- Startet headless Browser
- Wartet auf network idle
- Navigiert durch mehrstufige Formulare (Lux-Portal)
- Extrahiert gerenderten DOM
- Lässt dann Parser A über den gerenderten HTML laufen
- Fallback: Eigene Datumsextraktion aus Lux-Terminliste

Benötigt: pip3 install playwright && python3 -m playwright install chromium
"""

import re
from .models import ScrapeResult, ExamEvent
from . import parser_a

# German month names for parsing "7. Oktober 2026" format
GERMAN_MONTHS = {
    "januar": 1, "februar": 2, "märz": 3, "april": 4,
    "mai": 5, "juni": 6, "juli": 7, "august": 8,
    "september": 9, "oktober": 10, "november": 11, "dezember": 12,
}


def parse(url: str, ihk_id: str, wait_seconds: int = 5, verbose: bool = True, lux_url: str = None) -> ScrapeResult:
    """
    Render page with Playwright, then run Parser A on rendered DOM.
    Falls back to Lux-specific date extraction if Parser A finds nothing.
    """
    result = ScrapeResult(ihk_id=ihk_id, stage="parser_c", url_used=url)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        result.error = "playwright nicht installiert (pip3 install playwright && python3 -m playwright install chromium)"
        return result

    rendered_html = None
    lux_navigated = False

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                locale="de-DE",
            )
            page = context.new_page()

            # Step 1: Try loading main page (may timeout — that's OK if we have lux_url)
            main_page_loaded = False
            try:
                page.goto(url, wait_until="networkidle", timeout=30000)
                page.wait_for_timeout(wait_seconds * 1000)
                main_page_loaded = True
            except Exception as e:
                if verbose:
                    print(f"   ⚠️  Hauptseite Timeout — versuche Alternativen...")

            # Step 2: If main page loaded, try Lux detection (iframes, page content)
            lux_navigated = False
            if main_page_loaded:
                # Auto-discover lux_url from page HTML if not provided
                if not lux_url:
                    lux_url = _auto_discover_lux_url(page, verbose)

                # Debug output
                if verbose:
                    try:
                        rendered_html = page.content()
                        page_text = page.inner_text("body")
                        debug_path = f"data/logs/debug_browser_{ihk_id}.txt"
                        with open(debug_path, "w", encoding="utf-8") as f:
                            f.write(f"=== URL: {url} ===\n")
                            f.write(f"=== PAGE TEXT ({len(page_text)} chars) ===\n")
                            f.write(page_text[:5000])
                            f.write(f"\n\n=== IFRAMES ===\n")
                            for i, frame in enumerate(page.frames):
                                f.write(f"Frame {i}: {frame.url}\n")
                                try:
                                    f.write(f"  Text: {frame.inner_text('body')[:1000]}\n")
                                except Exception:
                                    f.write(f"  Text: (error)\n")
                        print(f"   📝 Debug: {debug_path}")
                    except Exception:
                        pass

                # Try Lux in iframes on main page
                lux_navigated = _try_lux_portal(page, verbose)

            # Step 3: If no Lux found yet, navigate directly to lux_url
            if not lux_navigated and lux_url:
                if verbose:
                    print(f"   🔗 Navigiere direkt zu Lux-URL...")
                try:
                    page.goto(lux_url, wait_until="networkidle", timeout=30000)
                    page.wait_for_timeout(wait_seconds * 1000)
                    lux_navigated = _try_lux_portal(page, verbose)
                except Exception as e:
                    if verbose:
                        print(f"   ⚠️  Lux-URL Timeout: {str(e)[:80]}")

            # Step 4: Collect all rendered HTML (main page + iframes)
            rendered_html = page.content()
            for frame in page.frames:
                if frame != page.main_frame:
                    try:
                        frame_html = frame.content()
                        rendered_html += "\n<!-- IFRAME: " + frame.url + " -->\n" + frame_html
                    except Exception:
                        pass

            result.content_length = len(rendered_html)

            browser.close()

    except Exception as e:
        result.error = f"Playwright-Fehler: {str(e)[:200]}"
        return result

    if not rendered_html:
        result.error = "Kein HTML nach Browser-Rendering"
        return result

    # Always run both extractors, then merge
    parser_a_result = parser_a.parse(rendered_html, ihk_id, url)
    lux_events = _extract_lux_dates(rendered_html, verbose) if lux_navigated else []

    # Merge: Lux has precise schr/mdl, Parser A has fees/keywords/status
    if lux_events:
        # Lux-Events als primäre Quelle (korrekte schr/mdl-Zuordnung)
        result.exam_events = lux_events
        result.raw_dates_2026 = list(set(d for e in lux_events for d in _event_dates(e) if "2026" in d))
        result.strategies_used = ["browser+lux_dates"]
        result.success = True

        # Check: Hat Parser A zusätzliche Events die Lux nicht hat?
        if parser_a_result.exam_events:
            lux_date_set = set()
            for e in lux_events:
                for d in _event_dates(e):
                    lux_date_set.add(d)

            extra_events = []
            for e in parser_a_result.exam_events:
                e_dates = set(_event_dates(e))
                if not e_dates & lux_date_set:  # kein Overlap mit Lux
                    extra_events.append(e)

            if extra_events:
                result.exam_events.extend(extra_events)
                result.strategies_used.append("browser+parser_a_extra")
                if verbose:
                    print(f"   ℹ️  Parser A: {len(extra_events)} zusätzliche Events ergänzt")

    elif parser_a_result.success and parser_a_result.exam_events:
        # Kein Lux oder Lux leer → Parser A komplett übernehmen
        result.exam_events = parser_a_result.exam_events
        result.raw_dates_2026 = parser_a_result.raw_dates_2026
        result.strategies_used = [f"browser+{s}" for s in parser_a_result.strategies_used]
        result.success = True

    # Metadaten immer von Parser A (fees, keyword_score)
    result.keyword_score = parser_a_result.keyword_score
    result.fees = parser_a_result.fees or (_extract_fees(rendered_html) if lux_events else [])

    # Mark events as browser-sourced
    for event in result.exam_events:
        if hasattr(event, 'source') and event.source and not event.source.startswith("browser_"):
            event.source = f"browser_{event.source}"

    return result


def _auto_discover_lux_url(page, verbose=True) -> str:
    """
    Scan page HTML and iframes for eoa2.bildung1.gfi.ihk.de Lux portal URLs.
    Returns the URL if found, None otherwise.
    """
    try:
        html = page.content()

        # Look for Lux portal URLs in page HTML (iframes, links, scripts)
        lux_match = re.search(
            r'(https?://eoa2\.bildung1\.gfi\.ihk\.de/kammer/[^"\'<>\s]+)',
            html
        )
        if lux_match:
            lux_url = lux_match.group(1)
            if verbose:
                print(f"   🔍 Auto-Discovery: Lux-URL gefunden: {lux_url[:80]}")
            return lux_url

        # Also check iframe URLs directly
        for frame in page.frames:
            if "eoa2.bildung1.gfi.ihk.de" in frame.url:
                if verbose:
                    print(f"   🔍 Auto-Discovery: Lux-iframe gefunden: {frame.url[:80]}")
                return frame.url

    except Exception:
        pass

    return None


def _try_lux_portal(page, verbose=True) -> bool:
    """
    Navigate through a Lux booking portal: Variantenauswahl -> Terminauswahl.
    Returns True if navigation was attempted.
    """
    try:
        page_text = page.inner_text("body")

        # Detection: Look for stepper indicators in main page
        is_lux = any(kw in page_text for kw in [
            "Variantenauswahl", "Terminauswahl", "Gebührenbescheidempfänger",
            "Prüfungsvariante", "Prüfungsumfang",  # Stuttgart variant
        ])

        # Also check iframes for Lux content
        target_frame = page
        if not is_lux:
            for frame in page.frames:
                if frame == page.main_frame:
                    continue
                try:
                    ft = frame.inner_text("body")
                    if any(kw in ft for kw in ["Variantenauswahl", "Terminauswahl", "Gebührenbescheidempfänger", "Prüfungsvariante", "Prüfungsumfang"]):
                        is_lux = True
                        target_frame = frame
                        if verbose:
                            print(f"   🔍 Lux-Portal in iframe gefunden: {frame.url[:80]}")
                        break
                except Exception:
                    continue

        if not is_lux:
            if verbose:
                print("   ℹ️  Kein Lux-Portal erkannt")
            return False

        if verbose and target_frame == page:
            print("   🔍 Lux-Portal erkannt — navigiere...")

        # Step 1: Click "Gesamtprüfung" option
        clicked_option = False

        # Try label with "Gesamtprüfung"
        for text in ["Gesamtprüfung", "schriftlich + mündlich", "schriftlich und mündlich"]:
            try:
                loc = target_frame.locator(f"label:has-text('{text}')").first
                if loc.is_visible():
                    loc.click()
                    clicked_option = True
                    if verbose:
                        print(f"   ✅ '{text}' angeklickt")
                    break
            except Exception:
                continue

        # Fallback: click first radio button
        if not clicked_option:
            try:
                radio = target_frame.locator("input[type='radio']").first
                if radio.is_visible():
                    radio.click()
                    clicked_option = True
                    if verbose:
                        print("   ✅ Erstes Radio-Button angeklickt")
            except Exception:
                pass

        if not clicked_option:
            if verbose:
                print("   ⚠️  Keine Variante zum Anklicken gefunden")
            return False

        page.wait_for_timeout(2000)

        # Step 2: Click "Weiter"
        clicked_weiter = False
        for selector in [
            "button:has-text('Weiter')",
            "a:has-text('Weiter')",
            "input[value='Weiter']",
            ".btn:has-text('Weiter')",
            "[type='submit']",
        ]:
            try:
                btn = target_frame.locator(selector).first
                if btn.is_visible():
                    btn.click()
                    clicked_weiter = True
                    if verbose:
                        print("   ✅ 'Weiter' angeklickt")
                    break
            except Exception:
                continue

        if not clicked_weiter:
            if verbose:
                print("   ⚠️  'Weiter' Button nicht gefunden")
            return False

        # Wait for Terminauswahl to load
        page.wait_for_timeout(4000)

        if verbose:
            new_text = page.inner_text("body")
            month_names = '|'.join(GERMAN_MONTHS.keys())
            has_dates = bool(re.search(
                rf'\d{{1,2}}\.\s*(?:{month_names})\s*\d{{4}}', new_text, re.IGNORECASE
            ))
            print(f"   ℹ️  Nach Navigation: {'Termine sichtbar ✅' if has_dates else 'Prüfe Daten...'}")

        return True

    except Exception as e:
        if verbose:
            print(f"   ⚠️  Lux-Navigation fehlgeschlagen: {str(e)[:100]}")
        return False


def _extract_lux_dates(html: str, verbose: bool = True) -> list:
    """
    Extract exam dates from Lux portal Terminauswahl.

    Patterns:
    - "Mittwoch, 7. Oktober 2026 (schriftlich in Offenburg | mündlich 08.10.2026 in Freiburg)"
    - "Donnerstag, 5. März 2026" (standalone)
    """
    events = []
    month_names = '|'.join(GERMAN_MONTHS.keys())

    # Pattern 1: Full format with schriftlich/mündlich
    pattern_full = re.compile(
        r'(?:Montag|Dienstag|Mittwoch|Donnerstag|Freitag|Samstag|Sonntag),?\s*'
        r'(\d{1,2})\.\s*(' + month_names + r')\s*(\d{4})'
        r'\s*\(schriftlich[^|]*\|\s*m[uü]ndlich\s+(\d{2}\.\d{2}\.\d{4})',
        re.IGNORECASE
    )

    for m in pattern_full.finditer(html):
        day, month_name, year = m.group(1), m.group(2).lower(), m.group(3)
        mdl_date_str = m.group(4)

        month = GERMAN_MONTHS.get(month_name)
        if not month or year != "2026":
            continue

        schr_date = f"{int(day):02d}.{month:02d}.{year}"

        event = ExamEvent(
            dates=[schr_date, mdl_date_str],
            type="combined",
            schriftlich=schr_date,
            muendlich=mdl_date_str,
            source="lux_portal",
        )
        events.append(event)

    # Pattern 2: Simple dates (no mündlich info)
    if not events:
        pattern_simple = re.compile(
            r'(?:Montag|Dienstag|Mittwoch|Donnerstag|Freitag|Samstag|Sonntag),?\s*'
            r'(\d{1,2})\.\s*(' + month_names + r')\s*(\d{4})',
            re.IGNORECASE
        )

        for m in pattern_simple.finditer(html):
            day, month_name, year = m.group(1), m.group(2).lower(), m.group(3)
            month = GERMAN_MONTHS.get(month_name)
            if not month or year != "2026":
                continue

            date_str = f"{int(day):02d}.{month:02d}.{year}"
            event = ExamEvent(
                dates=[date_str],
                type="exam_date",
                schriftlich=date_str,
                source="lux_portal",
            )
            events.append(event)

    # Deduplicate (same dates can appear in main page + iframe)
    seen = set()
    unique_events = []
    for e in events:
        key = (e.schriftlich, e.muendlich)
        if key not in seen:
            seen.add(key)
            unique_events.append(e)
    events = unique_events

    if verbose and events:
        print(f"   🎯 Lux-Extraktion: {len(events)} Termine gefunden")

    return events


def _event_dates(event) -> list:
    dates = []
    for attr in ('dates', 'schriftlich', 'muendlich'):
        val = getattr(event, attr, None)
        if val:
            if isinstance(val, list):
                dates.extend(val)
            else:
                dates.append(val)
    return dates


def _extract_fees(html: str) -> list:
    fees = []
    for m in re.finditer(r'(\d{1,3}(?:[.,]\d{2})?)\s*€', html):
        fee = m.group(1).replace(',', '.')
        try:
            val = float(fee)
            if 20 <= val <= 1000:
                fee_str = f"{int(val)} €" if val == int(val) else f"{val} €"
                if fee_str not in fees:
                    fees.append(fee_str)
        except ValueError:
            pass
    return fees


def is_available() -> bool:
    """Check if Playwright is installed and ready."""
    try:
        from playwright.sync_api import sync_playwright
        return True
    except ImportError:
        return False
