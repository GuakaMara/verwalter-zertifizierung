"""
Pipeline — Orchestrator für die Fallback-Kette

Pro IHK läuft diese Pipeline:

  Stufe 0: Preflight (Cache-Check, Skip wenn unverändert)
  Stufe 1: Parser A (BeautifulSoup, deterministic)
  Stufe 2: Parser C (Playwright, Browser-Render → Parser A)
  Stufe 3: Source Discovery (neue URL suchen)
  Stufe 4: Parser LLM (Claude API mit Guardrails)
  Stufe 5: Safe Degrade (alte Daten behalten + Alert)

Jedes Ergebnis durchläuft den Validator.
"""

import json
import time
from datetime import datetime
from pathlib import Path

from .models import ScrapeResult, ExamEvent
from . import parser_a
from . import parser_b
from . import parser_c
from . import parser_llm
from . import source_discovery
from . import validator
from . import cache
from . import alert


# Availability labels
AVAIL_LABELS = {
    "online": "✅ Online-Termine",
    "online_js": "🔧 JS-Formular",
    "online_pdf": "📄 PDF-Dokument",
    "on_request": "📞 Nur auf Anfrage",
    "not_offered": "❌ Nicht angeboten",
    "refers_to": "↗️  Verweist auf andere IHK",
}


def run_pipeline(registry: list, options: dict = None) -> list:
    """
    Run the full scraper pipeline for all IHKs in the registry.

    Options:
    - skip_cache: bool — ignore cache, always fetch fresh
    - skip_browser: bool — skip Playwright stage
    - skip_llm: bool — skip Claude API stage
    - skip_discovery: bool — skip source discovery
    - api_key: str — Anthropic API key for LLM stage
    - verbose: bool — print detailed output
    - delay: float — seconds between requests (default 1.5)
    """
    opts = options or {}
    verbose = opts.get("verbose", True)
    delay = opts.get("delay", 1.5)

    all_results = []
    stats = {
        "total": len(registry),
        "parser_a_ok": 0,
        "parser_b_ok": 0,
        "parser_c_ok": 0,
        "llm_ok": 0,
        "manual_ok": 0,
        "cached": 0,
        "on_request": 0,
        "not_offered": 0,
        "refers_to": 0,
        "degraded": 0,
        "errors": [],
    }

    if verbose:
        print("=" * 70)
        print(f"🔍 IHK Prüfungstermin-Scraper — Pipeline v4")
        print(f"   Datum: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
        print(f"   IHKs: {len(registry)}")
        print("=" * 70)

    for ihk in registry:
        ihk_id = ihk["id"]
        avail = ihk.get("availability", "online")
        label = AVAIL_LABELS.get(avail, avail)

        if verbose:
            print(f"\n{'─' * 60}")
            print(f"📍 {ihk['name']} ({ihk['city']}) — {label}")

        # ── Skip non-scrapeable IHKs ──
        if avail in ("not_offered", "refers_to", "on_request"):
            entry = _handle_non_scrapeable(ihk, avail, verbose)
            all_results.append(entry)
            if avail == "not_offered":
                stats["not_offered"] += 1
            elif avail == "refers_to":
                stats["refers_to"] += 1
            elif avail == "on_request":
                stats["on_request"] += 1
            continue

        # ── Handle manual data ──
        if ihk.get("manual_dates"):
            entry = _handle_manual_data(ihk, verbose)
            all_results.append(entry)
            stats["manual_ok"] += 1
            continue

        # ── Run fallback chain ──
        result = _run_fallback_chain(ihk, opts, verbose)

        # ── Validate ──
        prev_result = cache.get_cached_result(ihk_id)
        vr = validator.validate_scrape_result(result, prev_result)

        if verbose:
            if vr.errors:
                print(f"   ❗ Validierung: {len(vr.errors)} Fehler")
                for e in vr.errors[:3]:
                    print(f"      → {e}")
            if vr.warnings:
                print(f"   ⚠️  Warnungen: {len(vr.warnings)}")
                for w in vr.warnings[:3]:
                    print(f"      → {w}")

        # Build final result entry
        entry = _build_result_entry(ihk, result, vr)

        # Save to cache for next run comparison
        cache.save_result(ihk_id, entry)

        all_results.append(entry)

        # Update stats
        if result.success:
            stage_map = {
                "parser_a": "parser_a_ok",
                "parser_b": "parser_b_ok",
                "parser_c": "parser_c_ok",
                "parser_llm": "llm_ok",
                "cache": "cached",
            }
            stat_key = stage_map.get(result.stage, "parser_a_ok")
            stats[stat_key] = stats.get(stat_key, 0) + 1
        else:
            stats["degraded"] += 1
            stats["errors"].append({"ihk": ihk["name"], "error": result.error or "Keine Daten"})

        # Polite delay
        time.sleep(delay)

    # ── Alerts ──
    alerts = alert.check_alerts(all_results)
    if alerts:
        if verbose:
            print(f"\n🔔 {len(alerts)} Alert(s):")
            for a in alerts:
                print(f"   {a['message']}")
        alert.send_alert_email(alerts)

    # ── Log ──
    summary = alert.generate_health_summary(all_results)
    summary.update(stats)
    log_file = alert.log_run(
        [r for r in all_results],
        summary
    )

    # ── Print Summary ──
    if verbose:
        _print_summary(all_results, stats, registry)

    # ── Save final data ──
    output_file = Path("data/ihk_exam_dates.json")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(
        json.dumps(all_results, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    if verbose:
        print(f"\n💾 Ergebnisse: {output_file}")
        print(f"💾 Log:        {log_file}")

    return all_results


def _run_fallback_chain(ihk: dict, opts: dict, verbose: bool) -> ScrapeResult:
    """Run the fallback chain: Cache → Parser A → Parser B → Parser C → LLM → Degrade"""
    ihk_id = ihk["id"]
    url = ihk["url"]
    avail = ihk.get("availability", "online")
    parser_type = ihk.get("parser", "A")

    # ── Stufe 0: Fetch with Cache ──
    if verbose:
        print(f"   🔗 {url[:80]}...")

    fetch = cache.fetch_with_cache(url)

    if fetch["error"] and not fetch["html"]:
        if verbose:
            print(f"   ❌ Fetch-Fehler: {fetch['error']}")

        # Try source discovery before giving up
        if not opts.get("skip_discovery"):
            return _try_source_discovery(ihk, opts, verbose)

        return ScrapeResult(
            ihk_id=ihk_id, stage="fetch", error=fetch["error"],
            http_status=fetch.get("status_code"), url_used=url,
        )

    if verbose:
        cache_label = " (Cache)" if fetch["from_cache"] else ""
        status = fetch.get("status_code", "?")
        print(f"   ✅ HTTP {status}{cache_label} — {fetch['content_length']:,} Zeichen")

    html = fetch["html"]

    # ── Stufe 1: Parser A (deterministic HTML) ──
    if parser_type in ("A", "A+B"):
        if verbose:
            print(f"   🔧 Stufe 1: Parser A...")
        result = parser_a.parse(html, ihk_id, url)
        result.http_status = fetch.get("status_code")

        if result.success:
            if verbose:
                _print_result(result)
            return result
        elif verbose:
            print(f"   ⚠️  Parser A: 0 Termine gefunden")

    # ── Stufe 1b: Parser B (PDF) ──
    pdf_url = ihk.get("pdf_url")
    if pdf_url or parser_type in ("B", "A+B"):
        target_url = pdf_url or url
        if verbose:
            print(f"   🔧 Stufe 1b: Parser B (PDF)...")
        result = parser_b.parse(target_url, ihk_id)

        if result.success:
            if verbose:
                _print_result(result)
            return result
        elif verbose:
            msg = result.error or "0 Termine"
            print(f"   ⚠️  Parser B: {msg}")

    # ── Stufe 2: Parser C (Playwright Browser) ──
    if not opts.get("skip_browser") and (avail == "online_js" or parser_type == "C"):
        if parser_c.is_available():
            if verbose:
                print(f"   🔧 Stufe 2: Parser C (Browser)...")
            result = parser_c.parse(url, ihk_id)

            if result.success:
                if verbose:
                    _print_result(result)
                return result
            elif verbose:
                msg = result.error or "0 Termine"
                print(f"   ⚠️  Parser C: {msg}")
        elif verbose:
            print(f"   ⏭️  Parser C: Playwright nicht installiert")

    # ── Stufe 3: Source Discovery ──
    if not opts.get("skip_discovery"):
        discovery_result = _try_source_discovery(ihk, opts, verbose)
        if discovery_result and discovery_result.success:
            return discovery_result

    # ── Stufe 4: Claude LLM ──
    if not opts.get("skip_llm"):
        api_key = opts.get("api_key")
        if api_key or os.environ.get("ANTHROPIC_API_KEY"):
            if verbose:
                print(f"   🔧 Stufe 4: Claude LLM...")

            relevant_text = parser_llm.extract_relevant_text(html) if html else ""
            if relevant_text:
                result = parser_llm.parse(
                    text=relevant_text,
                    ihk_id=ihk_id,
                    ihk_name=ihk.get("name", ""),
                    ihk_city=ihk.get("city", ""),
                    url=url,
                    api_key=api_key,
                )

                if result.success:
                    if verbose:
                        _print_result(result)
                    return result
                elif verbose:
                    msg = result.error or "0 Termine"
                    print(f"   ⚠️  LLM: {msg}")
        elif verbose:
            print(f"   ⏭️  LLM: Kein API-Key")

    # ── Stufe 5: Safe Degrade ──
    if verbose:
        print(f"   🔻 Safe Degrade: Keine Termine gefunden")

    # Try to use cached previous result
    prev = cache.get_cached_result(ihk_id)
    if prev and prev.get("dates_2026"):
        if verbose:
            print(f"   📦 Verwende letzte bekannte Daten ({len(prev['dates_2026'])} Termine)")
        return ScrapeResult(
            ihk_id=ihk_id, stage="cache",
            success=True,
            exam_events=prev.get("exam_events", []),
            raw_dates_2026=prev.get("dates_2026", []),
            fees=prev.get("fees", []),
            url_used=url,
        )

    return ScrapeResult(ihk_id=ihk_id, stage="degraded", url_used=url)


def _try_source_discovery(ihk, opts, verbose):
    """Try to find a new URL via source discovery."""
    import os
    if verbose:
        print(f"   🔧 Stufe 3: Source Discovery...")

    discovery = source_discovery.discover_url(
        ihk_name=ihk.get("name", ""),
        ihk_city=ihk.get("city", ""),
        ihk_id=ihk["id"],
        current_url=ihk.get("url", ""),
    )

    if discovery.get("best_url"):
        new_url = discovery["best_url"]
        if verbose:
            print(f"   🔍 Neue URL gefunden: {new_url[:80]}...")
            print(f"   ⚠️  ACHTUNG: URL muss manuell geprüft werden!")

        # Try Parser A on new URL
        fetch = cache.fetch_with_cache(new_url)
        if fetch.get("html"):
            result = parser_a.parse(fetch["html"], ihk["id"], new_url)
            result.http_status = fetch.get("status_code")
            if result.success:
                if verbose:
                    print(f"   ✅ Neue URL liefert Daten!")
                    _print_result(result)
                return result
    elif verbose:
        print(f"   ⚠️  Keine alternative URL gefunden")

    return None


def _handle_non_scrapeable(ihk, avail, verbose):
    """Handle IHKs that don't need scraping."""
    entry = {
        "id": ihk["id"],
        "name": ihk["name"],
        "city": ihk["city"],
        "lat": ihk.get("lat"),
        "lon": ihk.get("lon"),
        "availability": avail,
        "status": "skip",
        "dates_2026": [],
        "exam_events": [],
    }

    if avail == "not_offered":
        if verbose:
            print(f"   ⏭️  Bietet keine Prüfung an.")
    elif avail == "refers_to":
        ref = ihk.get("refers_to", "?")
        entry["refers_to"] = ref
        if verbose:
            print(f"   ⏭️  Verweist auf: {ref}")
    elif avail == "on_request":
        contact = ihk.get("contact", {})
        entry["contact"] = contact
        if verbose:
            print(f"   📞 Termine nur auf Anfrage")
            if contact.get("name"):
                print(f"      Kontakt: {contact['name']}, Tel: {contact.get('phone', '—')}")

    if ihk.get("note"):
        entry["note"] = ihk["note"]
        if verbose:
            print(f"   📝 {ihk['note']}")

    return entry


def _handle_manual_data(ihk, verbose):
    """Handle IHKs with manually entered data."""
    manual = ihk.get("manual_dates", [])
    events = []
    dates = []

    for m in manual:
        ev_dates = []
        if m.get("schriftlich"):
            ev_dates.append(m["schriftlich"])
            dates.append(m["schriftlich"])
        if m.get("muendlich"):
            ev_dates.append(m["muendlich"])
            dates.append(m["muendlich"])

        events.append({
            "dates": ev_dates,
            "type": "combined" if m.get("schriftlich") and m.get("muendlich") else "exam_date",
            "schriftlich": m.get("schriftlich"),
            "muendlich": m.get("muendlich"),
            "anmeldeschluss": m.get("anmeldeschluss"),
            "source": "manual",
            "evidence": m.get("source", "manuell eingetragen"),
            "confidence": 1.0,
        })

    if verbose:
        print(f"   📋 Manuelle Daten: {len(manual)} Termin(e)")
        for m in manual:
            print(f"      schr: {m.get('schriftlich', '—')} | mdl: {m.get('muendlich', '—')}")

    return {
        "id": ihk["id"],
        "name": ihk["name"],
        "city": ihk["city"],
        "lat": ihk.get("lat"),
        "lon": ihk.get("lon"),
        "availability": ihk.get("availability", "online_js"),
        "status": "manual",
        "stage": "manual",
        "dates_2026": dates,
        "exam_events": events,
        "fee": ihk.get("fee"),
    }


def _build_result_entry(ihk, result, validation_result):
    """Build the final result entry for an IHK."""
    events = validation_result.cleaned_events if validation_result.cleaned_events else []
    if not events and result.exam_events:
        events = [e.to_dict() if hasattr(e, 'to_dict') else e for e in result.exam_events]

    return {
        "id": ihk["id"],
        "name": ihk["name"],
        "city": ihk["city"],
        "lat": ihk.get("lat"),
        "lon": ihk.get("lon"),
        "availability": ihk.get("availability", "online"),
        "status": "ok" if result.success else ("degraded" if result.stage == "degraded" else "error"),
        "stage": result.stage,
        "dates_2026": result.raw_dates_2026,
        "exam_events": events,
        "fees": result.fees,
        "keyword_score": result.keyword_score,
        "strategies_used": result.strategies_used,
        "url_used": result.url_used,
        "from_cache": getattr(result, 'from_cache', False),
        "validation_errors": validation_result.errors if validation_result else [],
        "validation_warnings": validation_result.warnings if validation_result else [],
        "timestamp": result.timestamp,
    }


def _print_result(result):
    """Print a parser result."""
    print(f"   📊 Keywords: {result.keyword_score}")
    print(f"   📅 2026-Daten: {len(result.raw_dates_2026)}")
    if result.fees:
        print(f"   💰 Gebühren: {', '.join(result.fees)}")
    if result.strategies_used:
        print(f"   🔧 Strategien: {', '.join(result.strategies_used)}")
    if result.raw_dates_2026:
        print(f"   🎯 Termine: {', '.join(result.raw_dates_2026[:8])}")
    if result.exam_events:
        n = len(result.exam_events)
        print(f"   📌 Events: {n}")
        for i, ev in enumerate(result.exam_events[:4]):
            if hasattr(ev, 'to_dict'):
                ev = ev.to_dict()
            dates = ", ".join(ev.get("dates", [])[:3])
            etype = ev.get("type", "?")
            parts = [f"[{etype}]"]
            if ev.get("schriftlich"): parts.append(f"schr: {ev['schriftlich']}")
            if ev.get("muendlich"): parts.append(f"mdl: {ev['muendlich']}")
            if ev.get("anmeldeschluss"): parts.append(f"frist: {ev['anmeldeschluss']}")
            if ev.get("status") and ev["status"] != "unknown": parts.append(f"→ {ev['status']}")
            print(f"      {i+1}. {dates} — {' | '.join(parts)}")


def _print_summary(results, stats, registry):
    """Print final summary."""
    print("\n" + "=" * 70)
    print("📊 ZUSAMMENFASSUNG — Pipeline v4")
    print("=" * 70)

    total_data = stats["parser_a_ok"] + stats["parser_b_ok"] + stats["parser_c_ok"] + stats["llm_ok"] + stats["manual_ok"] + stats["cached"]

    print(f"""
┌──────────────────────────────────────────────────┐
│  ✅ Parser A (HTML):           {stats['parser_a_ok']:<19}│
│  📄 Parser B (PDF):            {stats['parser_b_ok']:<19}│
│  🌐 Parser C (Browser):        {stats['parser_c_ok']:<19}│
│  🤖 Claude LLM:                {stats['llm_ok']:<19}│
│  ✏️  Manuelle Daten:            {stats['manual_ok']:<19}│
│  📦 Aus Cache:                  {stats['cached']:<19}│
│  📞 Nur auf Anfrage:           {stats['on_request']:<19}│
│  ❌ Nicht angeboten:           {stats['not_offered']:<19}│
│  ↗️  Verweist:                  {stats['refers_to']:<19}│
│  🔻 Degraded:                  {stats['degraded']:<19}│
│──────────────────────────────────────────────────│
│  📈 GESAMT mit Daten:          {total_data}/{stats['total']:<16}│
└──────────────────────────────────────────────────┘""")

    print(f"\n{'IHK':<34}{'Status':<12}{'Stufe':<14}{'2026':<8}{'Events':<8}")
    print("─" * 80)

    for r in results:
        name = r.get("name", "?")[:33].ljust(33)
        status_icon = {
            "ok": "✅", "manual": "✏️", "skip": "⏭️",
            "degraded": "🔻", "error": "❌",
        }.get(r.get("status", "?"), "?")
        stage = r.get("stage", r.get("availability", "—"))[:13].ljust(13)
        n_dates = len(r.get("dates_2026", []))
        dates = str(n_dates).ljust(7) if n_dates > 0 else "—".ljust(7)
        n_events = len(r.get("exam_events", []))
        events = str(n_events).ljust(7) if n_events > 0 else "—".ljust(7)
        print(f" {name} {status_icon:<11} {stage} {dates} {events}")

    print("─" * 80)


# Need os import for env var check
import os
