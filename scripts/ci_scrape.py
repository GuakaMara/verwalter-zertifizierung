#!/usr/bin/env python3
"""
CI Scraper — GitHub Actions Wrapper
====================================

Führt den Scraper im CI-Modus aus:
- Randomisierte Pausen (5-30 Sek) zwischen Requests
- Backup alter Daten vor dem Run
- Bei Fehler: alte Daten wiederherstellen
- Detailliertes JSON-Log pro Run
- Stand-Datum in data/meta.json speichern
"""

import sys
import json
import shutil
import random
from datetime import datetime
from pathlib import Path

# Project root
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from scraper.pipeline import run_pipeline

# ─── Registry aus JSON laden ─────────────────────────────────────────
REGISTRY_FILE = ROOT / "data" / "ihk_registry_bw.json"
REGISTRY = json.loads(REGISTRY_FILE.read_text(encoding="utf-8"))


def main():
    timestamp = datetime.now()
    print("=" * 70)
    print(f"🤖 CI Scraper — {timestamp.strftime('%d.%m.%Y %H:%M')}")
    print(f"   IHKs: {len(REGISTRY)}")
    print(f"   Modus: GitHub Actions (randomisierte Pausen)")
    print("=" * 70)

    # ── Backup alte Daten ──
    data_dir = ROOT / "data"
    exam_file = data_dir / "ihk_exam_dates.json"
    backup_file = data_dir / "ihk_exam_dates.backup.json"

    if exam_file.exists():
        shutil.copy2(exam_file, backup_file)
        print(f"\n💾 Backup erstellt: {backup_file.name}")

    # ── Randomisierte Pause zwischen 5-30 Sekunden ──
    delay = random.uniform(5, 30)
    print(f"⏱️  Pause zwischen Requests: {delay:.0f}s (randomisiert 5-30s)")

    # ── Pipeline ausführen ──
    options = {
        "skip_browser": True,     # Kein Playwright in CI
        "skip_llm": True,         # Kein Claude API (noch kein Key)
        "skip_discovery": True,   # Kein Source Discovery
        "skip_cache": True,       # Immer frisch fetchen in CI
        "verbose": True,
        "delay": delay,
    }

    try:
        results = run_pipeline(REGISTRY, options)
    except Exception as e:
        print(f"\n❌ Pipeline-Fehler: {e}")
        _restore_backup(exam_file, backup_file)
        sys.exit(1)

    # ── Ergebnis auswerten ──
    successful = []
    failed = []
    skipped = []
    total_events = 0

    for r in results:
        ihk_id = r.get("ihk_id") or r.get("id", "unknown")
        name = r.get("name", ihk_id)
        avail = r.get("availability", "online")
        events = r.get("exam_events", [])

        if avail in ("not_offered", "refers_to", "on_request"):
            skipped.append({"id": ihk_id, "name": name, "reason": avail})
        elif events or r.get("status") == "ok":
            successful.append({
                "id": ihk_id,
                "name": name,
                "events": len(events),
                "stage": r.get("stage", "?"),
            })
            total_events += len(events)
        else:
            failed.append({
                "id": ihk_id,
                "name": name,
                "error": r.get("error", "Keine Daten"),
            })

    # ── Stand-Datum speichern ──
    meta = {
        "last_update": timestamp.strftime("%d.%m.%Y"),
        "last_update_iso": timestamp.isoformat(),
        "total_ihks": len(REGISTRY),
        "ihks_with_data": len(successful),
        "total_events": total_events,
        "failed": len(failed),
        "skipped": len(skipped),
    }
    meta_file = data_dir / "meta.json"
    meta_file.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    # ── Run-Log speichern ──
    log_dir = data_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"run_{timestamp.strftime('%Y-%m-%d_%H%M%S')}.json"
    log_data = {
        "timestamp": timestamp.isoformat(),
        "duration_note": f"~{delay:.0f}s delay between requests",
        "summary": meta,
        "successful": successful,
        "failed": failed,
        "skipped": skipped,
    }
    log_file.write_text(json.dumps(log_data, ensure_ascii=False, indent=2), encoding="utf-8")

    # ── Alte Logs aufräumen (max 20 behalten) ──
    logs = sorted(log_dir.glob("run_*.json"), reverse=True)
    for old_log in logs[20:]:
        old_log.unlink()
        print(f"   🗑️  Altes Log gelöscht: {old_log.name}")

    # ── Zusammenfassung ──
    print("\n" + "=" * 70)
    print(f"📊 ERGEBNIS")
    print(f"   ✅ Erfolgreich: {len(successful)} IHKs ({total_events} Termine)")
    print(f"   ❌ Fehlgeschlagen: {len(failed)} IHKs")
    print(f"   ⏭️  Übersprungen: {len(skipped)} IHKs")
    print(f"   📅 Stand-Datum: {meta['last_update']}")
    print(f"   💾 Log: {log_file.name}")

    if failed:
        print(f"\n   Fehlgeschlagene IHKs:")
        for f in failed:
            print(f"     → {f['name']}: {f['error']}")

    print("=" * 70)

    # ── Bei komplettem Fehlschlag: Backup wiederherstellen ──
    if len(successful) == 0 and len(failed) > 0:
        print("\n⚠️  KEIN einziger Erfolg — stelle Backup wieder her!")
        _restore_backup(exam_file, backup_file)
        sys.exit(1)

    # ── Backup löschen bei Erfolg ──
    if backup_file.exists():
        backup_file.unlink()

    print("\n✅ CI Scraper erfolgreich abgeschlossen")
    sys.exit(0)


def _restore_backup(exam_file: Path, backup_file: Path):
    """Alte Daten wiederherstellen."""
    if backup_file.exists():
        shutil.copy2(backup_file, exam_file)
        print(f"   ↩️  Backup wiederhergestellt: {exam_file.name}")
        backup_file.unlink()
    else:
        print(f"   ⚠️  Kein Backup vorhanden")


if __name__ == "__main__":
    main()
