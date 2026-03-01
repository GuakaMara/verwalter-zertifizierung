#!/usr/bin/env python3
"""
IHK Prüfungstermin-Scraper — Main Runner v4
=============================================

Nutzung:
  python3 run.py                              # Alles (ganz Deutschland)
  python3 run.py --state "Baden-Württemberg"  # Nur ein Bundesland
  python3 run.py --state "Bayern"             # Bayern testen
  python3 run.py --skip-browser               # Ohne Playwright
  python3 run.py --skip-llm                   # Ohne Claude API
  python3 run.py --skip-discovery             # Ohne URL-Suche
  python3 run.py --fresh                      # Cache ignorieren
  python3 run.py --only ihk-muenchen          # Einzelne IHK testen
"""

import sys
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from scraper.pipeline import run_pipeline


def load_registry():
    """Load IHK registry from JSON file."""
    registry_path = Path("data/ihk_registry_de.json")
    if not registry_path.exists():
        print(f"❌ Registry nicht gefunden: {registry_path}")
        sys.exit(1)
    data = json.loads(registry_path.read_text(encoding="utf-8"))
    return [entry for entry in data if "id" in entry]


def main():
    parser = argparse.ArgumentParser(description="IHK Prüfungstermin-Scraper v4")
    parser.add_argument("--state", type=str, help="Nur ein Bundesland (z.B. 'Bayern')")
    parser.add_argument("--only", type=str, help="Nur eine IHK (z.B. 'ihk-muenchen')")
    parser.add_argument("--skip-browser", action="store_true", help="Playwright-Stufe überspringen")
    parser.add_argument("--skip-llm", action="store_true", help="Claude API-Stufe überspringen")
    parser.add_argument("--skip-discovery", action="store_true", help="Source Discovery überspringen")
    parser.add_argument("--fresh", action="store_true", help="Cache ignorieren")
    parser.add_argument("--api-key", type=str, help="Anthropic API Key")
    parser.add_argument("--quiet", action="store_true", help="Weniger Output")
    args = parser.parse_args()

    all_registry = load_registry()
    registry = all_registry

    # Filter by state
    if args.state:
        state_lower = args.state.lower()
        registry = [r for r in all_registry if r["state"].lower() == state_lower]
        if not registry:
            all_states = sorted(set(r["state"] for r in all_registry))
            matches = [s for s in all_states if state_lower in s.lower()]
            if matches:
                registry = [r for r in all_registry if r["state"] in matches]
                print(f"🔍 Gefiltert auf: {', '.join(matches)}")
            else:
                print(f"❌ Kein Bundesland '{args.state}' gefunden.")
                print(f"   Verfügbar: {', '.join(all_states)}")
                sys.exit(1)

    # Filter by single IHK
    if args.only:
        registry = [r for r in all_registry if r["id"] == args.only]
        if not registry:
            print(f"❌ IHK '{args.only}' nicht gefunden.")
            sys.exit(1)

    options = {
        "skip_browser": args.skip_browser,
        "skip_llm": args.skip_llm,
        "skip_discovery": args.skip_discovery,
        "skip_cache": args.fresh,
        "api_key": args.api_key,
        "verbose": not args.quiet,
    }

    results = run_pipeline(registry, options)

    has_data = any(len(r.get("dates_2026", [])) > 0 or len(r.get("exam_events", [])) > 0 for r in results)
    sys.exit(0 if has_data else 1)


if __name__ == "__main__":
    main()
