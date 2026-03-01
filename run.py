#!/usr/bin/env python3
"""
IHK Prüfungstermin-Scraper — Main Runner v4
=============================================

Führt die komplette Pipeline aus:
  Stufe 1: Parser A (HTML, deterministic)
  Stufe 2: Parser C (Browser/Playwright)
  Stufe 3: Source Discovery (neue URLs)
  Stufe 4: Claude LLM (mit Guardrails)
  Stufe 5: Safe Degrade (alte Daten + Alert)

Nutzung:
  python3 run.py                    # Alles laufen lassen
  python3 run.py --skip-browser     # Ohne Playwright
  python3 run.py --skip-llm         # Ohne Claude API
  python3 run.py --skip-discovery   # Ohne Source Discovery
  python3 run.py --fresh            # Cache ignorieren
"""

import sys
import json
import argparse
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from scraper.pipeline import run_pipeline


# ─── BaWü Registry (Stand: 28.02.2026) ─────────────────────────────────────

REGISTRY = [
    {
        "id": "ihk-rhein-neckar",
        "name": "IHK Rhein-Neckar",
        "city": "Mannheim",
        "state": "Baden-Württemberg",
        "lat": 49.4875, "lon": 8.4660,
        "availability": "online",
        "parser": "A",
        "url": "https://www.ihk.de/rhein-neckar/wirtschaftsstandort/branchen/dienstleistungen/immobilienwirtschaft/pruefung-zertifizierter-verwalter-5033716",
    },
    {
        "id": "ihk-reutlingen",
        "name": "IHK Reutlingen",
        "city": "Reutlingen",
        "state": "Baden-Württemberg",
        "lat": 48.4914, "lon": 9.2043,
        "availability": "online",
        "parser": "A",
        "url": "https://www.reutlingen.ihk.de/gruendung/gruenden-nach-branchen/erlaubnis-gemaess-34-c-gewo/zertifizierter-weg-verwalter/",
    },
    {
        "id": "ihk-stuttgart",
        "name": "IHK Region Stuttgart",
        "city": "Stuttgart",
        "state": "Baden-Württemberg",
        "lat": 48.7758, "lon": 9.1829,
        "availability": "online_js",
        "parser": "C",
        "url": "https://www.ihk.de/stuttgart/bildung-schulung-pruefung/pruefungen/dienstleistungen/anmeldung-zum-zertifizierten-verwalter-5619738",
        "manual_dates": [
            {
                "schriftlich": "08.06.2026",
                "muendlich": "24.06.2026",
                "source": "manuell geprüft 28.02.2026",
            }
        ],
        "fee": "250 €",
    },
    {
        "id": "ihk-karlsruhe",
        "name": "IHK Karlsruhe",
        "city": "Karlsruhe",
        "state": "Baden-Württemberg",
        "lat": 49.0069, "lon": 8.4037,
        "availability": "online",
        "parser": "A",
        "url": "https://www.ihk.de/karlsruhe/fachthemen/recht/gewerbetreibende-nach-34c-gewo/wohnimmobilienverwalter/zertifizierter-wohnimmobilienverwalter-4926478",
    },
    {
        "id": "ihk-konstanz",
        "name": "IHK Hochrhein-Bodensee",
        "city": "Konstanz",
        "state": "Baden-Württemberg",
        "lat": 47.6603, "lon": 9.1758,
        "availability": "online",
        "parser": "A",
        "url": "https://www.ihk.de/konstanz/berufliche-bildung/unterrichtungen-sachkunde/zertifizierter-hausverwalter/pruefungstermine-5689984",
    },
    {
        "id": "ihk-heilbronn",
        "name": "IHK Heilbronn-Franken",
        "city": "Heilbronn",
        "state": "Baden-Württemberg",
        "lat": 49.1427, "lon": 9.2109,
        "availability": "online",
        "parser": "A",
        "url": "https://www.ihk.de/heilbronn-franken/produktmarken/branchen/gewerbeportal/immobilien/wohnimmobilienverwalter/einfuehrung-zertifizierter-verwalter-6049444",
    },
    {
        "id": "ihk-freiburg",
        "name": "IHK Südlicher Oberrhein",
        "city": "Freiburg",
        "state": "Baden-Württemberg",
        "lat": 47.9990, "lon": 7.8421,
        "availability": "online_js",
        "parser": "C",
        "url": "https://www.ihk.de/freiburg/unternehmen-beraten/gruendung-sicherung/unterrichtungsverfahren-und-sach-fachkundepruefungen/zertifizierter-wohnimmobilienverwalter-6795604",
        "manual_dates": [],
        "note": "Gleiches Lux-Formular wie Stuttgart. Termine nur nach Durchklicken sichtbar.",
    },
    {
        "id": "ihk-pforzheim",
        "name": "IHK Nordschwarzwald",
        "city": "Pforzheim",
        "state": "Baden-Württemberg",
        "lat": 48.8922, "lon": 8.6946,
        "availability": "not_offered",
        "parser": "none",
        "url": "https://www.ihk.de/nordschwarzwald/recht/recht/erlaubnisse-und-registrierungen/immobilienmakler-bautraeger-hausverwalter/zertifizierter-wohnimmobilienverwalter-was-ist-unter-einem-zertifizierten-verwalter-zu-verstehen--5093908",
        "note": "IHK Nordschwarzwald bietet die Prüfung nicht an (Stand Feb 2026).",
    },
    {
        "id": "ihk-vs",
        "name": "IHK Schwarzwald-Baar-Heuberg",
        "city": "Villingen-Schwenningen",
        "state": "Baden-Württemberg",
        "lat": 48.0623, "lon": 8.4935,
        "availability": "on_request",
        "parser": "none",
        "url": "https://www.ihk.de/sbh/fuer-unternehmen/recht-und-steuern/vermittlergewerbe/immobilienmakler-darlehensvermittler-bautraeger-baubetreuer-und-wohnimmobilienverwalter/zertifizierter-wohnimmobilienverwalter-5381730",
        "contact": {
            "name": "Florian Merz",
            "phone": "07721 922-129",
            "email": "florian.merz@vs.ihk.de",
        },
        "note": "Bietet Prüfung an, aber keine Termine online. Nur auf Anfrage.",
    },
    {
        "id": "ihk-bodensee",
        "name": "IHK Bodensee-Oberschwaben",
        "city": "Weingarten",
        "state": "Baden-Württemberg",
        "lat": 47.8105, "lon": 9.6384,
        "availability": "on_request",
        "parser": "none",
        "url": "https://www.weingarten.ihk.de/fuer-unternehmen/branchen/vermittlergewerbe/makler-und-bautraeger/zertifizierter-wohnimmobilienverwalter-4955336",
        "contact": {
            "name": "Team Vermittlergewerbe",
            "phone": "0751 409-146",
            "email": "info@weingarten.ihk.de",
        },
        "note": "Infoseite vorhanden, aber keine Prüfungstermine online.",
    },
    {
        "id": "ihk-ulm",
        "name": "IHK Ulm",
        "city": "Ulm",
        "state": "Baden-Württemberg",
        "lat": 48.4011, "lon": 9.9876,
        "availability": "refers_to",
        "refers_to": "ihk-reutlingen",
        "parser": "none",
        "url": "",
        "note": "Verweist auf IHK Reutlingen.",
    },
    {
        "id": "ihk-ostwuerttemberg",
        "name": "IHK Ostwürttemberg",
        "city": "Heidenheim",
        "state": "Baden-Württemberg",
        "lat": 48.6761, "lon": 10.1543,
        "availability": "refers_to",
        "refers_to": "ihk-stuttgart",
        "parser": "none",
        "url": "https://www.ostwuerttemberg.ihk.de/recht/vermittler/immobilienmakler-darlehensvermittler-bautraeger-baubetreuer-wohnimmobilienverwalter/pruefung-zum-zertifizierten-weg-verwalter-6150206",
        "contact": {
            "name": "Thorsten Drescher",
            "phone": "07321 324-121",
            "email": "thorsten.drescher@ostwuerttemberg.ihk.de",
        },
        "note": "Verweist auf IHK Stuttgart. Prüfung findet in Stuttgart statt.",
    },
]


def main():
    parser = argparse.ArgumentParser(description="IHK Prüfungstermin-Scraper v4")
    parser.add_argument("--skip-browser", action="store_true", help="Playwright-Stufe überspringen")
    parser.add_argument("--skip-llm", action="store_true", help="Claude API-Stufe überspringen")
    parser.add_argument("--skip-discovery", action="store_true", help="Source Discovery überspringen")
    parser.add_argument("--fresh", action="store_true", help="Cache ignorieren")
    parser.add_argument("--api-key", type=str, help="Anthropic API Key")
    parser.add_argument("--quiet", action="store_true", help="Weniger Output")
    args = parser.parse_args()

    options = {
        "skip_browser": args.skip_browser,
        "skip_llm": args.skip_llm,
        "skip_discovery": args.skip_discovery,
        "skip_cache": args.fresh,
        "api_key": args.api_key,
        "verbose": not args.quiet,
    }

    # Also save the registry
    registry_path = Path("data/ihk_registry_bw.json")
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(json.dumps(REGISTRY, ensure_ascii=False, indent=2), encoding="utf-8")

    results = run_pipeline(REGISTRY, options)

    # Exit code: 0 if at least some data found
    has_data = any(len(r.get("dates_2026", [])) > 0 or len(r.get("exam_events", [])) > 0 for r in results)
    sys.exit(0 if has_data else 1)


if __name__ == "__main__":
    main()
