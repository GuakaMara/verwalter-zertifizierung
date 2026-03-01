#!/usr/bin/env python3
"""
Build Script — Scraper-Daten ins JS-Bundle schreiben
=====================================================

Liest data/ihk_exam_dates.json und aktualisiert das G1-Array
im JS-Bundle (assets/index-EXWOLlBA.js).

Das G1-Array enthält alle Prüfungstermine im Format:
  {schr:"DD.MM.YYYY",mdl:"DD.MM.YYYY"|null,frist:"DD.MM.YYYY"|null,
   ihk:"Name",city:"Stadt",state:"Bundesland",fee:250,lat:49.48,lon:8.46}

Zusätzlich werden refers_to-IHKs aufgelöst: wenn IHK Darmstadt auf
IHK Frankfurt verweist, bekommt Darmstadt die Frankfurt-Termine mit
eigenen Koordinaten.
"""

import json
import re
import sys
from datetime import datetime, date
from pathlib import Path

ROOT = Path(__file__).parent.parent

# ─── Display-Name-Mapping ────────────────────────────────────────────
# m3 im Bundle nutzt Kurznamen. Diese Zuordnung muss 1:1 zu m3 passen.
# Registry-ID → Bundle-Anzeigename
DISPLAY_NAMES = {
    "ihk-rhein-neckar": "IHK Rhein-Neckar",
    "ihk-reutlingen": "IHK Reutlingen",
    "ihk-stuttgart": "IHK Region Stuttgart",
    "ihk-karlsruhe": "IHK Karlsruhe",
    "ihk-konstanz": "IHK Hochrhein-Bodensee",
    "ihk-heilbronn": "IHK Heilbronn-Franken",
    "ihk-freiburg": "IHK Südlicher Oberrhein",
    "ihk-pforzheim": "IHK Nordschwarzwald",
    "ihk-vs": "IHK Schwarzwald-Baar-Heuberg",
    "ihk-bodensee": "IHK Bodensee-Oberschwaben",
    "ihk-ulm": "IHK Ulm",
    "ihk-ostwuerttemberg": "IHK Ostwürttemberg",
    "ihk-muenchen": "IHK München",
    "ihk-nuernberg": "IHK Nürnberg",
    "ihk-augsburg": "IHK Augsburg",
    "ihk-bayreuth": "IHK Oberfranken",
    "ihk-regensburg": "IHK Regensburg",
    "ihk-passau": "IHK Niederbayern",
    "ihk-coburg": "IHK Coburg",
    "ihk-wuerzburg": "IHK Würzburg-Schweinfurt",
    "ihk-aschaffenburg": "IHK Aschaffenburg",
    "ihk-berlin": "IHK Berlin",
    "ihk-potsdam": "IHK Potsdam",
    "ihk-cottbus": "IHK Cottbus",
    "ihk-frankfurt-oder": "IHK Ostbrandenburg",
    "ihk-bremen": "HK Bremen",
    "ihk-hamburg": "HK Hamburg",
    "ihk-frankfurt": "IHK Frankfurt",
    "ihk-darmstadt": "IHK Darmstadt",
    "ihk-kassel": "IHK Kassel-Marburg",
    "ihk-giessen": "IHK Gießen-Friedberg",
    "ihk-wiesbaden": "IHK Wiesbaden",
    "ihk-hanau": "IHK Hanau-Gelnhausen",
    "ihk-fulda": "IHK Fulda",
    "ihk-offenbach": "IHK Offenbach",
    "ihk-dillenburg": "IHK Lahn-Dill",
    "ihk-limburg": "IHK Limburg",
    "ihk-rostock": "IHK Rostock",
    "ihk-schwerin": "IHK Schwerin",
    "ihk-neubrandenburg": "IHK Neubrandenburg",
    "ihk-hannover": "IHK Hannover",
    "ihk-braunschweig": "IHK Braunschweig",
    "ihk-oldenburg": "IHK Oldenburg",
    "ihk-osnabrueck": "IHK Osnabrück",
    "ihk-lueneburg": "IHK Lüneburg-Wolfsburg",
    "ihk-stade": "IHK Stade",
    "ihk-emden": "IHK Emden",
    "ihk-koeln": "IHK Köln",
    "ihk-duesseldorf": "IHK Düsseldorf",
    "ihk-dortmund": "IHK Dortmund",
    "ihk-essen": "IHK Essen",
    "ihk-muenster": "IHK Nord Westfalen",
    "ihk-bielefeld": "IHK Bielefeld",
    "ihk-aachen": "IHK Aachen",
    "ihk-bonn": "IHK Bonn/Rhein-Sieg",
    "ihk-wuppertal": "IHK Wuppertal",
    "ihk-duisburg": "IHK Duisburg",
    "ihk-krefeld": "IHK Mittlerer Niederrhein",
    "ihk-hagen": "SIHK zu Hagen",
    "ihk-bochum": "IHK Bochum",
    "ihk-arnsberg": "IHK Arnsberg",
    "ihk-siegen": "IHK Siegen",
    "ihk-detmold": "IHK Detmold",
    "ihk-koblenz": "IHK Koblenz",
    "ihk-mainz": "IHK Rheinhessen",
    "ihk-ludwigshafen": "IHK Pfalz",
    "ihk-trier": "IHK Trier",
    "ihk-saarbruecken": "IHK Saarland",
    "ihk-dresden": "IHK Dresden",
    "ihk-leipzig": "IHK Leipzig",
    "ihk-chemnitz": "IHK Chemnitz",
    "ihk-magdeburg": "IHK Magdeburg",
    "ihk-halle": "IHK Halle-Dessau",
    "ihk-kiel": "IHK Kiel",
    "ihk-luebeck": "IHK Lübeck",
    "ihk-flensburg": "IHK Flensburg",
    "ihk-erfurt": "IHK Erfurt",
    "ihk-gera": "IHK Gera",
    "ihk-suhl": "IHK Südthüringen",
}


def parse_fee(fees_list):
    """Extract largest numeric fee from scraper fees (= Gesamtprüfung)."""
    if not fees_list:
        return None
    max_fee = None
    for fee_str in fees_list:
        cleaned = fee_str.replace("€", "").replace(".", "").replace(",", ".").strip()
        try:
            val = float(cleaned)
            if 20 <= val <= 1000:
                if max_fee is None or val > max_fee:
                    max_fee = val
        except ValueError:
            continue
    return int(max_fee) if max_fee else None


def is_future(date_str):
    """Check if DD.MM.YYYY is today or in the future."""
    try:
        d = datetime.strptime(date_str, "%d.%m.%Y").date()
        return d >= date.today()
    except (ValueError, TypeError):
        return False


def build_entries(exam_data, registry):
    """Build G1-compatible entries from scraper data."""
    # Index registry by ID
    reg_by_id = {r["id"]: r for r in registry}

    # Index exam data by ID
    data_by_id = {}
    for d in exam_data:
        ihk_id = d.get("id") or d.get("ihk_id")
        if ihk_id:
            data_by_id[ihk_id] = d

    entries = []

    # Step 1: Build entries for IHKs with actual data
    for d in exam_data:
        ihk_id = d.get("id") or d.get("ihk_id")
        if not ihk_id:
            continue

        events = d.get("exam_events", [])
        if not events:
            continue

        reg_entry = reg_by_id.get(ihk_id, {})
        display_name = DISPLAY_NAMES.get(ihk_id, d.get("name", ihk_id))
        state = reg_entry.get("state", "")
        city = d.get("city") or reg_entry.get("city", "")
        lat = d.get("lat") or reg_entry.get("lat", 0)
        lon = d.get("lon") or reg_entry.get("lon", 0)

        # Fee: from scraper, or registry, or default
        fee = parse_fee(d.get("fees"))
        if not fee:
            fee = reg_entry.get("fee_amount") or 200

        for event in events:
            schr = event.get("schriftlich")
            mdl = event.get("muendlich")
            frist = event.get("anmeldeschluss")

            # Skip past events
            check_date = schr or mdl
            if check_date and not is_future(check_date):
                continue

            # Skip ausgebucht
            if event.get("status") == "ausgebucht":
                continue

            entries.append({
                "schr": schr,
                "mdl": mdl,
                "frist": frist,
                "ihk": display_name,
                "city": city,
                "state": state,
                "fee": fee,
                "lat": round(lat, 4),
                "lon": round(lon, 4),
                "_ihk_id": ihk_id,  # internal, not in output
            })

    # Step 2: Duplicate entries for refers_to IHKs
    for reg_entry in registry:
        if reg_entry.get("availability") != "refers_to":
            continue

        refers_to = reg_entry.get("refers_to")
        if not refers_to:
            continue

        # Find source IHK's entries
        source_entries = [e for e in entries if e["_ihk_id"] == refers_to]
        if not source_entries:
            continue

        ref_id = reg_entry["id"]
        ref_name = DISPLAY_NAMES.get(ref_id, reg_entry.get("name", ref_id))
        ref_city = reg_entry.get("city", "")
        ref_state = reg_entry.get("state", "")
        ref_lat = reg_entry.get("lat", 0)
        ref_lon = reg_entry.get("lon", 0)

        for src in source_entries:
            entries.append({
                "schr": src["schr"],
                "mdl": src["mdl"],
                "frist": src["frist"],
                "ihk": ref_name,
                "city": ref_city,
                "state": ref_state,
                "fee": src["fee"],
                "lat": round(ref_lat, 4),
                "lon": round(ref_lon, 4),
                "_ihk_id": ref_id,
            })

    # Sort by date
    def sort_key(e):
        try:
            return datetime.strptime(e["schr"] or "31.12.2099", "%d.%m.%Y")
        except ValueError:
            return datetime(2099, 12, 31)

    entries.sort(key=sort_key)
    return entries


def entry_to_js(e):
    """Convert entry dict to JS object literal string."""
    def val(v):
        if v is None:
            return "null"
        if isinstance(v, str):
            return f'"{v}"'
        if isinstance(v, float):
            # Remove trailing zeros
            s = f"{v:.4f}".rstrip("0").rstrip(".")
            return s
        return str(v)

    return (
        f'{{schr:{val(e["schr"])},'
        f'mdl:{val(e["mdl"])},'
        f'frist:{val(e["frist"])},'
        f'ihk:{val(e["ihk"])},'
        f'city:{val(e["city"])},'
        f'state:{val(e["state"])},'
        f'fee:{e["fee"]},'
        f'lat:{val(e["lat"])},'
        f'lon:{val(e["lon"])}}}'
    )


def update_m3(js, registry):
    """Update m3 object to include all IHKs with display names."""
    # Find existing m3
    m3_start = js.index("m3={")
    depth = 0
    for i in range(m3_start + 3, len(js)):
        if js[i] == "{":
            depth += 1
        elif js[i] == "}":
            depth -= 1
            if depth == 0:
                m3_end = i + 1
                break

    old_m3 = js[m3_start:m3_end]

    # Parse existing m3 entries to keep URLs
    existing = {}
    for match in re.finditer(r'"([^"]+)":\{url:"([^"]+)"\}', old_m3):
        existing[match.group(1)] = match.group(2)

    # Add any new IHKs from registry
    for r in registry:
        display_name = DISPLAY_NAMES.get(r["id"])
        if display_name and display_name not in existing:
            # Try to derive homepage URL from exam URL
            url = r.get("url", "")
            if url:
                # Extract base domain
                match = re.match(r'(https?://[^/]+)', url)
                if match:
                    existing[display_name] = match.group(1)

    # Rebuild m3
    parts = []
    for name in sorted(existing.keys()):
        parts.append(f'"{name}":{{url:"{existing[name]}"}}')
    new_m3 = "m3={" + ",".join(parts) + "}"

    return js[:m3_start] + new_m3 + js[m3_end:]


def main():
    print("🔨 Build: Scraper-Daten → JS-Bundle")
    print("=" * 50)

    # Load data
    exam_file = ROOT / "data" / "ihk_exam_dates.json"
    registry_file = ROOT / "data" / "ihk_registry_de.json"
    bundle_file = ROOT / "assets" / "index-EXWOLlBA.js"

    if not exam_file.exists():
        print("❌ data/ihk_exam_dates.json nicht gefunden")
        sys.exit(1)

    if not bundle_file.exists():
        print("❌ assets/index-EXWOLlBA.js nicht gefunden")
        sys.exit(1)

    exam_data = json.loads(exam_file.read_text(encoding="utf-8"))
    registry = json.loads(registry_file.read_text(encoding="utf-8")) if registry_file.exists() else []

    print(f"   📄 {len(exam_data)} IHKs in Scraper-Daten")
    print(f"   📄 {len(registry)} IHKs in Registry")

    # Build entries
    entries = build_entries(exam_data, registry)

    # Remove internal field
    for e in entries:
        del e["_ihk_id"]

    # Stats
    ihk_names = set(e["ihk"] for e in entries)
    print(f"   📊 {len(entries)} Termine von {len(ihk_names)} IHKs")

    # Convert to JS
    js_entries = ",".join(entry_to_js(e) for e in entries)
    new_g1 = f"G1=[{js_entries}]"

    # Read bundle
    js = bundle_file.read_text(encoding="utf-8")

    # Find and replace G1 array
    g1_start = js.index("G1=[")
    depth = 0
    for i in range(g1_start + 3, len(js)):
        if js[i] == "[":
            depth += 1
        elif js[i] == "]":
            depth -= 1
            if depth == 0:
                g1_end = i + 1
                break

    old_count = len(re.findall(r'\{schr:', js[g1_start:g1_end]))
    print(f"   🔄 Ersetze G1: {old_count} → {len(entries)} Einträge")

    js = js[:g1_start] + new_g1 + js[g1_end:]

    # Update m3 (add new IHKs if needed)
    js = update_m3(js, registry)

    # Write bundle
    bundle_file.write_text(js, encoding="utf-8")

    print(f"   ✅ Bundle aktualisiert: {bundle_file}")
    print(f"   📊 Website zeigt jetzt: \"Alle {len(entries)} Prüfungstermine von {len(ihk_names)} IHKs\"")


if __name__ == "__main__":
    main()
