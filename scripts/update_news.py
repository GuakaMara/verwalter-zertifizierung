#!/usr/bin/env python3
"""
Automatischer News-Updater für verwalter-zertifizierung.de
===========================================================

Läuft täglich via GitHub Actions:
1. RSS-Feeds scannen (kostenlos)
2. Keyword-Filter (kostenlos)
3. Claude Sonnet bewertet Relevanz (< $0.01/Tag)
4. Claude Sonnet schreibt Artikel (nur bei Treffern)
5. JS-Bundle aktualisieren, committen, pushen

Kosten: ~$0.15–0.30/Monat
"""

import json
import re
import os
import sys
import hashlib
import requests
from datetime import datetime, timedelta
from pathlib import Path
from xml.etree import ElementTree

# ─── Konfiguration ──────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
JS_FILE = ROOT / "assets" / "index-EXWOLlBA.js"
SEEN_FILE = ROOT / "data" / "news_seen.json"
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL = "claude-sonnet-4-20250514"
RELEVANCE_THRESHOLD = 7          # 1-10, nur 7+ werden Artikel
MAX_ARTICLES_PER_RUN = 3         # Maximal 3 neue Artikel pro Lauf
MAX_ARTICLES_TOTAL = 20          # Älteste fliegen raus
MAX_AGE_DAYS = 365               # Artikel älter als 1 Jahr entfernen

# Kategorie-Bilder (Pexels, lizenzfrei)
IMAGES = {
    "Gesetzgebung": "https://images.pexels.com/photos/109629/pexels-photo-109629.jpeg?auto=compress&cs=tinysrgb&w=800&h=300&fit=crop",
    "Rechtsprechung": "https://images.pexels.com/photos/5668882/pexels-photo-5668882.jpeg?auto=compress&cs=tinysrgb&w=800&h=300&fit=crop",
    "Praxis": "https://images.pexels.com/photos/323780/pexels-photo-323780.jpeg?auto=compress&cs=tinysrgb&w=800&h=300&fit=crop",
}

COLORS = {
    "Gesetzgebung": ("#1d4ed8", "#eff6ff"),
    "Rechtsprechung": ("#7c3aed", "#f5f3ff"),
    "Praxis": ("#059669", "#ecfdf5"),
}

# ─── RSS-Quellen ────────────────────────────────────────────────────
RSS_FEEDS = [
    {
        "name": "BGH Pressemitteilungen",
        "url": "https://juris.bundesgerichtshof.de/cgi-bin/rechtsprechung/rss.py?Gericht=bgh&Art=pm",
        "type": "rss",
    },
    {
        "name": "Haufe Immobilien",
        "url": "https://www.haufe.de/xml/rss_129130.xml",
        "type": "rss",
    },
]

# Schlüsselwörter für Vorfilter (mindestens 1 muss vorkommen)
KEYWORDS = [
    "weg", "wohnungseigent", "eigentümergemeinschaft", "eigentuemergemeinschaft",
    "verwalter", "verwaltung", "teilungserkl", "sondereigentum",
    "gemeinschaftseigentum", "hausgeld", "wirtschaftsplan", "erhaltung",
    "instandhaltung", "instandsetzung", "beschlussanfechtung",
    "wohnungseigentumsgesetz", "wemog", "§ 26a", "§26a", "34c gewo",
    "geg", "gebäudeenergiegesetz", "gebaeudeenergiegesetz", "heizungsgesetz",
    "wärmeplanung", "waermeplanung", "co2-kostenaufteilung", "co2kostenaufteilung",
    "heizkostenverordnung", "energieausweis",
    "bgh v zr", "v zr", "weg-reform",
    "zertifizierter verwalter", "weiterbildungspflicht",
    "balkonsanierung", "fassadensanierung", "aufzug",
    "eigentümerversammlung", "eigentuemer",
]


# ─── Hilfsfunktionen ────────────────────────────────────────────────

def load_seen():
    """Bereits verarbeitete Artikel laden."""
    if SEEN_FILE.exists():
        return json.loads(SEEN_FILE.read_text())
    return {}


def save_seen(seen: dict):
    """Verarbeitete Artikel speichern."""
    SEEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    SEEN_FILE.write_text(json.dumps(seen, indent=2, ensure_ascii=False))


def article_hash(title: str, url: str) -> str:
    """Eindeutiger Hash für einen Artikel."""
    return hashlib.md5(f"{title}|{url}".encode()).hexdigest()[:12]


def fetch_rss(feed: dict) -> list:
    """RSS-Feed abrufen und Artikel extrahieren."""
    articles = []
    try:
        resp = requests.get(feed["url"], timeout=15, headers={
            "User-Agent": "VerwalterZertifizierung-NewsBot/1.0"
        })
        resp.raise_for_status()
        root = ElementTree.fromstring(resp.content)

        # RSS 2.0
        for item in root.findall(".//item"):
            title = item.findtext("title", "").strip()
            link = item.findtext("link", "").strip()
            desc = item.findtext("description", "").strip()
            pub_date = item.findtext("pubDate", "").strip()

            if title and link:
                articles.append({
                    "title": title,
                    "url": link,
                    "description": desc,
                    "pub_date": pub_date,
                    "source_name": feed["name"],
                })

        # Atom
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        for entry in root.findall(".//atom:entry", ns):
            title = entry.findtext("atom:title", "", ns).strip()
            link_el = entry.find("atom:link", ns)
            link = link_el.get("href", "") if link_el is not None else ""
            desc = entry.findtext("atom:summary", "", ns).strip()

            if title and link:
                articles.append({
                    "title": title,
                    "url": link,
                    "description": desc,
                    "pub_date": entry.findtext("atom:updated", "", ns),
                    "source_name": feed["name"],
                })

    except Exception as e:
        print(f"  ⚠️  Feed-Fehler ({feed['name']}): {e}")

    return articles


def keyword_match(article: dict) -> bool:
    """Prüft ob mindestens ein Keyword im Titel oder der Beschreibung vorkommt."""
    text = f"{article['title']} {article['description']}".lower()
    return any(kw in text for kw in KEYWORDS)


def call_claude(system: str, prompt: str, max_tokens: int = 1500) -> str:
    """Claude API aufrufen."""
    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": MODEL,
            "max_tokens": max_tokens,
            "system": system,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    return data["content"][0]["text"]


def evaluate_relevance(article: dict) -> int:
    """Claude bewertet die Relevanz eines Artikels (1-10)."""
    system = (
        "Du bist ein Experte für WEG-Verwaltung in Deutschland. "
        "Bewerte ob der folgende Artikel für einen WEG-Verwalter in der täglichen Praxis relevant ist. "
        "Antworte NUR mit einer Zahl von 1-10 und einem kurzen Satz Begründung. "
        "Format: SCORE: [Zahl]\\nGRUND: [Begründung]\\n"
        "10 = direkt praxisrelevant (neues BGH-Urteil zu WEG, Gesetzesänderung die Verwalter betrifft)\\n"
        "7 = relevant (betrifft Immobilienverwaltung allgemein)\\n"
        "4 = am Rande relevant\\n"
        "1 = irrelevant für WEG-Verwalter"
    )
    prompt = f"Titel: {article['title']}\n\nBeschreibung: {article['description']}\n\nQuelle: {article['source_name']}"

    try:
        result = call_claude(system, prompt, max_tokens=150)
        match = re.search(r"SCORE:\s*(\d+)", result)
        if match:
            score = int(match.group(1))
            print(f"    Score: {score}/10 — {result.split('GRUND:')[-1].strip()[:80]}")
            return min(score, 10)
    except Exception as e:
        print(f"    ⚠️  Bewertungsfehler: {e}")

    return 0


def write_article(article: dict) -> dict | None:
    """Claude schreibt einen News-Artikel basierend auf dem Quellmaterial."""
    system = """Du schreibst kurze, sachliche Nachrichtenartikel für die Website verwalter-zertifizierung.de.
Zielgruppe: WEG-Verwalter in Deutschland.

REGELN:
- Schreibe NUR basierend auf den gegebenen Informationen. Erfinde NICHTS dazu.
- Aktenzeichen, Paragraphen und Daten müssen exakt aus der Quelle stammen.
- 3-4 Absätze, getrennt durch \\n\\n
- Der letzte Absatz muss immer die praktische Relevanz für WEG-Verwalter erklären (beginne mit "Für WEG-Verwalter..." oder "Für Verwalter...")
- Sachlicher Stil, kein Marketing, keine Übertreibungen
- Deutsche Sprache

Antworte im folgenden JSON-Format (und NUR JSON, kein anderer Text):
{
  "title": "Kurzer, prägnanter Titel (max 80 Zeichen)",
  "summary": "Zusammenfassung in 1-2 Sätzen (max 160 Zeichen)",
  "category": "Gesetzgebung" oder "Rechtsprechung" oder "Praxis",
  "body": "Der vollständige Artikeltext mit \\n\\n zwischen Absätzen",
  "source": "Exakte Quellenangabe mit Aktenzeichen/Dokumentennummer"
}"""

    prompt = f"""Schreibe einen Nachrichtenartikel basierend auf dieser Quelle:

Titel: {article['title']}
Beschreibung: {article['description']}
Quelle: {article['source_name']}
URL: {article['url']}

Schreibe den Artikel. Antworte NUR mit dem JSON-Objekt."""

    try:
        result = call_claude(system, prompt, max_tokens=1500)

        # JSON extrahieren (mit oder ohne Code-Block)
        json_str = result.strip()
        if json_str.startswith("```"):
            json_str = re.sub(r"^```(?:json)?\s*", "", json_str)
            json_str = re.sub(r"\s*```$", "", json_str)

        data = json.loads(json_str)

        # Validierung
        required = ["title", "summary", "category", "body", "source"]
        if not all(k in data for k in required):
            print(f"    ⚠️  Unvollständiges JSON: fehlende Felder")
            return None

        if data["category"] not in IMAGES:
            data["category"] = "Praxis"

        return data

    except json.JSONDecodeError as e:
        print(f"    ⚠️  JSON-Parsing-Fehler: {e}")
        print(f"    Response (first 200): {result[:200]}")
        return None
    except Exception as e:
        print(f"    ⚠️  Artikelschreib-Fehler: {e}")
        return None


def js_escape(text: str) -> str:
    """Text für JS-Einbettung escapen."""
    # Backticks und Dollar-Signs escapen (wir nutzen Template Literals)
    text = text.replace("\\", "\\\\")
    text = text.replace("`", "\\`")
    text = text.replace("${", "\\${")
    # Deutsche Umlaute als Unicode
    replacements = {
        "ä": "\\u00e4", "ö": "\\u00f6", "ü": "\\u00fc",
        "Ä": "\\u00c4", "Ö": "\\u00d6", "Ü": "\\u00dc",
        "ß": "\\u00df", "€": "\\u20ac",
        "–": "\\u2013", "—": "\\u2014",
        "„": "\\u201e", """: "\\u201c", """: "\\u201d",
        "‚": "\\u201a", "'": "\\u2018", "'": "\\u2019",
        "…": "\\u2026", "§": "\\u00a7", "²": "\\u00b2",
        "³": "\\u00b3", "°": "\\u00b0", "×": "\\u00d7",
        "→": "\\u2192", "←": "\\u2190",
        "₂": "\\u2082",
    }
    for char, escape in replacements.items():
        text = text.replace(char, escape)
    return text


def article_to_js(article_data: dict, date_str: str) -> str:
    """Konvertiert einen Artikel-Dict in einen JS-Objekt-String."""
    cat = article_data["category"]
    color, bg = COLORS.get(cat, ("#059669", "#ecfdf5"))
    image = IMAGES.get(cat, IMAGES["Praxis"])

    title = js_escape(article_data["title"])
    summary = js_escape(article_data["summary"])
    body = js_escape(article_data["body"])
    source = js_escape(article_data["source"])

    return (
        "{"
        f'date:"{date_str}",'
        f'title:"{title}",'
        f'summary:"{summary}",'
        f'tag:"{cat}",'
        f'color:"{color}",'
        f'bg:"{bg}",'
        f'image:"{image}",'
        f'body:`{body}`,'
        f'source:"{source}"'
        "}"
    )


def parse_date(date_str: str) -> datetime:
    """DD.MM.YYYY String zu datetime."""
    try:
        return datetime.strptime(date_str, "%d.%m.%Y")
    except ValueError:
        return datetime.min


def update_bundle(new_articles_js: list[str]):
    """Neue Artikel ins JS-Bundle einfügen, alte entfernen."""
    content = JS_FILE.read_text()

    # Bestehendes Array extrahieren
    match = re.search(r'const gu=\[(.*?)\];', content, re.DOTALL)
    if not match:
        print("FEHLER: News-Array 'const gu=[...]' nicht gefunden!")
        sys.exit(1)

    old_array_str = match.group(1)

    # Bestehende Artikel einzeln parsen (grob)
    existing = []
    depth = 0
    current = ""
    for char in old_array_str:
        if char == '{' and depth == 0:
            current = char
            depth = 1
        elif char == '{':
            current += char
            depth += 1
        elif char == '}' and depth == 1:
            current += char
            depth = 0
            existing.append(current)
        elif depth > 0:
            current += char
            if char == '`':
                # Toggle template literal
                pass

    print(f"  Bestehende Artikel: {len(existing)}")

    # Neue Artikel vorne einfügen
    all_articles = new_articles_js + existing

    # Nach Datum sortieren (neueste zuerst)
    def extract_date(js_str):
        m = re.search(r'date:"(\d{2}\.\d{2}\.\d{4})"', js_str)
        if m:
            return parse_date(m.group(1))
        return datetime.min

    all_articles.sort(key=extract_date, reverse=True)

    # Alte Artikel entfernen (> MAX_AGE_DAYS)
    cutoff = datetime.now() - timedelta(days=MAX_AGE_DAYS)
    all_articles = [a for a in all_articles if extract_date(a) > cutoff]

    # Maximal MAX_ARTICLES_TOTAL behalten
    all_articles = all_articles[:MAX_ARTICLES_TOTAL]

    # Duplikate entfernen (gleicher Titel)
    seen_titles = set()
    deduped = []
    for a in all_articles:
        m = re.search(r'title:"(.*?)"', a)
        title = m.group(1) if m else a[:50]
        if title not in seen_titles:
            seen_titles.add(title)
            deduped.append(a)
    all_articles = deduped

    print(f"  Artikel nach Bereinigung: {len(all_articles)}")

    # Neues Array zusammenbauen
    new_array = "const gu=[" + ",".join(all_articles) + "];"
    content = content[:match.start()] + new_array + content[match.end():]

    # useState aktualisieren: immer alle Artikel zeigen (bis 8)
    display_count = min(len(all_articles), 8)
    content = re.sub(
        r'\[D,T\]=pe\.useState\(\d+\)',
        f'[D,T]=pe.useState({display_count})',
        content,
        count=1,
    )

    JS_FILE.write_text(content)
    print(f"  Bundle aktualisiert ({len(all_articles)} Artikel, Display: {display_count})")


# ─── Hauptprogramm ──────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("📰 News-Updater gestartet")
    print(f"   {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print("=" * 60)

    if not ANTHROPIC_API_KEY:
        print("⚠️  ANTHROPIC_API_KEY nicht gesetzt — nur RSS-Scan ohne Bewertung")

    # Bereits gesehene Artikel laden
    seen = load_seen()
    print(f"\n📋 {len(seen)} bereits verarbeitete Artikel in Historie")

    # ── Stufe 1: RSS-Feeds scannen ──
    print("\n🔍 Stufe 1: RSS-Feeds scannen...")
    all_items = []
    for feed in RSS_FEEDS:
        items = fetch_rss(feed)
        print(f"  {feed['name']}: {len(items)} Artikel")
        all_items.extend(items)

    if not all_items:
        print("  Keine Artikel gefunden — Ende.")
        return

    # Bereits gesehene filtern
    new_items = []
    for item in all_items:
        h = article_hash(item["title"], item["url"])
        if h not in seen:
            new_items.append(item)

    print(f"\n  {len(new_items)} neue Artikel (von {len(all_items)} gesamt)")

    if not new_items:
        print("  Alles bereits verarbeitet — Ende.")
        return

    # ── Stufe 2: Keyword-Filter ──
    print("\n🔑 Stufe 2: Keyword-Filter...")
    filtered = [a for a in new_items if keyword_match(a)]
    print(f"  {len(filtered)} Artikel passieren Keyword-Filter (von {len(new_items)})")

    # Alle neuen als "gesehen" markieren (auch die ohne Keywords)
    for item in new_items:
        h = article_hash(item["title"], item["url"])
        seen[h] = {
            "title": item["title"],
            "url": item["url"],
            "date": datetime.now().isoformat(),
            "passed_keywords": keyword_match(item),
        }

    if not filtered:
        save_seen(seen)
        print("  Keine relevanten Artikel — Ende.")
        return

    if not ANTHROPIC_API_KEY:
        save_seen(seen)
        print("  Kein API-Key — überspringe Bewertung und Artikelerstellung.")
        return

    # ── Stufe 3: Claude bewertet Relevanz ──
    print("\n🤖 Stufe 3: Claude bewertet Relevanz...")
    scored = []
    for item in filtered:
        print(f"  📄 {item['title'][:60]}...")
        score = evaluate_relevance(item)
        seen[article_hash(item["title"], item["url"])]["score"] = score
        if score >= RELEVANCE_THRESHOLD:
            scored.append((score, item))

    scored.sort(key=lambda x: x[0], reverse=True)
    top = scored[:MAX_ARTICLES_PER_RUN]

    print(f"\n  {len(scored)} Artikel über Schwelle {RELEVANCE_THRESHOLD}")
    print(f"  {len(top)} werden geschrieben (Max: {MAX_ARTICLES_PER_RUN})")

    if not top:
        save_seen(seen)
        print("  Keine Artikel über Relevanz-Schwelle — Ende.")
        return

    # ── Stufe 4: Artikel schreiben ──
    print("\n✍️  Stufe 4: Artikel schreiben...")
    new_js_articles = []
    today = datetime.now().strftime("%d.%m.%Y")

    for score, item in top:
        print(f"  📝 Schreibe: {item['title'][:50]}... (Score: {score})")
        article_data = write_article(item)

        if article_data:
            js_str = article_to_js(article_data, today)
            new_js_articles.append(js_str)
            seen[article_hash(item["title"], item["url"])]["published"] = True
            print(f"    ✅ '{article_data['title'][:50]}' ({article_data['category']})")
        else:
            print(f"    ❌ Konnte nicht geschrieben werden")

    save_seen(seen)

    if not new_js_articles:
        print("\n  Keine Artikel erfolgreich geschrieben — Ende.")
        return

    # ── Stufe 5: Bundle aktualisieren ──
    print(f"\n📦 Stufe 5: Bundle aktualisieren ({len(new_js_articles)} neue Artikel)...")
    update_bundle(new_js_articles)

    print(f"\n{'=' * 60}")
    print(f"✅ Fertig! {len(new_js_articles)} neue Artikel veröffentlicht.")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
