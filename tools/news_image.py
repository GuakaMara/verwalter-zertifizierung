#!/usr/bin/env python3
"""
news_image.py – Automatische Bildsuche für News-Artikel
"""

import os, re, sys, json, base64, hashlib, argparse
from io import BytesIO
from pathlib import Path

import requests
from PIL import Image

BUNDLE_PATH = Path(__file__).parent.parent / "assets" / "index-EXWOLlBA.js"
IMAGE_DIR = Path(__file__).parent.parent / "assets" / "news"
DONE_FILE = Path(__file__).parent.parent / "data" / "news_images_done.json"
IMAGE_WIDTH = 920
IMAGE_HEIGHT = 400
IMAGE_QUALITY = 82
PEXELS_PER_QUERY = 5
CLAUDE_MODEL = "claude-sonnet-4-20250514"

def get_api_keys():
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY")
    pexels_key = os.environ.get("PEXELS_API_KEY")
    if not anthropic_key:
        print("⚠️  ANTHROPIC_API_KEY nicht gesetzt – überspringe Bildsuche")
        sys.exit(0)
    if not pexels_key:
        print("⚠️  PEXELS_API_KEY nicht gesetzt – überspringe Bildsuche")
        sys.exit(0)
    return anthropic_key, pexels_key

def _decode(s):
    try:
        return s.encode("utf-8").decode("unicode_escape")
    except Exception:
        return s

def parse_articles(bundle_text):
    start = bundle_text.find("gu=[")
    if start == -1:
        print("❌ gu-Array nicht gefunden im Bundle")
        sys.exit(0)
    depth = 0
    i = start + 3
    for j, ch in enumerate(bundle_text[i:], start=i):
        if ch == "[": depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                end = j + 1
                break
    gu_raw = bundle_text[start + 3:end]
    articles = []
    pattern = re.compile(
        r'date:"([^"]*)".*?title:"([^"]*)".*?summary:"([^"]*)".*?tag:"([^"]*)".*?image:"([^"]*)".*?body:`([^`]*)`',
        re.DOTALL
    )
    for m in pattern.finditer(gu_raw):
        raw_title = m.group(2)
        try:
            decoded_title = raw_title.encode("utf-8").decode("unicode_escape")
        except Exception:
            decoded_title = raw_title
        articles.append({
            "date": m.group(1),
            "title": decoded_title,
            "title_raw": raw_title,
            "summary": _decode(m.group(3)),
            "tag": _decode(m.group(4)),
            "image": m.group(5),
            "body": _decode(m.group(6))[:500],
        })
    return articles

def needs_new_image(article, done_tracker, force=False):
    if force:
        return True
    # Already processed before -> skip (saves API calls + money)
    title_hash = hashlib.md5(article["title"].encode()).hexdigest()[:12]
    if title_hash in done_tracker:
        return False
    img = article["image"]
    if img.startswith("./assets/news/") or img.startswith("/assets/news/"):
        local_path = BUNDLE_PATH.parent.parent / img.lstrip("./")
        if local_path.exists():
            return False
    return True

def load_done():
    if DONE_FILE.exists():
        try:
            return json.loads(DONE_FILE.read_text())
        except Exception:
            return {}
    return {}

def save_done(done):
    DONE_FILE.parent.mkdir(parents=True, exist_ok=True)
    DONE_FILE.write_text(json.dumps(done, indent=2, ensure_ascii=False))

def mark_done(done, article, image_path=None):
    title_hash = hashlib.md5(article["title"].encode()).hexdigest()[:12]
    done[title_hash] = {
        "title": article["title"][:80],
        "date": article["date"],
        "image": image_path or article["image"],
    }

def claude_api(api_key, messages, system=None, max_tokens=500):
    payload = {"model": CLAUDE_MODEL, "max_tokens": max_tokens, "messages": messages}
    if system:
        payload["system"] = system
    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
        json=payload, timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["content"][0]["text"]

def generate_search_queries(api_key, article):
    prompt = f"""Für diesen deutschen Nachrichtenartikel über Immobilienverwaltung/WEG-Recht brauche ich Suchbegriffe für Stock-Fotos.

Titel: {article['title']}
Zusammenfassung: {article['summary']}
Thema: {article['tag']}

Gib mir genau 3 englische Suchbegriffe (Pexels-kompatibel), die zu einem passenden Headerbild führen. Fokus auf das KONKRETE Thema, nicht generisch.
Jede Query auf einer eigenen Zeile, NICHTS anderes."""
    result = claude_api(api_key, [{"role": "user", "content": prompt}])
    queries = [q.strip() for q in result.strip().split("\n") if q.strip()]
    return queries[:3]

def select_best_image(api_key, article, candidates):
    if not candidates:
        return None
    content = [{"type": "text", "text": f"""Wähle das BESTE Headerbild für diesen Artikel:

Titel: {article['title']}
Zusammenfassung: {article['summary']}
Tag: {article['tag']}

Kriterien: Passt inhaltlich zum Thema, eignet sich als breites Banner (920x400), professionell, gutes Motiv im Zentrum.

Es folgen {len(candidates)} Kandidaten. Antworte NUR mit der Nummer (1-{len(candidates)}) des besten Bildes."""}]
    for i, cand in enumerate(candidates, 1):
        content.append({"type": "text", "text": f"Bild {i}:"})
        content.append({"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": cand["thumb_b64"]}})
    result = claude_api(api_key, [{"role": "user", "content": content}], max_tokens=50)
    match = re.search(r"(\d+)", result)
    if match:
        idx = int(match.group(1)) - 1
        if 0 <= idx < len(candidates):
            return candidates[idx]
    return candidates[0]

def search_pexels(api_key, query, per_page=5):
    resp = requests.get(
        "https://api.pexels.com/v1/search",
        headers={"Authorization": api_key},
        params={"query": query, "per_page": per_page, "orientation": "landscape", "size": "medium"},
        timeout=15,
    )
    resp.raise_for_status()
    results = []
    for photo in resp.json().get("photos", []):
        results.append({
            "id": photo["id"], "width": photo["width"], "height": photo["height"],
            "photographer": photo["photographer"],
            "thumb_url": photo["src"]["medium"], "full_url": photo["src"]["large2x"],
            "pexels_url": photo["url"],
        })
    return results

def download_thumbnail_b64(url):
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    img = Image.open(BytesIO(resp.content))
    img.thumbnail((400, 300), Image.LANCZOS)
    buf = BytesIO()
    img.save(buf, format="JPEG", quality=70)
    return base64.b64encode(buf.getvalue()).decode()

def download_and_save(url, output_path, width=IMAGE_WIDTH, height=IMAGE_HEIGHT):
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    img = Image.open(BytesIO(resp.content)).convert("RGB")
    target_ratio = width / height
    img_ratio = img.width / img.height
    if img_ratio > target_ratio:
        new_w = int(img.height * target_ratio)
        left = (img.width - new_w) // 2
        img = img.crop((left, 0, left + new_w, img.height))
    else:
        new_h = int(img.width / target_ratio)
        top = min(img.height - new_h, int(img.height * 0.2))
        img = img.crop((0, top, img.width, top + new_h))
    img = img.resize((width, height), Image.LANCZOS)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(output_path, format="WEBP", quality=IMAGE_QUALITY)
    return output_path

def update_bundle_image(bundle_path, title_raw, old_image_url, new_image_path):
    data = bundle_path.read_text(encoding="utf-8")
    rel_path = f"./assets/news/{new_image_path.name}"
    pattern = f'title:"{title_raw}"'
    pos = data.find(pattern)
    if pos != -1:
        search_zone = data[pos:pos+500]
        old_ref = f'image:"{old_image_url}"'
        new_ref = f'image:"{rel_path}"'
        if old_ref in search_zone:
            replace_start = pos + search_zone.index(old_ref)
            data = data[:replace_start] + new_ref + data[replace_start+len(old_ref):]
            bundle_path.write_text(data, encoding="utf-8")
            return True
    print(f"   ⚠️ Konnte Bild-Referenz im Bundle nicht updaten")
    return False

def process_article(article, index, anthropic_key, pexels_key, done_tracker, force=False):
    title = article["title"]
    print(f"\n{'='*60}")
    print(f"📰 [{index}] {title}")
    print(f"   Tag: {article['tag']} | Datum: {article['date']}")
    if not needs_new_image(article, done_tracker, force):
        print(f"   ✅ Hat bereits lokales Bild – übersprungen")
        return False
    print(f"   🔍 Generiere Suchbegriffe...")
    queries = generate_search_queries(anthropic_key, article)
    print(f"   → Queries: {queries}")
    print(f"   📷 Suche auf Pexels...")
    all_results = []
    seen_ids = set()
    for q in queries:
        try:
            results = search_pexels(pexels_key, q, per_page=PEXELS_PER_QUERY)
            for r in results:
                if r["id"] not in seen_ids:
                    seen_ids.add(r["id"])
                    all_results.append(r)
        except Exception as e:
            print(f"   ⚠️ Pexels-Fehler bei '{q}': {e}")
    if not all_results:
        print(f"   ❌ Keine Bilder gefunden")
        return False
    print(f"   → {len(all_results)} Kandidaten gefunden")
    print(f"   🖼️ Lade Thumbnails für Auswahl...")
    candidates = []
    for r in all_results[:10]:
        try:
            r["thumb_b64"] = download_thumbnail_b64(r["thumb_url"])
            candidates.append(r)
        except Exception as e:
            print(f"   ⚠️ Thumbnail-Fehler: {e}")
    if not candidates:
        print(f"   ❌ Keine Thumbnails ladbar")
        return False
    print(f"   🤖 Claude wählt bestes Bild aus {len(candidates)} Kandidaten...")
    best = select_best_image(anthropic_key, article, candidates)
    if not best:
        print(f"   ❌ Keine Auswahl möglich")
        return False
    print(f"   → Gewählt: Pexels #{best['id']} von {best['photographer']}")
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()[:50]).strip("-")
    filename = f"{slug}.webp"
    output_path = IMAGE_DIR / filename
    print(f"   💾 Speichere {filename}...")
    download_and_save(best["full_url"], output_path)
    file_size = output_path.stat().st_size / 1024
    print(f"   → {file_size:.0f} KB ({IMAGE_WIDTH}×{IMAGE_HEIGHT} WebP)")
    old_url = article["image"]
    update_bundle_image(BUNDLE_PATH, article["title_raw"], old_url, output_path)
    print(f"   ✅ Bundle aktualisiert")
    print(f"   📎 Fotograf: {best['photographer']} (Pexels)")
    mark_done(done_tracker, article, f"./assets/news/{filename}")
    return True

def main():
    parser = argparse.ArgumentParser(description="Automatische News-Bilder")
    parser.add_argument("--force", action="store_true", help="Alle Bilder neu generieren")
    parser.add_argument("--index", type=int, help="Nur Artikel mit diesem Index")
    parser.add_argument("--dry-run", action="store_true", help="Nur Suchbegriffe zeigen")
    args = parser.parse_args()
    anthropic_key, pexels_key = get_api_keys()
    print("📰 News-Bildautomatik für verwalter-zertifizierung.de")
    print(f"   Bundle: {BUNDLE_PATH}")
    print(f"   Bildordner: {IMAGE_DIR}")
    bundle_text = BUNDLE_PATH.read_text(encoding="utf-8")
    articles = parse_articles(bundle_text)
    print(f"   Artikel gefunden: {len(articles)}")
    done_tracker = load_done()
    print(f"   Bereits verarbeitet: {len(done_tracker)}")
    if args.dry_run:
        for i, a in enumerate(articles):
            print(f"\n[{i}] {a['title']}")
            print(f"    Bild: {a['image'][:70]}...")
            queries = generate_search_queries(anthropic_key, a)
            print(f"    Queries: {queries}")
        return
    updated = 0
    indices = [args.index] if args.index is not None else range(len(articles))
    for i in indices:
        if i >= len(articles):
            print(f"❌ Index {i} existiert nicht (max: {len(articles)-1})")
            continue
        try:
            if process_article(articles[i], i, anthropic_key, pexels_key, done_tracker, args.force):
                updated += 1
                bundle_text = BUNDLE_PATH.read_text(encoding="utf-8")
                articles = parse_articles(bundle_text)
        except Exception as e:
            print(f"   ❌ Fehler: {e}")
    save_done(done_tracker)
    print(f"\n{'='*60}")
    print(f"✅ Fertig! {updated} Bilder aktualisiert.")
    if updated > 0:
        print(f"\n  git add assets/news/ assets/index-EXWOLlBA.js data/news_images_done.json")
        print(f"  git commit -m 'News: Automatische Artikelbilder'")
        print(f"  git push")

if __name__ == "__main__":
    main()
