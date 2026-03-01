"""
Source Discovery — Automatische URL-Suche

Wenn eine IHK-Seite nicht mehr erreichbar ist oder sich die URL geändert hat,
sucht dieses Modul automatisch nach der neuen Seite.

Strategie:
1. Google-Suche: site:ihk.de "zertifizierter Verwalter" "Prüfungstermine" "{IHK-Name}"
2. Kandidaten-URLs testen
3. Beste URL vorschlagen (aber NICHT automatisch übernehmen)
"""

import re
from .models import ScrapeResult
from .parser_a import extract_all_dates, EXAM_KEYWORDS

try:
    import requests
except ImportError:
    requests = None

HEADERS = {
    "User-Agent": "Verwalter-Zertifizierung.de/1.0 (+https://verwalter-zertifizierung.de)",
    "Accept": "text/html",
    "Accept-Language": "de-DE,de;q=0.9",
}

# Known IHK domain patterns
IHK_DOMAINS = [
    "ihk.de", "ihk24.de",
    # Regional domains
    "reutlingen.ihk.de", "weingarten.ihk.de", "ostwuerttemberg.ihk.de",
]


def discover_url(ihk_name: str, ihk_city: str, ihk_id: str,
                 current_url: str = "", api_key: str = None) -> dict:
    """
    Try to find the correct URL for an IHK's exam page.

    Returns dict with:
    - candidates: list of {url, score, keyword_hits, has_2026_dates}
    - best_url: the highest-scoring candidate (or None)
    - needs_human_review: True (always — never auto-update)
    """
    result = {
        "ihk_id": ihk_id,
        "current_url": current_url,
        "candidates": [],
        "best_url": None,
        "needs_human_review": True,
        "error": None,
    }

    if not requests:
        result["error"] = "requests nicht installiert"
        return result

    # Build search queries
    search_queries = [
        f'site:ihk.de "zertifizierter Verwalter" "Prüfungstermine" "{ihk_city}"',
        f'site:ihk.de "zertifizierter Verwalter" "{ihk_name}"',
        f'site:ihk.de "§ 26a" "Prüfung" "{ihk_city}"',
        f'"zertifizierter Wohnimmobilienverwalter" "Prüfung" "{ihk_city}" IHK',
    ]

    # Try DuckDuckGo HTML search (no API key needed)
    candidate_urls = set()

    for query in search_queries[:2]:  # Limit to 2 queries to be polite
        urls = _search_duckduckgo(query)
        candidate_urls.update(urls)

    # Also try common IHK URL patterns
    slug = ihk_id.replace("ihk-", "")
    pattern_urls = [
        f"https://www.ihk.de/{slug}/",
        f"https://www.{slug}.ihk.de/",
    ]
    for base in pattern_urls:
        for suffix in [
            "sachkundepruefungen/zertifizierter-verwalter",
            "pruefungen/zertifizierter-verwalter",
            "recht/vermittlergewerbe/zertifizierter-wohnimmobilienverwalter",
            "bildung/pruefungen/zertifizierter-verwalter",
        ]:
            candidate_urls.add(base + suffix)

    # Test each candidate
    for url in candidate_urls:
        if url == current_url:
            continue

        score = _test_candidate(url)
        if score and score["keyword_hits"] > 2:
            result["candidates"].append(score)

    # Sort by score
    result["candidates"].sort(key=lambda x: (
        x.get("has_2026_dates", False),
        x.get("keyword_hits", 0),
    ), reverse=True)

    if result["candidates"]:
        result["best_url"] = result["candidates"][0]["url"]

    return result


def _search_duckduckgo(query: str, max_results: int = 5) -> list:
    """Search DuckDuckGo and return URLs."""
    urls = []
    try:
        resp = requests.get(
            "https://html.duckduckgo.com/html/",
            params={"q": query},
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; Verwalter-Zertifizierung/1.0)",
            },
            timeout=10,
        )
        if resp.status_code == 200:
            # Extract URLs from results
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.text, "html.parser")
            for link in soup.find_all("a", class_="result__a"):
                href = link.get("href", "")
                if "ihk" in href.lower():
                    # DuckDuckGo redirects — try to extract actual URL
                    actual = re.search(r"uddg=([^&]+)", href)
                    if actual:
                        import urllib.parse
                        urls.append(urllib.parse.unquote(actual.group(1)))
                    elif href.startswith("http"):
                        urls.append(href)
    except Exception:
        pass

    return urls[:max_results]


def _test_candidate(url: str) -> dict:
    """Test a candidate URL for relevance."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10, allow_redirects=True)
        if resp.status_code != 200:
            return None

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text(separator=" ", strip=True).lower()

        keyword_hits = sum(1 for kw in EXAM_KEYWORDS if kw in text)
        all_dates = extract_all_dates(resp.text)
        has_2026 = any(d["year"] == "2026" for d in all_dates)

        return {
            "url": resp.url,  # Use final URL after redirects
            "keyword_hits": keyword_hits,
            "has_2026_dates": has_2026,
            "status_code": resp.status_code,
            "content_length": len(resp.text),
        }
    except Exception:
        return None
