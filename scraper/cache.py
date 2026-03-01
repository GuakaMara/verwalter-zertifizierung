"""
Cache — ETag / If-Modified-Since Caching

Spart Bandbreite und reduziert Last auf IHK-Server.
Speichert pro URL: ETag, Last-Modified, letzter HTML-Content, letztes Ergebnis.
"""

import json
import hashlib
from pathlib import Path
from datetime import datetime

try:
    import requests
except ImportError:
    requests = None

CACHE_DIR = Path("data/cache")

HEADERS_BASE = {
    "User-Agent": "Verwalter-Zertifizierung.de/1.0 (+https://verwalter-zertifizierung.de)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.5",
}


def fetch_with_cache(url: str, timeout: int = 15) -> dict:
    """
    Fetch a URL with ETag/If-Modified-Since caching.

    Returns dict with:
    - html: page content (or None on error)
    - status_code: HTTP status
    - from_cache: True if 304 Not Modified
    - error: error message (or None)
    - content_length: length of content
    - content_type: Content-Type header
    """
    result = {
        "url": url,
        "html": None,
        "status_code": None,
        "from_cache": False,
        "error": None,
        "content_length": 0,
        "content_type": "",
    }

    if not requests:
        result["error"] = "requests nicht installiert"
        return result

    # Load cache entry
    cache_entry = _load_cache(url)
    headers = dict(HEADERS_BASE)

    if cache_entry:
        if cache_entry.get("etag"):
            headers["If-None-Match"] = cache_entry["etag"]
        if cache_entry.get("last_modified"):
            headers["If-Modified-Since"] = cache_entry["last_modified"]

    try:
        resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        result["status_code"] = resp.status_code
        result["content_type"] = resp.headers.get("Content-Type", "")

        if resp.status_code == 304:
            # Not modified — use cached content
            result["html"] = cache_entry.get("html", "")
            result["from_cache"] = True
            result["content_length"] = len(result["html"])
            return result

        if resp.status_code == 200:
            result["html"] = resp.text
            result["content_length"] = len(resp.text)

            # Update cache
            _save_cache(url, {
                "etag": resp.headers.get("ETag"),
                "last_modified": resp.headers.get("Last-Modified"),
                "html": resp.text,
                "fetched_at": datetime.now().isoformat(),
                "status_code": 200,
            })
        else:
            result["error"] = f"HTTP {resp.status_code}"

    except requests.exceptions.Timeout:
        result["error"] = "TIMEOUT"
        # Return cached content on timeout
        if cache_entry and cache_entry.get("html"):
            result["html"] = cache_entry["html"]
            result["from_cache"] = True
            result["content_length"] = len(result["html"])
    except requests.exceptions.ConnectionError as e:
        result["error"] = f"CONNECTION_ERROR: {str(e)[:100]}"
        if cache_entry and cache_entry.get("html"):
            result["html"] = cache_entry["html"]
            result["from_cache"] = True
    except Exception as e:
        result["error"] = f"ERROR: {str(e)[:100]}"

    return result


def get_cached_result(ihk_id: str) -> dict:
    """Load previous scrape result for an IHK."""
    path = CACHE_DIR / f"result_{ihk_id}.json"
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None


def save_result(ihk_id: str, result: dict):
    """Save scrape result for future comparison."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / f"result_{ihk_id}.json"
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")


def _url_hash(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:16]


def _load_cache(url: str) -> dict:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / f"page_{_url_hash(url)}.json"
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None


def _save_cache(url: str, entry: dict):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / f"page_{_url_hash(url)}.json"
    entry["url"] = url
    path.write_text(json.dumps(entry, ensure_ascii=False, indent=2), encoding="utf-8")
