# Verwalter-Zertifizierung — IHK Prüfungstermin-Scraper v4

## Schnellstart

```bash
chmod +x start.sh
./start.sh
```

## Pipeline-Architektur

Für jede IHK läuft automatisch diese Fallback-Kette:

```
Stufe 1: Parser A  → HTML-Tabellen + Textblöcke (BeautifulSoup)
Stufe 2: Parser C  → Browser-Rendering für JS-Formulare (Playwright)
Stufe 3: Discovery → Automatische URL-Suche bei Fehler
Stufe 4: Claude    → LLM-Extraktion mit Guardrails
Stufe 5: Degrade   → Alte Daten behalten + Alert
```

Jedes Ergebnis durchläuft den **Validator** (Datum gültig? In der Zukunft? Frist vor Prüfung?).

## Module

| Datei | Funktion |
|---|---|
| `run.py` | Hauptprogramm + Registry |
| `scraper/pipeline.py` | Orchestrator (Fallback-Kette) |
| `scraper/parser_a.py` | HTML-Parser (deterministic) |
| `scraper/parser_b.py` | PDF-Parser (pdfplumber) |
| `scraper/parser_c.py` | Browser-Parser (Playwright) |
| `scraper/parser_llm.py` | Claude API Extraktion |
| `scraper/validator.py` | Strenge Validierung |
| `scraper/cache.py` | ETag/If-Modified-Since Caching |
| `scraper/alert.py` | Email-Alerts + Logging |
| `scraper/source_discovery.py` | Automatische URL-Suche |

## Optionen

```bash
python3 run.py                          # Standard (Parser A only)
python3 run.py --skip-browser           # Ohne Playwright
python3 run.py --skip-llm              # Ohne Claude API
python3 run.py --skip-discovery        # Ohne URL-Suche
python3 run.py --api-key sk-ant-...    # Mit Claude API
python3 run.py --fresh                 # Cache ignorieren
```

## Für Playwright (optional)

```bash
pip3 install playwright
python3 -m playwright install chromium
```
