#!/bin/bash
set -e

echo "🚀 Verwalter-Zertifizierung — Pipeline v4"
echo "==========================================="
echo ""

# Install dependencies
echo "📦 Installiere benötigte Pakete..."
pip3 install --quiet --break-system-packages requests beautifulsoup4 2>/dev/null || \
pip3 install --quiet requests beautifulsoup4 2>/dev/null || true

# Optional: pdfplumber for Parser B
pip3 install --quiet --break-system-packages pdfplumber 2>/dev/null || \
pip3 install --quiet pdfplumber 2>/dev/null || true

echo ""

# Run pipeline
echo "🔍 Starte Pipeline..."
echo ""
python3 run.py --skip-browser --skip-llm --skip-discovery "$@"

echo ""
echo "✅ Fertig! Ergebnisse in data/ihk_exam_dates.json"
echo ""
echo "Optionen für nächste Runs:"
echo "  python3 run.py                         # Alles (braucht Playwright + API Key)"
echo "  python3 run.py --skip-browser           # Ohne Browser-Rendering"
echo "  python3 run.py --skip-llm               # Ohne Claude API"
echo "  python3 run.py --skip-discovery          # Ohne URL-Suche"
echo "  python3 run.py --api-key sk-...         # Mit Claude API Key"
echo "  python3 run.py --fresh                  # Cache ignorieren"
