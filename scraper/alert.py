"""
Alert — Email-Benachrichtigungen und Logging

Alert-Logik:
- Email erst nach 2 aufeinanderfolgenden Fails
- Oder wenn sich Termine ändern (neue/entfernte)
- Wöchentliches Health-Summary
- Logfile immer
"""

import json
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from pathlib import Path

LOG_DIR = Path("data/logs")
FAIL_TRACKER = Path("data/cache/fail_tracker.json")


def log_run(results: list, summary: dict):
    """Log scraper run results."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    log_file = LOG_DIR / f"run_{timestamp}.json"
    log_file.write_text(json.dumps({
        "timestamp": datetime.now().isoformat(),
        "summary": summary,
        "results": results,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    return log_file


def check_alerts(results: list) -> list:
    """
    Check results and generate alerts if needed.
    Returns list of alert messages.
    """
    alerts = []
    fail_tracker = _load_fail_tracker()

    for r in results:
        ihk_id = r.get("ihk_id") or r.get("id", "unknown")
        status = r.get("status", "")
        stage = r.get("stage", "")

        if status in ("error", "no_data") and r.get("availability") == "online":
            # Track consecutive failures
            prev_fails = fail_tracker.get(ihk_id, 0)
            fail_tracker[ihk_id] = prev_fails + 1

            if fail_tracker[ihk_id] >= 2:
                alerts.append({
                    "type": "consecutive_failure",
                    "ihk_id": ihk_id,
                    "ihk_name": r.get("name", ihk_id),
                    "fails": fail_tracker[ihk_id],
                    "error": r.get("error", "Keine Daten gefunden"),
                    "message": f"⚠️ {r.get('name', ihk_id)}: {fail_tracker[ihk_id]}x hintereinander fehlgeschlagen. Fehler: {r.get('error', '?')}",
                })
        else:
            # Reset fail counter on success
            fail_tracker[ihk_id] = 0

        # Check for date changes
        warnings = r.get("validation_warnings", [])
        for w in warnings:
            if "NEUE Termine" in w or "ENTFERNTE Termine" in w:
                alerts.append({
                    "type": "date_change",
                    "ihk_id": ihk_id,
                    "ihk_name": r.get("name", ihk_id),
                    "message": f"📅 {r.get('name', ihk_id)}: {w}",
                })

    _save_fail_tracker(fail_tracker)
    return alerts


def send_alert_email(alerts: list, config: dict = None):
    """
    Send alert email.

    Config (from env vars or config dict):
    - ALERT_EMAIL_TO: recipient
    - ALERT_EMAIL_FROM: sender
    - ALERT_SMTP_HOST: SMTP server
    - ALERT_SMTP_PORT: port (default 587)
    - ALERT_SMTP_USER: username
    - ALERT_SMTP_PASS: password
    """
    if not alerts:
        return

    cfg = config or {}
    to_email = cfg.get("to") or os.environ.get("ALERT_EMAIL_TO")
    from_email = cfg.get("from") or os.environ.get("ALERT_EMAIL_FROM")
    smtp_host = cfg.get("smtp_host") or os.environ.get("ALERT_SMTP_HOST")
    smtp_port = int(cfg.get("smtp_port") or os.environ.get("ALERT_SMTP_PORT") or "587")
    smtp_user = cfg.get("smtp_user") or os.environ.get("ALERT_SMTP_USER")
    smtp_pass = cfg.get("smtp_pass") or os.environ.get("ALERT_SMTP_PASS")

    if not all([to_email, from_email, smtp_host]):
        print(f"  ⚠️  Email nicht konfiguriert — {len(alerts)} Alert(s) nur im Log:")
        for a in alerts:
            print(f"     {a['message']}")
        return

    # Build email
    subject = f"🔔 Verwalter-Zertifizierung: {len(alerts)} Alert(s)"
    body = "IHK Prüfungstermin-Scraper Alerts\n"
    body += f"Zeitpunkt: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
    body += "=" * 50 + "\n\n"

    for a in alerts:
        body += f"• {a['message']}\n\n"

    body += "\n---\nAutomatisch generiert von verwalter-zertifizierung.de"

    msg = MIMEMultipart()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            if smtp_user and smtp_pass:
                server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        print(f"  📧 Alert-Email gesendet an {to_email}")
    except Exception as e:
        print(f"  ❌ Email-Fehler: {e}")


def generate_health_summary(results: list) -> dict:
    """Generate a weekly health summary."""
    total = len(results)
    with_data = sum(1 for r in results if len(r.get("dates_2026", [])) > 0 or len(r.get("exam_events", [])) > 0)
    errors = sum(1 for r in results if r.get("status") == "error")
    from_cache = sum(1 for r in results if r.get("from_cache", False))

    return {
        "timestamp": datetime.now().isoformat(),
        "total_ihks": total,
        "with_data": with_data,
        "errors": errors,
        "from_cache": from_cache,
        "coverage_pct": round(with_data / max(total, 1) * 100, 1),
    }


def _load_fail_tracker() -> dict:
    if FAIL_TRACKER.exists():
        try:
            return json.loads(FAIL_TRACKER.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_fail_tracker(tracker: dict):
    FAIL_TRACKER.parent.mkdir(parents=True, exist_ok=True)
    FAIL_TRACKER.write_text(json.dumps(tracker, ensure_ascii=False, indent=2), encoding="utf-8")
