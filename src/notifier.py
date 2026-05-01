"""Ilmoitukset Slack-webhookilla ja sähköpostilla.

.env-muuttujat (kaikki valinnaisia):
  SLACK_WEBHOOK_URL  — Slack incoming webhook URL
  SMTP_HOST          — esim. smtp.gmail.com
  SMTP_PORT          — oletus 587
  SMTP_USER          — lähettäjän sähköposti
  SMTP_PASS          — salasana / app password
  NOTIFY_EMAIL       — vastaanottajan sähköposti (oletus = SMTP_USER)
"""

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env", override=True)


def send_slack(text: str) -> None:
    url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not url:
        return
    import requests
    r = requests.post(url, json={"text": text}, timeout=10)
    if r.status_code == 200:
        print("  [Slack] Viesti lähetetty.")
    else:
        print(f"  [Slack] Virhe {r.status_code}: {r.text[:100]}")


def send_email(subject: str, body: str) -> None:
    host = os.getenv("SMTP_HOST", "")
    if not host:
        return
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER", "")
    pwd = os.getenv("SMTP_PASS", "")
    to = os.getenv("NOTIFY_EMAIL", user)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = to
    msg.attach(MIMEText(body, "plain", "utf-8"))

    with smtplib.SMTP(host, port) as s:
        s.ehlo()
        s.starttls()
        s.login(user, pwd)
        s.sendmail(user, to, msg.as_string())
    print(f"  [Email] Lähetetty → {to}")


def send(subject: str, body: str) -> None:
    """Lähetä ilmoitus kaikille konfiguroiduille kanaville."""
    send_slack(f"*{subject}*\n\n{body}")
    send_email(subject, body)
