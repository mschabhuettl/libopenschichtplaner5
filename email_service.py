"""E-Mail service for OpenSchichtplaner5.

Sends HTML notification emails via SMTP.  Configuration is read from
environment variables (see .env.example):

    SP5_SMTP_HOST      – SMTP server hostname (default: empty = disabled)
    SP5_SMTP_PORT      – SMTP port (default: 587)
    SP5_SMTP_USER      – SMTP username
    SP5_SMTP_PASSWORD  – SMTP password
    SP5_SMTP_FROM      – Sender address (default: SP5_SMTP_USER)
    SP5_SMTP_TLS       – "true" for STARTTLS (default), "ssl" for implicit SSL
    SP5_SMTP_ENABLED   – "true" / "false" (default: "true" when host is set)
    SP5_APP_URL        – Base URL for links in emails (e.g. https://sp5.example.com)
"""

from __future__ import annotations

import logging
import os
import smtplib
import threading
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

logger = logging.getLogger("sp5.email")


# ── Configuration ─────────────────────────────────────────────────────────────


class EmailConfig:
    """Read-only snapshot of SMTP settings from environment."""

    def __init__(self) -> None:
        self.host: str = os.environ.get("SP5_SMTP_HOST", "").strip()
        self.port: int = int(os.environ.get("SP5_SMTP_PORT", "587"))
        self.user: str = os.environ.get("SP5_SMTP_USER", "").strip()
        self.password: str = os.environ.get("SP5_SMTP_PASSWORD", "")
        self.from_addr: str = (
            os.environ.get("SP5_SMTP_FROM", "").strip() or self.user
        )
        self.tls_mode: str = os.environ.get("SP5_SMTP_TLS", "true").lower()
        self.app_url: str = os.environ.get("SP5_APP_URL", "http://localhost:8000").rstrip("/")

        _enabled_raw = os.environ.get("SP5_SMTP_ENABLED", "").lower()
        if _enabled_raw in ("true", "1", "yes"):
            self.enabled = True
        elif _enabled_raw in ("false", "0", "no"):
            self.enabled = False
        else:
            # auto: enabled when host is set
            self.enabled = bool(self.host)

    @property
    def is_configured(self) -> bool:
        """Return True if enough config is present to attempt sending."""
        return bool(self.host) and self.enabled

    def to_safe_dict(self) -> dict[str, Any]:
        """Return config without password (for admin display)."""
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "from_addr": self.from_addr,
            "tls_mode": self.tls_mode,
            "enabled": self.enabled,
            "is_configured": self.is_configured,
            "app_url": self.app_url,
        }


def get_config() -> EmailConfig:
    """Return a fresh EmailConfig (re-reads env each time)."""
    return EmailConfig()


# ── HTML template ─────────────────────────────────────────────────────────────

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="de">
<head><meta charset="utf-8">
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         margin: 0; padding: 0; background: #f5f7fa; color: #1e293b; }}
  .wrap {{ max-width: 560px; margin: 24px auto; background: #fff;
           border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,.08); }}
  .header {{ background: #3b82f6; color: #fff; padding: 20px 24px;
             border-radius: 8px 8px 0 0; font-size: 18px; font-weight: 600; }}
  .body {{ padding: 24px; line-height: 1.6; }}
  .body h2 {{ margin: 0 0 8px; font-size: 16px; color: #1e293b; }}
  .body p {{ margin: 0 0 16px; }}
  .cta {{ display: inline-block; background: #3b82f6; color: #fff !important;
          text-decoration: none; padding: 10px 20px; border-radius: 6px;
          font-weight: 500; }}
  .footer {{ padding: 16px 24px; font-size: 12px; color: #94a3b8;
             border-top: 1px solid #e2e8f0; text-align: center; }}
</style></head>
<body>
<div class="wrap">
  <div class="header">📋 OpenSchichtplaner5</div>
  <div class="body">
    <h2>{title}</h2>
    <p>{message}</p>
    {cta_html}
  </div>
  <div class="footer">
    Diese E-Mail wurde automatisch von OpenSchichtplaner5 versendet.<br>
    {app_url}
  </div>
</div>
</body></html>
"""


def _render_html(
    title: str,
    message: str,
    link: str | None = None,
    app_url: str = "",
) -> str:
    cta_html = ""
    if link:
        # Make relative links absolute
        href = link if link.startswith("http") else f"{app_url}{link}"
        cta_html = f'<p><a class="cta" href="{href}">Jetzt ansehen →</a></p>'
    return _HTML_TEMPLATE.format(
        title=title,
        message=message.replace("\n", "<br>"),
        cta_html=cta_html,
        app_url=app_url,
    )


def _render_plain(title: str, message: str, link: str | None = None, app_url: str = "") -> str:
    parts = [title, "", message]
    if link:
        href = link if link.startswith("http") else f"{app_url}{link}"
        parts += ["", f"Link: {href}"]
    parts += ["", "---", f"OpenSchichtplaner5 — {app_url}"]
    return "\n".join(parts)


# ── Sending ───────────────────────────────────────────────────────────────────


def send_email(
    *,
    to: str,
    subject: str,
    title: str,
    message: str,
    link: str | None = None,
    config: EmailConfig | None = None,
) -> bool:
    """Send a single email. Returns True on success, False on failure.

    This is a **blocking** call (network I/O).  Use ``send_email_async``
    for fire-and-forget from request handlers.
    """
    cfg = config or get_config()
    if not cfg.is_configured:
        logger.debug("Email not configured – skipping send to %s", to)
        return False

    msg = MIMEMultipart("alternative")
    msg["From"] = cfg.from_addr
    msg["To"] = to
    msg["Subject"] = subject

    plain = _render_plain(title, message, link, cfg.app_url)
    html = _render_html(title, message, link, cfg.app_url)
    msg.attach(MIMEText(plain, "plain", "utf-8"))
    msg.attach(MIMEText(html, "html", "utf-8"))

    try:
        if cfg.tls_mode == "ssl":
            with smtplib.SMTP_SSL(cfg.host, cfg.port, timeout=15) as srv:
                if cfg.user:
                    srv.login(cfg.user, cfg.password)
                srv.send_message(msg)
        else:
            with smtplib.SMTP(cfg.host, cfg.port, timeout=15) as srv:
                if cfg.tls_mode in ("true", "starttls", "1", "yes"):
                    srv.starttls()
                if cfg.user:
                    srv.login(cfg.user, cfg.password)
                srv.send_message(msg)
        logger.info("Email sent to %s: %s", to, subject)
        return True
    except Exception:
        logger.exception("Failed to send email to %s", to)
        return False


def send_email_async(
    *,
    to: str,
    subject: str,
    title: str,
    message: str,
    link: str | None = None,
) -> None:
    """Fire-and-forget email send in a background thread."""
    t = threading.Thread(
        target=send_email,
        kwargs=dict(to=to, subject=subject, title=title, message=message, link=link),
        daemon=True,
    )
    t.start()


# ── Notification-Email bridge ─────────────────────────────────────────────────

# Notification type → email subject prefix
_SUBJECT_MAP: dict[str, str] = {
    "shift_change": "Schichtänderung",
    "swap_request": "Tauschanfrage",
    "swap_approved": "Tausch genehmigt",
    "swap_rejected": "Tausch abgelehnt",
    "vacation_request": "Urlaubsantrag",
    "vacation_approved": "Urlaub genehmigt",
    "vacation_rejected": "Urlaub abgelehnt",
    "absence_status": "Urlaubsantrag-Status",
    "absence": "Abwesenheit",
    "general": "Benachrichtigung",
    "info": "Information",
    "warning": "Warnung",
}


def send_notification_email(
    *,
    notification_type: str,
    title: str,
    message: str,
    recipient_email: str | None,
    link: str | None = None,
) -> None:
    """Send email for a notification if email is configured and recipient has an address.

    Called from create_notification(). Non-blocking (async thread).
    """
    if not recipient_email:
        return
    cfg = get_config()
    if not cfg.is_configured:
        return
    prefix = _SUBJECT_MAP.get(notification_type, "Benachrichtigung")
    subject = f"[SP5] {prefix}: {title}"
    send_email_async(to=recipient_email, subject=subject, title=title, message=message, link=link)
