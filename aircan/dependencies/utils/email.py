"""Email sending utilities."""

import html as _html
import json
import smtplib
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from airflow.sdk import BaseHook


def _meta_row(label: str, value: str, *, mono: bool = False) -> str:
    """Render a labelled metadata row used inside the alert email summary table."""
    if not value:
        return ""
    safe = _html.escape(str(value))
    value_font = (
        'font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;'
        if mono
        else 'font-size:15px;'
    )
    return f"""
                <tr>
                  <td class="border" style="padding:12px 0;border-bottom:1px solid #eef0f3;">
                    <div class="text-secondary" style="color:#6b7280;font-size:12px;letter-spacing:.04em;text-transform:uppercase;margin-bottom:4px;">{_html.escape(label)}</div>
                    <div class="text-primary" style="color:#111827;{value_font}line-height:1.5;word-break:break-word;">{safe}</div>
                  </td>
                </tr>"""


def build_alert_html(resource_id: str, error_payload: dict) -> str:
    """Render the failure-alert email. Supports light + dark mode."""
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    error_message = error_payload.get("message", "") or ""
    run_id = error_payload.get("run_id", "")

    # The message often contains its own newlines (BQ summaries with bullet
    # points). Preserve them with white-space: pre-wrap inside the message block.
    safe_message = _html.escape(error_message)

    meta_rows = (
        _meta_row("Resource ID", resource_id, mono=True)
        + _meta_row("Time", timestamp)
        + _meta_row("Run", run_id, mono=True)
    )

    message_block = (
        f"""
          <tr>
            <td style="padding:8px 32px 0;">
              <div class="text-secondary" style="color:#6b7280;font-size:12px;letter-spacing:.04em;text-transform:uppercase;margin-bottom:8px;">Error Message</div>
              <div class="code" style="background:#fafafa;border:1px solid #eef0f3;border-radius:8px;padding:14px 16px;color:#111827;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;line-height:1.55;white-space:pre-wrap;word-break:break-word;">{safe_message}</div>
            </td>
          </tr>"""
        if error_message
        else ""
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <meta name="color-scheme" content="light dark" />
  <meta name="supported-color-schemes" content="light dark" />
  <title>Pipeline Failure Alert</title>
  <style>
    /* Dark-mode overrides — supported by Apple Mail, iOS Mail, Outlook 2019+,
       Thunderbird, and most modern web clients via prefers-color-scheme. */
    @media (prefers-color-scheme: dark) {{
      .bg-page   {{ background:#0b0d12 !important; }}
      .card      {{ background:#15181f !important; box-shadow:0 4px 24px rgba(0,0,0,.4) !important; }}
      .accent    {{ background:#ef4444 !important; }}
      .header    {{ background:linear-gradient(135deg,#2a1217 0%,#3b1620 100%) !important; }}
      .h-title   {{ color:#ffffff !important; }}
      .h-sub     {{ color:rgba(255,255,255,.72) !important; }}
      .pill      {{ background:rgba(239,68,68,.18) !important; color:#fecaca !important; }}
      .pill-dot  {{ background:#ef4444 !important; }}
      .text-primary   {{ color:#f3f4f6 !important; }}
      .text-secondary {{ color:#9ca3af !important; }}
      .border    {{ border-color:#262932 !important; }}
      .code      {{ background:#0f1218 !important; border-color:#262932 !important; color:#e5e7eb !important; }}
      .footer    {{ background:#0f1218 !important; border-color:#262932 !important; }}
      .footer-text {{ color:#6b7280 !important; }}
    }}
    /* Outlook.com dark-mode hook (uses [data-ogsc]) */
    [data-ogsc] .bg-page   {{ background:#0b0d12 !important; }}
    [data-ogsc] .card      {{ background:#15181f !important; }}
    [data-ogsc] .accent    {{ background:#ef4444 !important; }}
    [data-ogsc] .header    {{ background:linear-gradient(135deg,#2a1217 0%,#3b1620 100%) !important; }}
    [data-ogsc] .h-title   {{ color:#ffffff !important; }}
    [data-ogsc] .h-sub     {{ color:rgba(255,255,255,.72) !important; }}
    [data-ogsc] .pill      {{ background:rgba(239,68,68,.18) !important; color:#fecaca !important; }}
    [data-ogsc] .text-primary   {{ color:#f3f4f6 !important; }}
    [data-ogsc] .text-secondary {{ color:#9ca3af !important; }}
    [data-ogsc] .border    {{ border-color:#262932 !important; }}
    [data-ogsc] .code      {{ background:#0f1218 !important; border-color:#262932 !important; color:#e5e7eb !important; }}
    [data-ogsc] .footer    {{ background:#0f1218 !important; border-color:#262932 !important; }}
    [data-ogsc] .footer-text {{ color:#6b7280 !important; }}
  </style>
</head>
<body class="bg-page" style="margin:0;padding:0;background:#f5f6f8;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
  <table role="presentation" class="bg-page" width="100%" cellpadding="0" cellspacing="0" style="background:#f5f6f8;padding:40px 16px;">
    <tr>
      <td align="center">
        <table role="presentation" class="card" width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;background:#ffffff;border-radius:14px;overflow:hidden;box-shadow:0 4px 20px rgba(15,23,42,.08);">

          <!-- 3px red accent strip — carries the alert signal across light + dark. -->
          <tr>
            <td class="accent" style="background:#dc2626;height:3px;line-height:3px;font-size:0;">&nbsp;</td>
          </tr>

          <!-- Header — soft pastel-red banner so the alert cue is present without
               shouting. In dark mode this swaps to a deep wine red. -->
          <tr>
            <td class="header" style="background:#fff1f2;background:linear-gradient(135deg,#fff1f2 0%,#ffe4e6 100%);padding:28px 32px;">
              <div class="pill" style="display:inline-block;background:#fee2e2;color:#b91c1c;font-size:11px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;padding:5px 11px 5px 9px;border-radius:999px;margin-bottom:12px;line-height:1;">
                <span class="pill-dot" style="display:inline-block;width:6px;height:6px;border-radius:999px;background:#dc2626;vertical-align:middle;margin-right:6px;margin-bottom:1px;"></span>Pipeline Failed
              </div>
              <h1 class="h-title" style="margin:0;color:#7f1d1d;font-size:22px;font-weight:700;letter-spacing:-.01em;line-height:1.3;">
                Aircan Pipeline Failed
              </h1>
              <p class="h-sub" style="margin:6px 0 0;color:#9f1239;font-size:14px;line-height:1.5;">
                A scheduled data pipeline run encountered an error and stopped.
              </p>
            </td>
          </tr>

          <!-- Metadata -->
          <tr>
            <td style="padding:24px 32px 8px;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
                {meta_rows}
              </table>
            </td>
          </tr>

          <!-- Error message -->
          {message_block}

          <!-- Spacer -->
          <tr><td style="padding:8px 0;"></td></tr>

          <!-- Footer -->
          <tr>
            <td class="footer border" style="padding:18px 32px;background:#fafbfc;border-top:1px solid #eef0f3;">
              <p class="footer-text" style="margin:0;color:#9ca3af;font-size:12px;line-height:1.5;">
                This is an automated alert from the Aircan data pipeline.
                Reply to this email is not monitored.
              </p>
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""


def send_email(
    to: str | list[str],
    subject: str,
    html_content: str,
    from_email: str,
    conn_id: str,
) -> None:
    """Send an HTML email via the SMTP connection stored in Airflow.

    All connection details (host, port, login, password) are read from
    the Airflow connection identified by ``conn_id``, so each DAG can use
    a different provider without touching global config.
    """
    conn = BaseHook.get_connection(conn_id)
    recipients = [to] if isinstance(to, str) else to

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = ", ".join(recipients)

    msg.attach(MIMEText(html_content, "html"))

    try:
        with smtplib.SMTP(conn.host, conn.port) as smtp:
            smtp.starttls()
            smtp.login(conn.login, conn.password)
            smtp.sendmail(from_email, recipients, msg.as_string())
    except Exception as e:
        # Log the error but don't fail the task, since email is a "nice to have"
        print(f"Failed to send email: {e}")
