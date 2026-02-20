"""Email sending utilities."""

import json
import smtplib
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from airflow.sdk import BaseHook


def build_alert_html(resource_id: str, error_payload: dict) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    error_message = error_payload.get("message", "")
    error_json = json.dumps(error_payload, indent=2)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Pipeline Failure Alert</title>
</head>
<body style="margin:0;padding:0;background:#f4f4f5;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f5;padding:32px 0;">
    <tr>
      <td align="center">
        <table width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.08);">

          <!-- Header -->
          <tr>
            <td style="background:#dc2626;padding:24px 32px;">
              <h1 style="margin:0;color:#ffffff;font-size:20px;font-weight:700;">
                &#9888;&#65039; Aircan Pipeline Failed
              </h1>
            </td>
          </tr>

          <!-- Summary -->
          <tr>
            <td style="padding:24px 32px 0;">
              <table width="100%" cellpadding="0" cellspacing="0">
                <tr>
                  <td style="padding:8px 0;border-bottom:1px solid #e5e7eb;">
                    <span style="color:#6b7280;font-size:13px;display:block;">Resource ID</span>
                    <span style="color:#111827;font-size:15px;font-weight:600;">{resource_id}</span>
                  </td>
                </tr>
                <tr>
                  <td style="padding:8px 0;border-bottom:1px solid #e5e7eb;">
                    <span style="color:#6b7280;font-size:13px;display:block;">Time</span>
                    <span style="color:#111827;font-size:15px;">{timestamp}</span>
                  </td>
                </tr>
                {"" if not error_message else f"""
                <tr>
                  <td style="padding:8px 0;border-bottom:1px solid #e5e7eb;">
                    <span style="color:#6b7280;font-size:13px;display:block;">Message</span>
                    <span style="color:#111827;font-size:15px;">{error_message}</span>
                  </td>
                </tr>"""}
              </table>
            </td>
          </tr>
        
          <!-- Footer -->
          <tr>
            <td style="padding:16px 32px;background:#f9fafb;border-top:1px solid #e5e7eb;">
              <p style="margin:0;color:#9ca3af;font-size:12px;">
                This is an automated alert from the Aircan data pipeline.
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
