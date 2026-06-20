"""
Notification helpers for the Warren Buffett Screener pipeline.

Sends alerts to the Slack webhook configured in SLACK_WEBHOOK_URL.
If the webhook is not configured, the alert is only logged and not posted.
"""
import os
import logging
import requests as _requests

logger = logging.getLogger(__name__)

_PLACEHOLDER = "https://hooks.slack.com/..."


def send_slack_alert(title: str, details: dict) -> None:
    """Post a structured alert to Slack.

    Args:
        title:   Bold heading shown at the top of the message.
        details: Key-value pairs shown as bullet points below the title.
    """
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not webhook_url or webhook_url == _PLACEHOLDER:
        logger.warning(
            "SLACK_WEBHOOK_URL not configured — Slack alert skipped: %s", title
        )
        return

    lines = [f"*{title}*", ""]
    for key, value in details.items():
        lines.append(f"• *{key}*: {value}")

    try:
        resp = _requests.post(
            webhook_url,
            json={"text": "\n".join(lines)},
            timeout=10,
        )
        resp.raise_for_status()
        logger.info("Slack alert sent: %s", title)
    except Exception as exc:
        logger.error("Failed to send Slack alert '%s': %s", title, exc)
