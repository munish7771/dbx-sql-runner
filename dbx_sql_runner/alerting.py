import json
import logging
import urllib.request
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


class SlackAlert:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def send(
        self,
        environment: str,
        results: Dict[str, int],
        total_models: int,
        duration: float,
        status_message: str = "SQL Runner run finished",
        failed_models: List[str] = None,
        passed_models: List[str] = None,
    ):
        if not self.webhook_url:
            logger.warning("No webhook URL provided for Slack alert.")
            return

        passed = results.get("PASS", 0)
        failed = results.get("ERROR", 0)
        skipped = results.get("SKIP", 0)
        # Construct Slack Payload (Simple Text)
        message = f"{status_message} ({environment}): {passed} Passed, {skipped} Skipped, {failed} Failed."

        if failed_models:
            message += f"\nFailed models: {', '.join(failed_models)}"

        if passed_models:
            message += f"\nPassed models: {', '.join(passed_models)}"

        payload = {"text": message}

        self._send_payload(payload)

    def send_error(self, environment: str, error_message: str):
        if not self.webhook_url:
            return

        message = f"SQL Runner runtime error ({environment}): {error_message}"

        payload = {"text": message}

        self._send_payload(payload)

    def _send_payload(self, payload: Dict[str, Any]):
        try:
            req = urllib.request.Request(
                self.webhook_url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req) as response:
                if response.status >= 400:
                    logger.warning(
                        f"Failed to send webhook alert. Status: {response.status}"
                    )
                else:
                    logger.info("Webhook alert sent successfully.")
        except Exception as e:
            logger.warning(f"Failed to send webhook alert: {e}")
