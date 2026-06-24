import unittest
from unittest.mock import patch
from dbx_sql_runner.alerting import SlackAlert
import json


class TestSlackAlert(unittest.TestCase):
    @patch("urllib.request.urlopen")
    def test_send_alert(self, mock_urlopen):
        webhook_url = "http://example.com/webhook"
        alert = SlackAlert(webhook_url)

        results = {"PASS": 5, "ERROR": 1, "SKIP": 2}
        total_models = 8
        duration = 10.5
        environment = "prod"
        status_message = "Custom Run Message"
        failed_models = ["failed_model"]
        passed_models = ["model1", "model2", "model3", "model4", "model5"]

        alert.send(
            environment,
            results,
            total_models,
            duration,
            status_message,
            failed_models,
            passed_models,
        )

        self.assertTrue(mock_urlopen.called)
        args, kwargs = mock_urlopen.call_args
        req = args[0]

        payload = json.loads(req.data.decode("utf-8"))

        self.assertEqual(
            payload["text"],
            "Custom Run Message (prod): 5 Passed, 2 Skipped, 1 Failed.\nFailed models: failed_model\nPassed models: model1, model2, model3, model4, model5",
        )
        self.assertNotIn("blocks", payload)

    @patch("urllib.request.urlopen")
    def test_no_webhook_url(self, mock_urlopen):
        alert = SlackAlert(None)
        alert.send("env", {}, 0, 0)
        self.assertFalse(mock_urlopen.called)


if __name__ == "__main__":
    unittest.main()
