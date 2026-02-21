import unittest
from unittest.mock import patch, MagicMock
from dbx_sql_runner.runner import DbxRunner

class TestRuntimeErrorAlert(unittest.TestCase):
    def setUp(self):
        self.loader = MagicMock()
        # Mock load_models to raise an exception
        self.loader.load_models.side_effect = Exception("Auth Error")
        
        self.adapter = MagicMock()
        self.config = {
            "catalog": "cat",
            "schema": "sch",
            "alert_webhook_url": "http://example.com/webhook",
            "target_name": "prod"
        }
        self.runner = DbxRunner(self.loader, self.adapter, self.config)

    @patch('urllib.request.urlopen')
    def test_runtime_error_alert_sent(self, mock_urlopen):
        # Setup mock response success
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.__enter__.return_value = mock_response
        mock_urlopen.return_value = mock_response

        # Execute run, expect exception
        with self.assertRaises(Exception) as cm:
            self.runner.run()
        
        self.assertEqual(str(cm.exception), "Auth Error")

        # Verify alert sent
        self.assertTrue(mock_urlopen.called)
        args, kwargs = mock_urlopen.call_args
        req = args[0]
        
        import json
        payload = json.loads(req.data.decode('utf-8'))
        
        self.assertIn("SQL Runner runtime error (prod): Auth Error", payload['text'])

if __name__ == '__main__':
    unittest.main()
