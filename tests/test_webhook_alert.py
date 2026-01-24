import unittest
from unittest.mock import patch, MagicMock
from dbx_sql_runner.runner import DbxRunner
from dbx_sql_runner.project import ProjectLoader
from dbx_sql_runner.adapters.base import BaseAdapter
from dbx_sql_runner.models import Model
import json

class MockAdapter(BaseAdapter):
    def get_metadata(self, catalog, schema):
        return {}
    def get_next_execution_id(self, catalog, schema):
        return 123
    def execute(self, sql):
        pass
    def update_metadata(self, *args):
        pass
    def fetch_result(self, sql):
        return []

class TestWebhookAlert(unittest.TestCase):
    def setUp(self):
        self.loader = MagicMock(spec=ProjectLoader)
        self.loader.load_models.return_value = [
            Model(name="model1", materialized="view", sql="SELECT 1", depends_on=[], partition_by=[])
        ]
        self.adapter = MockAdapter()
        self.config = {
            "catalog": "cat",
            "schema": "sch",
            "alert_webhook_url": "http://example.com/webhook",
            "target_name": "dev",
            "silent": True
        }
        self.runner = DbxRunner(self.loader, self.adapter, self.config)

    @patch('urllib.request.urlopen')
    def test_alert_sent(self, mock_urlopen):
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.__enter__.return_value = mock_response
        mock_urlopen.return_value = mock_response

        # Run the runner (mocks make this fast/no-op for actual SQL)
        self.runner.run()

        # Check if urlopen was called
        self.assertTrue(mock_urlopen.called)
        
        # Verify the arguments
        args, kwargs = mock_urlopen.call_args
        req = args[0]
        
        self.assertEqual(req.full_url, "http://example.com/webhook")
        self.assertEqual(req.headers['Content-type'], 'application/json')
        
        payload = json.loads(req.data.decode('utf-8'))
        
        self.assertEqual(payload['environment'], 'dev')
        self.assertEqual(payload['run_stats']['passed'], 1)
        self.assertEqual(payload['total_models'], 1)
        self.assertIn('duration_seconds', payload)

    @patch('urllib.request.urlopen')
    def test_no_alert_configured(self, mock_urlopen):
        # Remove webhook url
        self.config.pop('alert_webhook_url')
        self.runner = DbxRunner(self.loader, self.adapter, self.config)
        
        self.runner.run()
        
        self.assertFalse(mock_urlopen.called)

if __name__ == '__main__':
    unittest.main()
