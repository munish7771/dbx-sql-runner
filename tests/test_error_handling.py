import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Add parent dir to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dbx_sql_runner.adapters.databricks import DatabricksAdapter
from dbx_sql_runner.exceptions import DbxConfigurationError, DbxAuthenticationError, DbxExecutionError
from databricks.sql.exc import RequestError, Error

class TestErrorHandling(unittest.TestCase):
    def setUp(self):
        self.config = {
            "server_hostname": "host",
            "http_path": "path",
            "access_token": "token"
        }

    def test_missing_config_raises_config_error(self):
        bad_config = {"server_hostname": "host"} # Missing others
        with self.assertRaises(DbxConfigurationError) as cm:
            DatabricksAdapter(bad_config)
        self.assertIn("missing required key", str(cm.exception))

    @patch("dbx_sql_runner.adapters.databricks.sql.connect")
    def test_auth_error_wrapping(self, mock_connect):
        # transform RequestError -> DbxAuthenticationError
        mock_connect.side_effect = RequestError("Auth Failed")
        
        adapter = DatabricksAdapter(self.config)
        
        with self.assertRaises(DbxAuthenticationError) as cm:
            adapter.execute("SELECT 1")
            
        self.assertIn("Failed to authenticate", str(cm.exception))
        # Ensure specific advice is present
        self.assertIn("check your 'access_token'", str(cm.exception))

    @patch("dbx_sql_runner.adapters.databricks.sql.connect")
    def test_execution_error_wrapping(self, mock_connect):
        # transform generic Error -> DbxExecutionError
        mock_connect.side_effect = Error("Syntax Error")
        
        adapter = DatabricksAdapter(self.config)
        
        with self.assertRaises(DbxExecutionError) as cm:
            adapter.execute("SELECT 1")
            
        self.assertIn("Databricks SQL Error", str(cm.exception))

if __name__ == '__main__':
    unittest.main()
