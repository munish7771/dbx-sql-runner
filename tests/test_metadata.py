import sys
import os
import unittest
from unittest.mock import MagicMock, patch

# Add parent dir to path to import dbx_sql_runner
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dbx_sql_runner.core import DbxRunnerProject, Model

class TestMetadataUpdate(unittest.TestCase):
    def setUp(self):
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value.__enter__.return_value = self.mock_cursor
        
        # Patch init to avoid loading models from disk
        with patch.object(DbxRunnerProject, '_load_models', return_value=[]), \
             patch.object(DbxRunnerProject, '_build_dag', return_value=None):
            self.runner = DbxRunnerProject('dummy_dir', {'catalog': 'cat', 'schema': 'sch', 'server_hostname': 'host', 'http_path': 'path', 'access_token': 'token'})
            
    def test_ensure_metadata_table(self):
        self.runner._ensure_metadata_table(self.mock_conn, 'cat', 'sch')
        
        # Verify CREATE TABLE has execution_id BIGINT
        create_call = [call for call in self.mock_cursor.execute.call_args_list if "CREATE TABLE" in call[0][0]]
        self.assertTrue(create_call, "CREATE TABLE not called")
        self.assertIn("execution_id BIGINT", create_call[0][0][0], "execution_id BIGINT missing in CREATE TABLE")
        
        # Verify ALTER TABLE is attempted with BIGINT
        alter_call = [call for call in self.mock_cursor.execute.call_args_list if "ALTER TABLE" in call[0][0]]
        self.assertTrue(alter_call, "ALTER TABLE not called")
        self.assertIn("ADD COLUMNS (execution_id BIGINT)", alter_call[0][0][0], "Incorrect ALTER TABLE statement")

    def test_update_metadata(self):
        execution_id = 123
        self.runner._update_metadata(self.mock_conn, 'cat', 'sch', 'my_model', 'hash123', 'view', execution_id)
        
        # Verify INSERT includes integer execution_id
        insert_call = [call for call in self.mock_cursor.execute.call_args_list if "INSERT INTO" in call[0][0]]
        self.assertTrue(insert_call, "INSERT INTO not called")
        sql = insert_call[0][0][0]
        self.assertIn(f", {execution_id})", sql, "execution_id value missing or quoted in INSERT")

    def test_get_metadata(self):
        # Mock fetchall to return integer execution_id
        self.mock_cursor.fetchall.return_value = [
            ('my_model', 'hash123', 'view', 5),
            ('my_model', 'hash456', 'view', 6)
        ]
        
        meta = self.runner._get_metadata(self.mock_conn, 'cat', 'sch')
        
        # Verify SELECT includes execution_id and ORDER BY
        select_call = [call for call in self.mock_cursor.execute.call_args_list if "SELECT" in call[0][0]]
        self.assertTrue(select_call, "SELECT not called")
        self.assertIn("execution_id", select_call[0][0][0])
        self.assertIn("ORDER BY last_executed_at ASC", select_call[0][0][0])
        
        # Verify we got the latest
        self.assertEqual(meta['my_model']['sql_hash'], 'hash456')
        self.assertEqual(meta['my_model']['execution_id'], 6)

    def test_get_next_execution_id(self):
        # Case 1: Empty table
        self.mock_cursor.fetchone.return_value = None
        self.assertEqual(self.runner._get_next_execution_id(self.mock_conn, 'cat', 'sch'), 1)
        
        # Case 2: Max ID is 10
        self.mock_cursor.fetchone.return_value = (10,)
        self.assertEqual(self.runner._get_next_execution_id(self.mock_conn, 'cat', 'sch'), 11)

    def test_run_generates_id(self):
        # Mock everything needed for run
        self.runner.models = [Model('m1', 'view', 'SELECT 1', [], [])]
        import networkx as nx
        self.runner.dag = nx.DiGraph()
        self.runner.dag.add_node('m1')
        
        # Mock methods
        self.runner._ensure_metadata_table = MagicMock()
        self.runner._get_metadata = MagicMock(return_value={})
        self.runner._execute_model = MagicMock()
        self.runner._update_metadata = MagicMock()
        self.runner._safe_drop = MagicMock()
        self.runner._render_sql = MagicMock(return_value="SELECT 1")
        self.runner._validate_model = MagicMock()
        
        # Mock _get_next_execution_id to return specific int
        self.runner._get_next_execution_id = MagicMock(return_value=101)
        
        # Patch databricks.sql.connect to return our mock conn
        with patch('databricks.sql.connect', return_value=self.mock_conn):
            self.runner.run()
            
        # Check execution_id attribute
        self.assertEqual(self.runner.execution_id, 101)
        
        # Verify it was passed to update_metadata
        self.runner._update_metadata.assert_called()
        args = self.runner._update_metadata.call_args[0]
        # args: conn, cat, sch, name, hash, mat, exec_id
        self.assertEqual(args[6], 101)

if __name__ == '__main__':
    unittest.main()
