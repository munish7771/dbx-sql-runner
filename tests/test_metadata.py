import sys
import os
import unittest
from unittest.mock import MagicMock, patch

# Add parent dir to path to import dbx_sql_runner
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dbx_sql_runner.adapters.databricks import DatabricksAdapter

class TestMetadataUpdate(unittest.TestCase):
    def setUp(self):
        self.config = {
            'server_hostname': 'host',
            'http_path': 'path',
            'access_token': 'token'
        }
        
        # Patch where it is USED
        self.connect_patcher = patch('dbx_sql_runner.adapters.databricks.sql.connect')
        self.mock_connect = self.connect_patcher.start()
        
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_connect.return_value.__enter__.return_value = self.mock_conn
        self.mock_conn.cursor.return_value.__enter__.return_value = self.mock_cursor
        
        self.adapter = DatabricksAdapter(self.config)

    def tearDown(self):
        self.connect_patcher.stop()
            
    def test_ensure_metadata_table(self):
        self.adapter._ensure_metadata_table('cat', 'sch')
        
        # Verify CREATE TABLE has execution_id BIGINT
        calls = [c[0][0] for c in self.mock_cursor.execute.call_args_list]
        create_call = [c for c in calls if "CREATE TABLE" in c]
        if not create_call:
            print(f"DEBUG CALLS: {calls}")
        self.assertTrue(create_call, "CREATE TABLE not called")
        self.assertIn("execution_id BIGINT", create_call[0], "execution_id BIGINT missing in CREATE TABLE")
        
        # Verify ALTER TABLE is attempted with BIGINT
        alter_call = [c for c in calls if "ALTER TABLE" in c]
        self.assertTrue(alter_call, "ALTER TABLE not called")
        self.assertIn("ADD COLUMNS (execution_id BIGINT)", alter_call[0], "Incorrect ALTER TABLE statement")

    def test_update_metadata(self):
        execution_id = 123
        self.adapter.update_metadata('cat', 'sch', 'my_model', 'hash123', 'view', execution_id)
        
        # Verify INSERT includes integer execution_id
        calls = [c[0][0] for c in self.mock_cursor.execute.call_args_list]
        insert_call = [c for c in calls if "INSERT INTO" in c]
        self.assertTrue(insert_call, "INSERT INTO not called")
        sql = insert_call[0]
        self.assertIn(f", {execution_id})", sql, "execution_id value missing or quoted in INSERT")

    def test_get_metadata_read(self):
        # Simply skip verifying the return structure detail for now, verify Call
        self.mock_cursor.fetchall.return_value = []
        self.adapter.get_metadata('cat', 'sch')
        
        calls = [c[0][0] for c in self.mock_cursor.execute.call_args_list]
        select_call = [c for c in calls if "SELECT" in c]
        self.assertTrue(select_call)
        self.assertIn("execution_id", select_call[0])
        self.assertIn("ORDER BY last_executed_at ASC", select_call[0])

    def test_get_next_execution_id(self):
        # Case 1: Empty table
        # fetchall for MAX returns [(None,)]
        self.mock_cursor.fetchall.return_value = [(None,)]
        self.assertEqual(self.adapter.get_next_execution_id('cat', 'sch'), 1)
        
        # Case 2: Max ID is 10
        self.mock_cursor.fetchall.return_value = [(10,)]
        self.assertEqual(self.adapter.get_next_execution_id('cat', 'sch'), 11)

if __name__ == '__main__':
    unittest.main()
