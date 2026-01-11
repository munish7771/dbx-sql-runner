import unittest
from unittest.mock import MagicMock
import sys
import os
import hashlib

# Add parent dir to path to import dbx_sql_runner
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dbx_sql_runner.runner import DbxRunner
from dbx_sql_runner.models import Model

# Enhanced Mock Adapter needed for Runner Logic testing
class MockAdapterLogic(MagicMock):
    def __init__(self):
        super().__init__()
        self.metadata = {}  # model_name -> {sql_hash, materialized}
        self.executed_sql = []
        self.next_id = 99

    def get_metadata(self, catalog, schema):
        return self.metadata

    def get_next_execution_id(self, catalog, schema):
        return self.next_id

    def execute(self, sql):
        self.executed_sql.append(sql)
        
    def ensure_schema_exists(self, c, s): pass
    def drop_schema_cascade(self, c, s): pass
    def update_metadata(self, c, s, name, hash, mat, eid): pass

class TestRunnerLogic(unittest.TestCase):
    def setUp(self):
        self.loader = MagicMock()
        self.adapter = MockAdapterLogic()
        self.config = {"catalog": "cat", "schema": "sch"}
        self.runner = DbxRunner(self.loader, self.adapter, self.config)

    def test_skip_view_logic(self):
        # Scenario: Model 'my_view' is a VIEW and Hash Matches -> Should be SKIPPED
        sql_content = "SELECT 1"
        model = Model("my_view", "view", sql_content, [], [])
        self.loader.load_models.return_value = [model]
        
        # Manually compute hash runner uses
        # Runner renders with target context: {m: cat.sch.my_view}
        # "SELECT 1" -> "SELECT 1" (no vars)
        expected_hash = hashlib.sha256(sql_content.encode('utf-8')).hexdigest()
        
        # Pre-populate metadata with SAME hash
        self.adapter.metadata = {"my_view": {"sql_hash": expected_hash, "materialized": "view"}}
        
        self.runner.run()
        
        # Verify NO execution (empty sql list or specific calls missing)
        # Should NOT see CREATE OR REPLACE ...
        create_calls = [s for s in self.adapter.executed_sql if "CREATE OR REPLACE" in s]
        self.assertEqual(len(create_calls), 0, "View should have been skipped")

    def test_execute_table_even_if_hash_matches(self):
        # Scenario: Model 'my_table' is a TABLE and Hash Matches -> Should be REBUILT (Not skipped)
        # Assuming current logic in runner.py: "if model.materialized == 'view' and ..."
        
        sql_content = "SELECT 1"
        model = Model("my_table", "table", sql_content, [], [])
        self.loader.load_models.return_value = [model]
        
        expected_hash = hashlib.sha256(sql_content.encode('utf-8')).hexdigest()
        self.adapter.metadata = {"my_table": {"sql_hash": expected_hash, "materialized": "table"}}
        
        self.runner.run()
        
        # Verify EXECUTION
        create_calls = [s for s in self.adapter.executed_sql if "CREATE OR REPLACE TABLE" in s]
        self.assertEqual(len(create_calls), 1, "Table should NOT be skipped")

    def test_failure_cleanup(self):
        # Scenario: Adapter raises exception during execution
        model = Model("bad_model", "view", "SELECT 1", [], [])
        self.loader.load_models.return_value = [model]
        
        # Replace execute method with a Mock so we can set side_effect
        self.adapter.execute = MagicMock()
        
        # Make adapter execute fail
        def fail_on_create(sql):
            if "CREATE" in sql:
                raise Exception("DB Error")
        self.adapter.execute.side_effect = fail_on_create
        
        # Mock drop schema to verify it's called
        self.adapter.drop_schema_cascade = MagicMock()
        
        with self.assertRaises(Exception):
            self.runner.run()
            
        # Verify Cleanup was called
        # once in except block, once in finally? Or just once ensures safety.
        # Runner code: 
        # except: drop_schema_cascade; raise
        # finally (implicit at end of function): drop_schema_cascade
        self.assertTrue(self.adapter.drop_schema_cascade.called)

if __name__ == '__main__':
    unittest.main()
