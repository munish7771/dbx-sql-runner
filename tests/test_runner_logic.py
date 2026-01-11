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
        # Make adapter execute fail
        def fail_on_create(sql):
            if "CREATE" in sql:
                raise Exception("DB Error")
            # Side effect needs to handle other calls like drop? No, just fail on Create.
        self.adapter.execute.side_effect = fail_on_create
        
        with self.assertRaises(Exception):
            self.runner.run()
            
        # Verify Cleanup was called
        # Should drop bad_model__staging
        # The runner calls _safe_drop_target which calls execute("DROP TABLE IF EXISTS ...")
        
        calls = [c[0][0] for c in self.adapter.execute.call_args_list]
        drop_calls = [s for s in calls if "DROP" in s and "bad_model__staging" in s]
        self.assertTrue(drop_calls, f"Cleanup didn't attempt to drop staging table. Calls: {calls}")

    def test_execute_ddl_with_this_variable(self):
        # Scenario: DDL model using {this} variable
        sql_content = "CREATE TABLE {this} (id int)"
        model = Model("ddl_model", "ddl", sql_content, [], [])
        self.loader.load_models.return_value = [model]
        
        # Pre-populate hash mismatch to ensure EXECUTE
        self.adapter.metadata = {}
        
        self.runner.run()
        
        # Verify SQL executed contains the staging FQN
        expected_fqn = "cat.sch.ddl_model__staging"
        expected_sql = f"CREATE TABLE {expected_fqn} (id int)"
        
        executed_sqls = self.adapter.executed_sql
        self.assertIn(expected_sql, executed_sqls, f"Did not find expected DDL execution. Found: {executed_sqls}")

    def test_source_injection(self):
        # Scenario: Model uses {external_source} which is defined in config['sources']
        sql_content = "SELECT * FROM {external_source}"
        model = Model("source_model", "view", sql_content, [], [])
        self.loader.load_models.return_value = [model]
        
        # Configure sources in runner
        self.runner.sources = {"external_source": "prod_catalog.schema.table"}
        self.runner.config["sources"] = self.runner.sources
        
        self.runner.run()
        
        # Verify rendered SQL contains the source FQN. 
        # Since it's a VIEW, DDL is "CREATE VIEW ... AS SELECT * FROM prod_catalog.schema.table"
        create_view_calls = [s for s in self.adapter.executed_sql if "CREATE OR REPLACE VIEW" in s]
        self.assertTrue(create_view_calls)
        self.assertIn("SELECT * FROM prod_catalog.schema.table", create_view_calls[0])

if __name__ == '__main__':
    unittest.main()
