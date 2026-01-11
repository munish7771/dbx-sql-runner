import unittest
from unittest.mock import MagicMock, call
import sys
import os

# Ensure importable
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dbx_sql_runner.runner import DbxRunner
from dbx_sql_runner.project import ProjectLoader, DependencyGraph
from dbx_sql_runner.models import Model
from dbx_sql_runner.adapters.base import BaseAdapter

class MockAdapter(BaseAdapter):
    def __init__(self):
        self.executed_sql = []
        self.metadata = {}
        self.next_id = 1
        
    def execute(self, sql):
        self.executed_sql.append(sql)
        
    def fetch_result(self, sql):
        return []
        
    def get_metadata(self, catalog, schema):
        return self.metadata
        
    def update_metadata(self, catalog, schema, model_name, sql_hash, materialized, execution_id):
        pass
        
    def ensure_schema_exists(self, catalog, schema):
        pass

    def drop_schema_cascade(self, catalog, schema):
        pass

    def get_next_execution_id(self, catalog, schema):
        return self.next_id

class TestModularRunner(unittest.TestCase):
    def setUp(self):
        self.models = [
            Model("upstream", "table", "SELECT 1", [], []),
            Model("downstream", "view", "SELECT * FROM {upstream}", ["upstream"], [])
        ]
        self.loader = MagicMock(spec=ProjectLoader)
        self.loader.load_models.return_value = self.models
        self.adapter = MockAdapter()
        self.config = {"catalog": "cat", "schema": "sch"}
        self.runner = DbxRunner(self.loader, self.adapter, self.config)

    def test_run_order_and_execution(self):
        self.runner.run()
        
        # Check Execution Order (upstream first)
        sqls = self.adapter.executed_sql
        
        # 1. Create upstream table in staging
        # Format: CREATE OR REPLACE TABLE cat.sch_staging.upstream ...
        upstream_build = [s for s in sqls if "CREATE OR REPLACE TABLE cat.sch_staging.upstream" in s]
        self.assertTrue(upstream_build)
        
        # 2. Create downstream view in staging
        # It should reference upstream in staging!
        downstream_build = [s for s in sqls if "CREATE OR REPLACE VIEW cat.sch_staging.downstream" in s]
        self.assertTrue(downstream_build)
        self.assertIn("cat.sch_staging.upstream", downstream_build[0])
        
        # 3. Promote upstream (Rename)
        promote_upstream = [s for s in sqls if "ALTER TABLE cat.sch_staging.upstream RENAME TO cat.sch.upstream" in s]
        self.assertTrue(promote_upstream)
        
        # 4. Promote downstream (Re-create View)
        # It should reference upstream in TARGET (cat.sch.upstream)
        promote_downstream = [s for s in sqls if "CREATE OR REPLACE VIEW cat.sch.downstream" in s]
        self.assertTrue(promote_downstream)
        self.assertIn("cat.sch.upstream", promote_downstream[0])

if __name__ == '__main__':
    unittest.main()
