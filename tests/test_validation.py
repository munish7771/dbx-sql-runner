import unittest
from unittest.mock import MagicMock, patch
import os
import shutil
import tempfile
from dbx_sql_runner.core import DbxRunnerProject, Model

class TestSchemaValidation(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.models_dir = os.path.join(self.test_dir, "models")
        os.makedirs(self.models_dir)
        
        # Create a dummy model
        with open(os.path.join(self.models_dir, "test_model.sql"), "w") as f:
            f.write("SELECT 1")


    def tearDown(self):
        shutil.rmtree(self.test_dir)

    @patch("dbx_sql_runner.core.sql.connect")
    def test_validation_called(self, mock_connect):
        # Setup mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Config
        config = {
            "server_hostname": "test_host",
            "http_path": "test_path",
            "access_token": "test_token",
            "catalog": "test_cat",
            "schema": "test_sch"
        }
        
        # Initialize project
        project = DbxRunnerProject(self.models_dir, config)
        
        # Inject dependencies manually to avoid DAG issues for this simple test
        # effectively mocking what _load_models and _build_dag would do if we had full setup
        # But actually, DbxRunnerProject loads models in __init__.
        # We need to make sure the model loaded has the right properties.
        # The model in file "test_model.sql" will be loaded.
        # It references {upstream}. We need to make sure validation runs for it.
        
        # Let's mock the DAG or just direct execution?
        # The run() method does topological sort.
        # If we have a missing dependency, run() might fail before execution.
        # Let's change the model to be simple: "SELECT 1"
        with open(os.path.join(self.models_dir, "simple_model.sql"), "w") as f:
            f.write("SELECT 1")
            f.write("-- materialized: view\nSELECT 1")
            
        with open(os.path.join(self.models_dir, "test_model.sql"), "w") as f:
             f.write("SELECT 1")

        # Re-init to pick up new models
        project = DbxRunnerProject(self.models_dir, config)
        
        # Run
        project.run()
        
        # We expect calls:
        # 1. CREATE SCHEMA _dbx_model_metadata (ensure exists)
        # 2. SELECT metadata
        # 3. DROP/CREATE staging schema
        # ... logic ...
        
        cursor_calls = [c[0][0] for c in mock_cursor.execute.call_args_list]
        
        # Check Metadata Table Creation
        self.assertTrue(any("_dbx_model_metadata" in c for c in cursor_calls))
        
        # Check Staging Creation
        self.assertTrue(any("CREATE SCHEMA IF NOT EXISTS test_cat.test_sch_staging" in c for c in cursor_calls))
        
        # Check Execution in Staging (simple_model is VIEW)
        # It should be created in STAGING first
        staging_view = [c for c in cursor_calls if "CREATE OR REPLACE VIEW test_cat.test_sch_staging.simple_model" in c]
        self.assertTrue(len(staging_view) > 0, "Should build view in staging")
        
        # Check Promotion (simple_model)
        # View promotion is RECREATE in Target
        target_view = [c for c in cursor_calls if "CREATE OR REPLACE VIEW test_cat.test_sch.simple_model" in c]
        self.assertTrue(len(target_view) > 0, "Should promote view to target")

    @patch("dbx_sql_runner.core.sql.connect")
    def test_partition_by(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []
        
        config = {"server_hostname": "h", "http_path": "p", "access_token": "t", "catalog": "c", "schema": "s", "materialized": "table"}
        
        # Model with partition_by
        model_content = "-- name: part_model\n-- partition_by: col1, col2\nSELECT * FROM source"
        with open(os.path.join(self.models_dir, "part_model.sql"), "w") as f:
            f.write(model_content)
            
        project = DbxRunnerProject(self.models_dir, config)
        project.run()
        
        cursor_calls = [c[0][0] for c in mock_cursor.execute.call_args_list]
        
        # Check Execution in Staging 
        # Should be created in STAGING with PARTITION BY
        staging_table = [c for c in cursor_calls if "CREATE OR REPLACE TABLE c.s_staging.part_model" in c]
        self.assertTrue(len(staging_table) > 0)
        self.assertIn("PARTITIONED BY (col1, col2)", staging_table[0])
        
        # Check Promotion (Rename)
        rename_call = [c for c in cursor_calls if "ALTER TABLE c.s_staging.part_model RENAME TO c.s.part_model" in c]
        self.assertTrue(len(rename_call) > 0, "Should rename staging table to target")

    @patch("dbx_sql_runner.core.sql.connect")
    def test_materialization_config(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []
        
        # Case 3: Override in model (table)
        config = {"server_hostname": "h", "http_path": "p", "access_token": "t", "catalog": "c", "schema": "s"}
        with open(os.path.join(self.models_dir, "override_model.sql"), "w") as f:
            f.write("-- materialized: table\nSELECT 1")
            
        project = DbxRunnerProject(self.models_dir, config)
        project.run()
        
        cursor_calls = [c[0][0] for c in mock_cursor.execute.call_args_list]
        
        # Check Staging Build
        staging_calls = [c for c in cursor_calls if "CREATE OR REPLACE TABLE c.s_staging.override_model" in c]
        self.assertTrue(len(staging_calls) > 0)
        
        # Check Promotion
        promote_calls = [c for c in cursor_calls if "RENAME TO c.s.override_model" in c]
        self.assertTrue(len(promote_calls) > 0)

    @patch("dbx_sql_runner.core.sql.connect")
    def test_validation_failure(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Make Execute raise an error when EXPLAIN is called
        def side_effect(query):
            if "EXPLAIN" in query:
                raise Exception("Column not found")
            return None
            
        mock_cursor.execute.side_effect = side_effect

        config = {
            "server_hostname": "test_host",
            "http_path": "test_path",
            "access_token": "test_token",
            "catalog": "test_cat",
            "schema": "test_sch"
        }
        
        with open(os.path.join(self.models_dir, "bad_model.sql"), "w") as f:
            f.write("SELECT * FROM non_existent")
            
        project = DbxRunnerProject(self.models_dir, config)
        
        with self.assertRaises(ValueError) as cm:
            project.run()
            
        self.assertIn("Schema validation failed", str(cm.exception))

if __name__ == "__main__":
    unittest.main()
