import unittest
import os
import tempfile
import shutil
import sys

# Add parent dir to path to import dbx_sql_runner
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dbx_sql_runner.project import ProjectLoader, DependencyGraph
from dbx_sql_runner.models import Model

class TestProjectLoader(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        
    def tearDown(self):
        shutil.rmtree(self.test_dir)
        
    def create_file(self, filename, content):
        with open(os.path.join(self.test_dir, filename), 'w') as f:
            f.write(content)

    def test_load_nonexistent_dir(self):
        loader = ProjectLoader("non_existent_path")
        with self.assertRaises(ValueError):
            loader.load_models()

    def test_parse_model_metadata(self):
        content = """-- name: my_model
-- materialized: table
-- partition_by: date, region
-- depends_on: source_a, source_b
SELECT * FROM {source_a} JOIN {source_b}
"""
        self.create_file("my_model.sql", content)
        loader = ProjectLoader(self.test_dir)
        models = loader.load_models()
        self.assertEqual(len(models), 1)
        m = models[0]
        self.assertEqual(m.name, "my_model")
        self.assertEqual(m.materialized, "table")
        self.assertEqual(m.partition_by, ["date", "region"])
        self.assertIn("source_a", m.depends_on)
        self.assertIn("source_b", m.depends_on)

    def test_variable_inference(self):
        content = "SELECT * FROM {inferred_table}"
        self.create_file("auto.sql", content)
        loader = ProjectLoader(self.test_dir)
        models = loader.load_models()
        m = models[0]
        self.assertEqual(m.name, "auto") # Default to filename
        self.assertEqual(m.materialized, "view") # Default
        self.assertIn("inferred_table", m.depends_on)

class TestDependencyGraph(unittest.TestCase):
    def test_simple_dag(self):
        m1 = Model("a", "view", "", [], [])
        m2 = Model("b", "view", "", ["a"], [])
        
        graph = DependencyGraph([m1, m2])
        order = graph.get_execution_order()
        names = [m.name for m in order]
        self.assertEqual(names, ["a", "b"])

    def test_cycle_detection(self):
        m1 = Model("a", "view", "", ["b"], [])
        m2 = Model("b", "view", "", ["a"], [])
        
        graph = DependencyGraph([m1, m2])
        with self.assertRaises(ValueError) as cm:
            graph.get_execution_order()
        self.assertIn("Cyclic dependency", str(cm.exception))

    def test_ignore_missing_upstream(self):
        # If A depends on External which is not in project, External should be ignored in internal DAG
        m1 = Model("a", "view", "", ["external_source"], [])
        
        graph = DependencyGraph([m1])
        order = graph.get_execution_order()
        self.assertEqual(len(order), 1)
        self.assertEqual(order[0].name, "a")

if __name__ == '__main__':
    unittest.main()
