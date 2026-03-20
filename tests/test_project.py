import unittest
import os
import tempfile
import shutil
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dbx_sql_runner.project import ProjectLoader, DependencyGraph
from dbx_sql_runner.models import Model
from dbx_sql_runner.exceptions import DbxModelLoadingError, DbxDependencyError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_dir_with_files(files: dict) -> str:
    """Create a temp directory containing the given {filename: content} files."""
    d = tempfile.mkdtemp()
    for name, content in files.items():
        with open(os.path.join(d, name), "w") as f:
            f.write(content)
    return d


# ---------------------------------------------------------------------------
# ProjectLoader – model_paths
# ---------------------------------------------------------------------------


class TestProjectLoaderModelPaths(unittest.TestCase):
    def setUp(self):
        self.dirs = []

    def tearDown(self):
        for d in self.dirs:
            shutil.rmtree(d, ignore_errors=True)

    def make_dir(self, files=None):
        d = make_dir_with_files(files or {})
        self.dirs.append(d)
        return d

    # --- construction errors ---

    def test_empty_model_paths_raises(self):
        """model_paths=[] should raise immediately on construction."""
        with self.assertRaises(DbxModelLoadingError):
            ProjectLoader(model_paths=[])

    def test_invalid_directory_raises(self):
        """A non-existent directory in model_paths should raise on load."""
        loader = ProjectLoader(model_paths=["/does/not/exist"])
        with self.assertRaises(DbxModelLoadingError):
            loader.load_models()

    def test_no_sql_files_raises(self):
        """A directory with no .sql files should raise."""
        d = self.make_dir({"readme.txt": "hello"})
        loader = ProjectLoader(model_paths=[d])
        with self.assertRaises(DbxModelLoadingError):
            loader.load_models()

    # --- basic loading ---

    def test_loads_sql_files_from_directory(self):
        """All .sql files in model_paths dirs are loaded."""
        d = self.make_dir(
            {
                "a.sql": "SELECT 1",
                "b.sql": "SELECT 2",
                "ignore.txt": "not sql",
            }
        )
        loader = ProjectLoader(model_paths=[d])
        models = loader.load_models()
        names = {m.name for m in models}
        self.assertEqual(names, {"a", "b"})
        self.assertNotIn("ignore", names)

    def test_multiple_directories(self):
        """Files from multiple directories in model_paths are combined."""
        d1 = self.make_dir({"model_x.sql": "SELECT 'x'"})
        d2 = self.make_dir({"model_y.sql": "SELECT 'y'"})
        loader = ProjectLoader(model_paths=[d1, d2])
        models = loader.load_models()
        names = {m.name for m in models}
        self.assertEqual(names, {"model_x", "model_y"})

    def test_deduplication_across_directories(self):
        """The same directory listed twice should not duplicate models."""
        d = self.make_dir({"dup.sql": "SELECT 1"})
        loader = ProjectLoader(model_paths=[d, d])
        models = loader.load_models()
        self.assertEqual(len(models), 1)

    # --- metadata parsing ---

    def test_parse_model_metadata(self):
        sql = (
            "-- name: my_model\n"
            "-- materialized: table\n"
            "-- partition_by: date, region\n"
            "-- depends_on: source_a, source_b\n"
            "SELECT * FROM {source_a} JOIN {source_b}\n"
        )
        d = self.make_dir({"my_model.sql": sql})
        loader = ProjectLoader(model_paths=[d])
        models = loader.load_models()
        self.assertEqual(len(models), 1)
        m = models[0]
        self.assertEqual(m.name, "my_model")
        self.assertEqual(m.materialized, "table")
        self.assertEqual(m.partition_by, ["date", "region"])
        self.assertIn("source_a", m.depends_on)
        self.assertIn("source_b", m.depends_on)

    def test_defaults_when_no_metadata(self):
        """Name defaults to filename stem, materialized defaults to 'view'."""
        d = self.make_dir({"plain.sql": "SELECT 42"})
        loader = ProjectLoader(model_paths=[d])
        m = loader.load_models()[0]
        self.assertEqual(m.name, "plain")
        self.assertEqual(m.materialized, "view")

    def test_variable_inference(self):
        """Variables in {braces} are inferred as dependencies."""
        d = self.make_dir({"infer.sql": "SELECT * FROM {inferred_table}"})
        loader = ProjectLoader(model_paths=[d])
        m = loader.load_models()[0]
        self.assertIn("inferred_table", m.depends_on)


# ---------------------------------------------------------------------------
# DependencyGraph
# ---------------------------------------------------------------------------


class TestDependencyGraph(unittest.TestCase):
    def test_simple_dag(self):
        m1 = Model("a", "view", "", [], [])
        m2 = Model("b", "view", "", ["a"], [])
        order = DependencyGraph([m1, m2]).get_execution_order()
        self.assertEqual([m.name for m in order], ["a", "b"])

    def test_cycle_detection(self):
        m1 = Model("a", "view", "", ["b"], [])
        m2 = Model("b", "view", "", ["a"], [])
        with self.assertRaises(DbxDependencyError) as cm:
            DependencyGraph([m1, m2]).get_execution_order()
        self.assertIn("Cyclic dependency", str(cm.exception))

    def test_ignore_missing_upstream(self):
        """External deps not in the project are simply not added as edges."""
        m = Model("a", "view", "", ["external_source"], [])
        order = DependencyGraph([m]).get_execution_order()
        self.assertEqual(len(order), 1)
        self.assertEqual(order[0].name, "a")


if __name__ == "__main__":
    unittest.main()
