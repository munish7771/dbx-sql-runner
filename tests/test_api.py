import unittest
import os
import tempfile
import shutil
import yaml


from unittest.mock import patch
from dbx_sql_runner.api import load_config_from_yaml, run_project


class TestAPI(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def create_yaml(self, filename, content):
        path = os.path.join(self.test_dir, filename)
        with open(path, "w") as f:
            yaml.dump(content, f)
        return path

    def test_load_config_flat(self):
        # Old style config (compat)
        data = {"catalog": "c", "schema": "s"}
        path = self.create_yaml("flat.yml", data)
        config = load_config_from_yaml(path)
        self.assertEqual(config["catalog"], "c")

    def test_load_config_profiles(self):
        # New style profiles.yml
        data = {
            "target": "dev",
            "outputs": {
                "dev": {"catalog": "dev_c", "schema": "dev_s"},
                "prod": {"catalog": "prod_c", "schema": "prod_s"},
            },
        }
        path = self.create_yaml("profiles.yml", data)
        config = load_config_from_yaml(path)
        self.assertEqual(config["catalog"], "dev_c")

    @patch.dict(os.environ, {"TEST_VAR": "my_secret_token"})
    def test_load_config_with_env_vars(self):
        # Test ${VAR} substitution
        data = {"token": "${TEST_VAR}"}
        path = self.create_yaml("env.yml", data)
        config = load_config_from_yaml(path)
        self.assertEqual(config["token"], "my_secret_token")

    def test_load_config_missing_target(self):
        data = {"target": "prod", "outputs": {"dev": {}}}
        path = self.create_yaml("bad.yml", data)
        with self.assertRaises(ValueError):
            load_config_from_yaml(path)

    @patch("dbx_sql_runner.api.DbxRunner")
    @patch("dbx_sql_runner.api.ProjectLoader")
    @patch("dbx_sql_runner.api.DatabricksAdapter")
    def test_run_project_integration(self, MockAdapter, MockLoader, MockRunner):
        """run_project reads model_paths from config and wires all components together."""
        data = {"catalog": "c", "model_paths": ["models/"]}
        path = self.create_yaml("config.yml", data)

        run_project(path, preview=True)

        # ProjectLoader should be called with model_paths from config
        MockLoader.assert_called_with(model_paths=["models/"])
        MockAdapter.assert_called()
        MockRunner.assert_called()
        MockRunner.return_value.run.assert_called_with(preview=True)

    def test_run_project_missing_model_paths_raises(self):
        """run_project raises ValueError when model_paths is absent from the config."""
        data = {"catalog": "c", "schema": "s"}  # no model_paths
        path = self.create_yaml("no_paths.yml", data)
        with self.assertRaises(
            ValueError, msg="Expected ValueError for missing model_paths"
        ):
            run_project(path)


if __name__ == "__main__":
    unittest.main()
