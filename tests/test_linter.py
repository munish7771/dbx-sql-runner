import os
import shutil
import tempfile
import pytest
import yaml
from dbx_sql_runner.linter import ProjectLinter

@pytest.fixture
def project_layout():
    # Setup standard project layout
    temp_dir = tempfile.mkdtemp()
    models_dir = os.path.join(temp_dir, "models")
    os.makedirs(models_dir)
    
    # helper to create model
    def create_model(name, content):
        with open(os.path.join(models_dir, f"{name}.sql"), "w") as f:
            f.write(content)
            
    # helper to create profiles
    def create_profiles(content_dict):
        with open(os.path.join(temp_dir, "profiles.yml"), "w") as f:
            yaml.dump(content_dict, f)
            
    # helper to create linter config
    def create_config(content_dict):
        with open(os.path.join(temp_dir, "lint.yml"), "w") as f:
            yaml.dump(content_dict, f)

    yield temp_dir, create_model, create_profiles, create_config
    
    shutil.rmtree(temp_dir)

def test_lint_valid_model(project_layout):
    temp_dir, create_model, _, _ = project_layout
    create_model("valid_model", "SELECT 1 as id, 'a' as name")
    
    linter = ProjectLinter(temp_dir)
    linter.lint_project()
    assert len(linter.errors) == 0

def test_lint_invalid_model_name(project_layout):
    temp_dir, create_model, _, _ = project_layout
    create_model("InvalidName", "SELECT 1 as id")
    
    linter = ProjectLinter(temp_dir)
    linter.lint_project()
    
    assert len(linter.errors) > 0
    assert any("Model names must be snake_case" in e for e in linter.errors)

def test_lint_invalid_column_name(project_layout):
    temp_dir, create_model, _, _ = project_layout
    create_model("valid_model", "SELECT 1 as CamelCol")
    
    linter = ProjectLinter(temp_dir)
    linter.lint_project()
    
    assert len(linter.errors) > 0
    assert any("Column names must be snake_case" in e for e in linter.errors)

def test_lint_invalid_source_name(project_layout):
    temp_dir, create_model, create_profiles, _ = project_layout
    create_model("valid_model", "SELECT 1")
    create_profiles({
        "target": "dev",
        "outputs": {
             "dev": {
                 "sources": {"BadSource": "tbl"}
             }
        }
    })
    
    linter = ProjectLinter(temp_dir)
    linter.lint_project()
    
    assert len(linter.errors) > 0
    assert any("Source names must be snake_case" in e for e in linter.errors)

def test_lint_custom_config(project_layout):
    temp_dir, create_model, _, create_config = project_layout
    create_model("m_valid", "SELECT 1 as id")
    
    # Enforce m_ prefix
    create_config({
        "rules": {
            "model_name": {
                "pattern": "^m_[a-z0-9_]+$"
            }
        }
    })
    
    linter = ProjectLinter(temp_dir, config_file="lint.yml")
    linter.lint_project()
    assert len(linter.errors) == 0
    
    # Now try one that fails custom rule but would pass default
    create_model("valid_but_no_prefix", "SELECT 1")
    linter = ProjectLinter(temp_dir, config_file="lint.yml")
    linter.lint_project()
    assert len(linter.errors) > 0
