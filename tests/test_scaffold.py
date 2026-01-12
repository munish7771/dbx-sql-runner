import os
import shutil
import tempfile
import pytest
from dbx_sql_runner.scaffold import init_project

@pytest.fixture
def temp_dir():
    # Create a temporary directory
    temp_dir = tempfile.mkdtemp()
    original_cwd = os.getcwd()
    os.chdir(temp_dir)
    yield temp_dir
    # Cleanup
    os.chdir(original_cwd)
    shutil.rmtree(temp_dir)

def test_init_project_creates_files(temp_dir):
    project_name = "test_proj"
    init_project(project_name)
    
    base_path = os.path.join(temp_dir, project_name)
    
    assert os.path.exists(base_path)
    assert os.path.exists(os.path.join(base_path, "models"))
    assert os.path.exists(os.path.join(base_path, "profiles.yml"))
    assert os.path.exists(os.path.join(base_path, ".gitignore"))
    assert os.path.exists(os.path.join(base_path, "lint.yml"))
    assert os.path.exists(os.path.join(base_path, "README.md"))
    assert os.path.exists(os.path.join(base_path, "models", "example.sql"))

def test_init_project_current_directory(temp_dir):
    init_project(".")
    
    base_path = temp_dir
    
    assert os.path.exists(os.path.join(base_path, "models"))
    assert os.path.exists(os.path.join(base_path, "profiles.yml"))
    assert os.path.exists(os.path.join(base_path, "lint.yml"))
