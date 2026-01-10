import os
import sys
from dbx_sql_runner.api import run_project

def main():
    # Example: Run the models in the "models" directory using "profiles.yml"
    models_dir = os.path.abspath("models")
    config_path = os.path.abspath("profiles.yml")
    
    print(f"Building project from {models_dir} using config {config_path}...")
    
    try:
        run_project(models_dir, config_path)
    except Exception as e:
        print(f"Build failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
