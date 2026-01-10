import argparse
import sys
from .api import run_project

def main():
    parser = argparse.ArgumentParser(description="dbx-sql-runner: Run SQL models on Databricks")
    parser.add_argument("--models-dir", default="models", help="Directory containing SQL models")
    parser.add_argument("--profile", required=True, help="Path to YAML configuration file (profiles.yml)")
    
    args = parser.parse_args()
    
    try:
        run_project(args.models_dir, args.profile)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()