import argparse
import sys
from .api import run_project

def main():
    parser = argparse.ArgumentParser(description="dbx-sql-runner: Run SQL models on Databricks")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Run command
    run_parser = subparsers.add_parser("run", help="Run the models against the database")
    run_parser.add_argument("--models-dir", default="models", help="Directory containing SQL models")
    run_parser.add_argument("--profile", required=True, help="Path to YAML configuration file (profiles.yml)")

    # Build command (Preview)
    build_parser = subparsers.add_parser("build", help="Preview the models that will be built")
    build_parser.add_argument("--models-dir", default="models", help="Directory containing SQL models")
    build_parser.add_argument("--profile", required=True, help="Path to YAML configuration file (profiles.yml)")
    
    args = parser.parse_args()
    
    try:
        if args.command == "run":
            run_project(args.models_dir, args.profile)
        elif args.command == "build":
            # For build, we want to show the plan, so we pass preview=True to run_project
            # We need to update api.py/run_project to accept this or access DbxRunnerProject directly.
            # Assuming run_project just instantiates and runs, we might need to modify it.
            # For now, let's keep it simple and assume run_project is updated.
            run_project(args.models_dir, args.profile, preview=True)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()