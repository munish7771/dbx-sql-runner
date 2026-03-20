import logging
import argparse
import sys
from .api import run_project


def main():
    # Configure logging to match dbt-style output
    # Format: HH:MM:SS  Message
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S"
    )
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(
        description="dbx-sql-runner: Run SQL models on Databricks"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Run command
    run_parser = subparsers.add_parser(
        "run", help="Run the models against the database"
    )
    run_parser.add_argument(
        "--profile",
        required=False,
        default="profiles.yml",
        help="Path to YAML configuration file (default: profiles.yml)",
    )

    # Build command (Preview)
    build_parser = subparsers.add_parser(
        "build", help="Preview the models that will be built"
    )
    build_parser.add_argument(
        "--profile",
        required=False,
        default="profiles.yml",
        help="Path to YAML configuration file (default: profiles.yml)",
    )

    # Init command
    init_parser = subparsers.add_parser("init", help="Initialize a new project")
    init_parser.add_argument(
        "project_name",
        nargs="?",
        default=".",
        help="Name of the project directory (default: current directory)",
    )

    # Lint command
    lint_parser = subparsers.add_parser(
        "lint", help="Lint the project for naming conventions"
    )
    lint_parser.add_argument(
        "--config", default="lint.yml", help="Path to linter config file"
    )

    args = parser.parse_args()

    try:
        if args.command == "run":
            run_project(args.profile)
        elif args.command == "build":
            run_project(args.profile, preview=True)
        elif args.command == "init":
            from .scaffold import init_project

            init_project(args.project_name)
        elif args.command == "lint":
            from .linter import ProjectLinter

            linter = ProjectLinter(config_file=args.config)
            success = linter.lint_project()
            if not success:
                sys.exit(1)
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
