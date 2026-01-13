import os
import re
import logging
import yaml
import sqlglot
from sqlglot import exp
from typing import Dict, Any
from .project import ProjectLoader

from .api import load_config_from_yaml

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = {
    "model_name": {
        "pattern": "^[a-z0-9_]+$",
        "message": "Model names must be snake_case (lowercase, numbers, underscores)"
    },
    "source_name": {
        "pattern": "^[a-z0-9_]+$",
        "message": "Source names must be snake_case (lowercase, numbers, underscores)"
    },
    "column_name": {
        "pattern": "^[a-z0-9_]+$",
        "message": "Column names must be snake_case (lowercase, numbers, underscores)"
    }
}

class ProjectLinter:
    def __init__(self, project_dir: str = ".", config_file: str = "lint.yml"):
        self.project_dir = os.path.abspath(project_dir)
        self.config_file = os.path.join(self.project_dir, config_file)
        self.config = self._load_config()
        self.loader = ProjectLoader(os.path.join(self.project_dir, "models"))
        self.errors = []

    def _load_config(self) -> Dict[str, Any]:
        config = DEFAULT_CONFIG.copy()
        if os.path.exists(self.config_file):
            logger.info(f"Loading linter config from {self.config_file}")
            try:
                with open(self.config_file, 'r') as f:
                    user_config = yaml.safe_load(f) or {}
                
                # Merge user config into default config
                if "rules" in user_config:
                    for rule_name, rule_def in user_config["rules"].items():
                        if rule_name in config:
                            config[rule_name].update(rule_def)
            except Exception as e:
                logger.warning(f"Warning: Failed to load config file: {e}")
        return config

    def lint_project(self) -> bool:
        logger.info(f"Linting project in {self.project_dir}...\n")
        
        self.check_models()
        self.check_sources()

        if self.errors:
            logger.info("\nFound the following issues:")
            for err in self.errors:
                logger.error(f" - {err}")
            logger.info(f"\nTotal errors: {len(self.errors)}")
            return False
        else:
            logger.info("All checks passed!")
            return True

    def _check_pattern(self, value: str, rule_name: str, context: str):
        rule = self.config.get(rule_name)
        if not rule:
            return

        pattern = rule.get("pattern")
        if pattern and not re.match(pattern, value):
            message = rule.get("message", f"Must match pattern {pattern}")
            self.errors.append(f"[{rule_name}] {context}: '{value}' - {message}")

    def check_models(self):
        try:
            models = self.loader.load_models()
        except Exception as e:
            self.errors.append(f"Failed to load models: {e}")
            return

        for model in models:
            # Check model name
            self._check_pattern(model.name, "model_name", f"Model '{model.name}'")

            # Check column names
            self._check_model_columns(model)

    def _check_model_columns(self, model):
        try:
            # Transpile to Databricks/Spark dialect to handle specific syntax if needed
            # For now, generic parsing should work for most SELECTs
             # We need to replace {vars} with dummy values to make it valid SQL for parsing
            
            # Simple heuristic replacement for parsing
            # This might fail on complex jinja-like usage, but good for basic {ref}
            clean_sql = re.sub(r"\{.*?\}", "dummy_table", model.sql)
            
            expression = sqlglot.parse_one(clean_sql)
            
            # We are interested in the final projection
            # This is a best-effort check.
            if isinstance(expression, exp.Select):
                for projection in expression.selects:
                    col_name = projection.alias_or_name
                    if col_name != "*":
                        self._check_pattern(col_name, "column_name", f"Model '{model.name}' Column")
            
        except Exception as e:
            # Don't fail the whole lint run if one file can't be parsed
            # Just warn or add to errors? Maybe a warning is better for parsing issues.
            logger.warning(f"Warning: Could not check columns for {model.name}: {e}")

    def check_sources(self):
        profile_path = os.path.join(self.project_dir, "profiles.yml")
        if not os.path.exists(profile_path):
            return # No profiles, skip source check
        
        try:
            # Load config using the same logic as the runner (supports nested profiles)
            config = load_config_from_yaml(profile_path)
            
            sources = config.get("sources", {})
            for source_name in sources.keys():
                self._check_pattern(source_name, "source_name", "Source")
                
        except Exception as e:
            self.errors.append(f"Failed to parse profiles.yml: {e}")
