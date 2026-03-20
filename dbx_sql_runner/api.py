import yaml
import os
from .runner import DbxRunner
from .project import ProjectLoader
from .adapters.databricks import DatabricksAdapter


def load_config_from_yaml(path):
    with open(path, "r") as f:
        content = f.read()

    # Expand environment variables ${VAR}
    expanded_content = os.path.expandvars(content)
    raw = yaml.safe_load(expanded_content)

    # helper to resolve profile
    if "target" in raw and "outputs" in raw:
        target = raw["target"]
        outputs = raw.get("outputs", {})
        if target not in outputs:
            raise ValueError(f"Target environment '{target}' not found in 'outputs'")

        config = outputs[target]
        config["target_name"] = target
        return config
    return raw


def run_project(config_path, preview=False):
    config = load_config_from_yaml(config_path)

    model_paths = config.get("model_paths")
    if not model_paths:
        raise ValueError(
            "'model_paths' is required in profiles.yml but was not found in the active target."
        )

    loader = ProjectLoader(model_paths=model_paths)
    adapter = DatabricksAdapter(config)
    runner = DbxRunner(loader, adapter, config)

    runner.run(preview=preview)
