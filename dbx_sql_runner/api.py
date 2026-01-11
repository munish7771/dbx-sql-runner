import yaml
import os
from .runner import DbxRunner
from .project import ProjectLoader
from .adapters.databricks import DatabricksAdapter

def load_config_from_yaml(path):
    with open(path, 'r') as f:
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
        return outputs[target]
    return raw

def run_project(models_dir, config_path, preview=False):
    config = load_config_from_yaml(config_path)
    
    loader = ProjectLoader(models_dir)
    adapter = DatabricksAdapter(config)
    runner = DbxRunner(loader, adapter, config)
    
    runner.run(preview=preview)
