from .core import DbxRunnerProject, load_config_from_yaml

def run_project(models_dir, config_path, preview=False):
    config = load_config_from_yaml(config_path)
    project = DbxRunnerProject(models_dir, config)
    project.run(preview=preview)
