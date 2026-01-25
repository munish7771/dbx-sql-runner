---
sidebar_position: 3
title: Python API
description: "Python API Guide for DBX SQL Runner"
---

# Python API

`dbx-sql-runner` is designed to be imported and used within Python scripts, making it ideal for orchestration tools like Databricks Workflows, Airflow, or Prefect.

## `run_project`

The main entry point for running your SQL models is the `run_project` function.

```python
from dbx_sql_runner.api import run_project

def run_project(models_dir: str, config_path: str, preview: bool = False) -> None:
    ...
```

### Arguments

- **`models_dir`** (`str`): Absolute or relative path to the directory containing your `.sql` model files.
- **`config_path`** (`str`): Path to your `profiles.yml` configuration file.
- **`preview`** (`bool`, optional): If `True`, the runner will calculate dependencies and display the execution plan without running any SQL. Defaults to `False`.

### Usage Example

Here is a common pattern for running the project within a Databricks Job:

```python
import os
from dbx_sql_runner.api import run_project

# Path to your project assets
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
MODELS_DIR = os.path.join(PROJECT_ROOT, "models")
PROFILE_PATH = os.path.join(PROJECT_ROOT, "profiles.yml")

# Execute the project
if __name__ == "__main__":
    print("Starting DBX SQL Runner...")
    try:
        run_project(
            models_dir=MODELS_DIR,
            config_path=PROFILE_PATH
        )
        print("Success!")
    except Exception as e:
        print(f"Job failed: {e}")
        raise
```

## Exceptions

The API may raise the following exceptions:

- **`ValueError`**: If configuration is missing or invalid (e.g., missing target in `profiles.yml`).
- **`RuntimeError`**: If cyclic dependencies are detected in the model graph.
- **`Exception`**: For general execution failures (syntax errors, connection issues).
