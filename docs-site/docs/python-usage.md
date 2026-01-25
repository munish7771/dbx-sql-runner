# Python & Notebook Usage

`dbx-sql-runner` is designed to be library-first. This means you can import it directly into your Python scripts, Airflow DAGs, or Databricks Notebooks.

## Installation in Notebooks

In a Databricks notebook, you can install the library directly from PyPI (once published) or from a wheel.

```python
%pip install dbx-sql-runner
```

## Running a Project

The core API is `run_project`. This function handles the entire lifecycle: loading config, parsing models, building the DAG, and executing queries.

```python
from dbx_sql_runner.api import run_project

# Run the project assuming models are in 'models/' folder 
# and config is in 'profiles.yml'
run_project(
    models_dir="models",
    config_path="profiles.yml"
)
```

### Running Specific Models
*Note: Feature coming soon.*

## Advanced: Customizing Execution

You can programmatically construct the configuration if you don't want to use a `profiles.yml` file (e.g., using Secrets directly).

```python
from dbx_sql_runner.api import run_project
# Hypothetical API for direct config injection (check source for latest)
# config = { ... }
# run_project(config=config)
```

## Error Handling

The runner raises specific exceptions that you can catch to handle failures gracefully.

```python
from dbx_sql_runner.exceptions import ProjectConfigError, QueryExecutionError

try:
    run_project()
except QueryExecutionError as e:
    print(f"Model failed: {e.model_name}")
    # Trigger custom alert...
```
