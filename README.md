# dbx-sql-runner

A lightweight, library-first SQL transformation tool for Databricks SQL, inspired by DBT.

## Features

- **Simple SQL Models**: Just write `.sql` files. No complex boilerplate.
- **Automated Dependency Management**: Reference other models using `{upstream_model}` and let the runner build the DAG for you.
- **Environment Aware**: Seamlessly switch between Dev and Prod using `profiles.yml` and Environment Variables.
- **Library Design**: Import `dbx_sql_runner` in your Python scripts (great for Airflow/Databricks Jobs) or run it via CLI.
- **Flexible Sources**: Define external tables in `profiles.yml` and reference them as `{source_name}` in your SQL.
- **Automated Linting**: Built-in linter (using Ruff) to ensure code quality.

## Installation

### Development
To install the project in editable mode:

```bash
pip install -e .
```

### Running Tests
To run the automated test suite:

```bash
pip install .[dev]
python -m pytest
```

### Production
To install the package normally:

```bash
pip install dbx-sql-runner
```

## Configuration (profiles.yml)
Create a `profiles.yml` file to store your credentials. **Do not commit this file to version control.**

```yaml
server_hostname: "dbc-xxxxxxxx-xxxx.cloud.databricks.com"
http_path: "/sql/1.0/warehouses/xxxxxxxxxxxxxxxx"
access_token: "${DBX_ACCESS_TOKEN}"  # Env var expansion supported for any field
catalog: "my_catalog"
schema: "my_schema"
sources:
    # keys here can be used in SQL as {my_source}
    my_source: "prod_catalog.schema.table"
    raw_sales: "raw_data.sales_table"
```

## Usage

### 1. CLI (Easiest)
Run your project from the command line. By default, it looks for `profiles.yml` in the current directory.

```bash
# Initialize a new project
dbx-sql-runner init my_project

# Run with default profile (profiles.yml)
dbx-sql-runner run

# Run with custom profile
dbx-sql-runner run --profile my_config.yml

# Preview execution plan
dbx-sql-runner build
```

### 2. Python (Advanced)
For fine-grained control (e.g., inside a Databricks Job):

```python
from dbx_sql_runner.api import run_project

# Run models in the 'models/' directory using the config from 'profiles.yml'
run_project(models_dir="models", config_path="profiles.yml")
```

## Project Structure
```text
.
├── models/                  # SQL files (.sql)
│   └── example.sql
├── dbx_sql_runner/          # Library source code
│   ├── adapters/            # Database Adapters
│   ├── api.py               # Public API
│   ├── cli.py               # Command Line Interface
│   ├── exceptions.py        # Custom Exceptions
│   ├── linter.py            # Linting Logic
│   ├── models.py            # Data Models
│   ├── project.py           # Model Loading & DAG
│   ├── runner.py            # Execution Orchestrator
│   └── scaffold.py          # Project Scaffolding
├── profiles.yml             # Configuration (gitignored)
├── pyproject.toml           # Project metadata
└── README.md
```

## Defining Models
Create `.sql` files in your `models/` directory. 
- Use header comments for metadata.
- Use `{upstream_model}` syntax for references (automatically infers dependency).
- Use `{source_name}` to reference sources defined in `profiles.yml`.

```sql
-- name: my_table
-- materialized: table
-- partition_by: date, region

/*
    Multi-line comments are supported.
    Refer to upstream models: {upstream_model}
    Refer to sources: {my_source}
*/

SELECT 
    id,
    amount
FROM {source_view}
WHERE id > 100
```
