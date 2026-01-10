# dbx-sql-runner

A lightweight, library-first SQL transformation tool for Databricks SQL, inspired by DBT.

## Features
- **Library-First Architecture**: Designed to be imported and orchestrated via Python scripts (e.g., in Databricks Jobs or Airflow).
- **YAML Configuration**: standard `profiles.yml` configuration management.
- **DAG Resolution**: Automatically resolves dependencies between SQL models (views/tables) using `networkx`.
- **Lightweight**: Minimal overhead, using `databricks-sql-connector`.

## Installation

### Local Development
To install the project in editable mode:

```bash
pip install -e .
```

### Production
To install the package normally:

```bash
pip install .
```

## Configuration (profiles.yml)
Create a `profiles.yml` file to store your credentials. **Do not commit this file to version control.**

```yaml
server_hostname: "dbc-xxxxxxxx-xxxx.cloud.databricks.com"
http_path: "/sql/1.0/warehouses/xxxxxxxxxxxxxxxx"
access_token: "dapi..."
```

## Usage

### 1. Python (Recommended)
The preferred way to run your project is by creating a `build.py` script. This gives you full control over the execution environment.

```python
from dbx_sql_runner.api import run_project

# Run models in the 'models/' directory using the config from 'profiles.yml'
run_project(models_dir="models", config_path="profiles.yml")
```

Run it via:
```bash
python build.py
```

### 2. CLI
You can also run the project directly from the command line:

```bash
dbx-sql-runner --models-dir ./models --profile profiles.yml
```

## Project Structure
```text
.
├── models/                  # SQL files (.sql)
│   └── example.sql
├── dbx_sql_runner/          # Library source code
├── profiles.yml             # Configuration (gitignored)
├── build.py                 # Build script
├── pyproject.toml           # Project metadata
└── README.md
```

## Defining Models
Create `.sql` files in your `models/` directory. Use header comments to define metadata.

```sql
-- name: my_table
-- materialized: table
-- depends_on: source_view

SELECT * FROM source_view
WHERE id > 100
```
