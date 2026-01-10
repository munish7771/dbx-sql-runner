# DBT Lite

A lightweight DBT-like SQL transformation tool for Databricks SQL.

## Installation

### Local Development
To install the project in editable mode (so changes to the code are immediately reflected):

```bash
pip install -e .
```

### Production
To install the package normally:

```bash
pip install .
```

## Usage

After installation, the `dbt-lite` command will be available in your terminal.

```bash
dbt-lite --models-dir ./models
```

Options:
- `--models-dir`: Directory containing SQL models (defaults to "models").

## Packaging

To build the project for distribution (creates a `.whl` and `.tar.gz` in `dist/`):

1. Install the build tool:
   ```bash
   pip install build
   ```

2. Run the build:
   ```bash
   python -m build
   ```
