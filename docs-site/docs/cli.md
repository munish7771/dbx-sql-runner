---
sidebar_position: 2
title: CLI Reference
description: "Command Line Interface Guide for DBX SQL Runner"
---

# CLI Reference

`dbx-sql-runner` provides a command-line interface to manage and execute your SQL projects.

## Installation

Ensure you have the package installed:

```bash
pip install dbx-sql-runner
```

## Commands

### `init`

Initialize a new `dbx-sql-runner` project with a scaffold structure.

```bash
dbx-sql-runner init [project_name]
```

- **Arguments**:
  - `project_name` (optional): Name of the directory to create. Defaults to current directory (`.`).

**Example**:
```bash
dbx-sql-runner init my_analytics_project
```

### `run`

Execute the SQL models against the database. This calculates the dependency graph and runs models in the correct order.

```bash
dbx-sql-runner run [options]
```

- **Options**:
  - `--models-dir`: Directory containing your `.sql` models. Default: `models`.
  - `--profile`: Path to the YAML configuration file. Default: `profiles.yml`.

**Example**:
```bash
# Run with default settings
dbx-sql-runner run

# Run with custom config
dbx-sql-runner run --profile configs/prod.yml --models-dir src/models
```

### `build`

Preview the execution plan without running the actual SQL. Useful for debugging dependencies and checking what models will be executed.

```bash
dbx-sql-runner build [options]
```

- **Options**:
  - `--models-dir`: Directory containing your `.sql` models. Default: `models`.
  - `--profile`: Path to the YAML configuration file. Default: `profiles.yml`.

### `lint`

Check your project for adherence to naming conventions and configuration validity.

```bash
dbx-sql-runner lint [options]
```

- **Options**:
  - `--config`: Path to the linter configuration file. Default: `lint.yml`.

**Default Rules**:
- Model names must be `snake_case`.
- Source names must be `snake_case`.
- Column names in final projection must be `snake_case`.

**Configuration**:
You can customize rules by creating a `lint.yml`:

```yaml
rules:
  model_name:
    pattern: "^[a-z0-9_]+$"
    message: "Models should be lower case!"
```
