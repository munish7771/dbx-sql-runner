# CLI and Configuration Reference

## Project Initialization

### `init`
Initialize a new `dbx-sql-runner` project in the current directory.

```bash
dbx-sql-runner init [project_name]
```

**Arguments:**
- `project_name`: Name of the directory to create.

**What it does:**
- Creates the project directory.
- Generates a sample `profiles.yml` (do not commit this!).
- Creates a `models/` directory with a sample SQL model.

---

## Execution

### `run`
Execute the SQL models in your project.

```bash
dbx-sql-runner run [options]
```

**Options:**
- `--profile <path>`: Path to your configuration file (default: `profiles.yml`).
- `--target <env>`: Target environment to run against (coming soon).

**Behavior:**
1.  **Parses** all SQL files in `models/`.
2.  **Builds** a dependency graph (DAG) based on `{upstream_model}` references.
3.  **Executes** models in topological order (parents first).
4.  **Lints** code before execution (if enabled).

---

## Planning

### `build`
Compile the project and show the execution plan without running SQL against the warehouse.

```bash
dbx-sql-runner build
```

**Use cases:**
- Debugging dependency issues.
- Verifying SQL syntax (via linter).
- Checking Dag structure.
