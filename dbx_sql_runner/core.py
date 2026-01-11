import os
import networkx as nx
import yaml
import re
import hashlib
from databricks import sql

class DbxRunnerProject:
    def __init__(self, models_dir, config):
        self.models_dir = models_dir
        # Config can be flat (old style) or structured (target/outputs)
        # We normalize it to the active environment's config here.
        self.config = self._resolve_config(config)
        self.models = self._load_models()
        self.dag = self._build_dag()

    def _resolve_config(self, raw_config):
        # Check if it's the new format with "target" and "outputs"
        if "target" in raw_config and "outputs" in raw_config:
            target = raw_config["target"]
            outputs = raw_config.get("outputs", {})
            if target not in outputs:
                 raise ValueError(f"Target environment '{target}' not found in 'outputs'")
            return outputs[target]
        # Fallback for flat config (backward compatibility)
        return raw_config


    def _load_models(self):
        models = []
        if not os.path.exists(self.models_dir):
            raise ValueError(f"Models directory not found: {self.models_dir}")
            
        for f in os.listdir(self.models_dir):
            if f.endswith(".sql"):
                models.append(self._parse_model_file(os.path.join(self.models_dir, f)))
        return models

    def _parse_model_file(self, path):
        with open(path, 'r') as f:
            lines = f.readlines()
        meta = {"depends_on": []}
        sql_lines = []
        for line in lines:
            if line.startswith("--"):
                if "name:" in line:
                    meta["name"] = line.split("name:")[1].strip()
                elif "materialized:" in line:
                    meta["materialized"] = line.split("materialized:")[1].strip()
                elif "depends_on:" in line:
                    deps = line.split("depends_on:")[1].strip()
                    meta["depends_on"] = [d.strip() for d in deps.split(",") if d.strip()]
            else:
                sql_lines.append(line)
        
        sql_body = ''.join(sql_lines)
        
        # Inference: Find all {variable} patterns and add them as dependencies
        # This allows users to write "FROM {upstream_model}" without explicit Depends On
        inferred_deps = re.findall(r"\{(\w+)\}", sql_body)
        for dep in inferred_deps:
            if dep not in meta["depends_on"]:
                meta["depends_on"].append(dep)

        return Model(
            name=meta.get("name", os.path.basename(path).replace(".sql", "")),
            materialized=meta.get("materialized", "view"),
            sql_body=sql_body,
            depends_on=meta.get("depends_on", [])
        )

    def _build_dag(self):
        dag = nx.DiGraph()
        model_map = {m.name: m for m in self.models}
        
        for m in self.models:
            dag.add_node(m.name, model=m)
            for dep in m.depends_on:
                # Only add edge if the dependency exists in our project
                # (Users might reference a non-project table using variables, though less common)
                if dep in model_map:
                    dag.add_edge(dep, m.name)
                # We silently ignore deps that are not found (maybe they are external vars?)
                # But typically this would crash the formatter if not handled.
                # For now, we assume if it's in {} it's a model reference.
        return dag

    def run(self, preview=False):
        # Topological sort
        try:
            sorted_models = list(nx.topological_sort(self.dag))
        except nx.NetworkXUnfeasible:
            raise ValueError("Cyclic dependency detected in models")

        model_map = {m.name: m for m in self.models}
        
        catalog = self.config.get('catalog')
        schema = self.config.get('schema')
        staging_schema = f"{schema}_staging"
        
        if not catalog or not schema:
            raise ValueError("Config must include 'catalog' and 'schema'")
            
        print(f"Found {len(sorted_models)} models.")
        
        print(f"Connecting to Databricks host: {self.config.get('server_hostname')}")
        with sql.connect(**self.config) as conn:
            # Metadata & Preparation
            self._ensure_metadata_table(conn, catalog, schema)
            
            # Generate Execution ID (Incremental)
            self.execution_id = self._get_next_execution_id(conn, catalog, schema)
            print(f"Run Execution ID: {self.execution_id}")
            
            metadata = self._get_metadata(conn, catalog, schema)
            
            # Plan Execution
            execution_plan = []
            context_map = {} # model_name -> fqn (target or staging)
            
            # Decide SKIP vs EXECUTE
            # We must iterate in DAG order to resolve contexts
            for name in sorted_models:
                model = model_map[name]
                
                # Render with TARGET context to compute stable hash
                # (Ideally hash should depend on logic, not transient staging names)
                # But wait, if upstream changes, downstream SQL might not change textually, 
                # but data changes. We normally REBUILD if upstream rebuilds?
                # User said: "next runs it checks if the sql is changed from before... skip if schema hasnt changed for views."
                # Standard dbt behavior: View is skipped if SQL is same. 
                # Table is built from scratch? User said "For tables it cretes from scratch obviously."
                # So SKIP logic applies ONLY to Views?
                
                # Let's verify upstream dependency.
                # If upstream changed (was executed), we probably should re-verify/re-run downstream view? 
                # Databricks views are logical, so if upstream table is dropped and swapped, 
                # the view might break if not refreshed? No, Swap is atomic RENAME. 
                # Does View hold ID reference or Name reference? Usually Name.
                # So if Upstream is swapped, View pointing to Upstream Name is fine.
                # So we CAN skip View if SQL is same.
                
                # Context for Hash: Use generic placeholder or Target FQN?
                # Using Target FQN allows detecting if config (catalog/schema) changes.
                target_context = {m: f"{catalog}.{schema}.{model_map[m].name}" for m in model_map}
                current_sql_content = self._render_sql(model.sql, target_context)
                current_hash = hashlib.sha256(current_sql_content.encode('utf-8')).hexdigest()
                
                last_hash = metadata.get(name, {}).get("sql_hash")
                
                action = "EXECUTE"
                if model.materialized == 'view' and last_hash == current_hash:
                    action = "SKIP"
                
                # Special case: If ANY upstream was validly executed (rebuilt), 
                # maybe we shouldn't skip? 
                # If we skip View, it points to Target Upstream. 
                # If Upstream was rebuilt, Target Upstream is new. View still works. OK.
                
                execution_plan.append({
                    "name": name, 
                    "action": action, 
                    "model": model,
                    "hash": current_hash
                })
                
                # Determine Context for downstream
                # If we EXECUTE this model, it will exist in STAGING during build phase.
                # If we SKIP this model, it exists in TARGET.
                # BUT: Downstream models attempting to read THIS model during their build need to know where to look.
                # If we are building Downstream in Staging, it should read Upstream from Staging (if rebuilt) 
                # or Target (if skipped).
                
                if action == "EXECUTE":
                    context_map[name] = f"{catalog}.{staging_schema}.{name}"
                else:
                    context_map[name] = f"{catalog}.{schema}.{name}"

            # Print Plan
            print("Execution Plan:")
            for item in execution_plan:
                print(f" - {item['name']}: {item['action']}")
                
            if preview:
                return

            # Phase 1: Create Staging Schema
            with conn.cursor() as cursor:
                print(f"Ensuring staging schema {catalog}.{staging_schema} exists...")
                # Cleanup potential leftover
                cursor.execute(f"DROP SCHEMA IF EXISTS {catalog}.{staging_schema} CASCADE") 
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{staging_schema}")

            # Phase 2: Execution (into Staging)
            try:
                for item in execution_plan:
                    if item['action'] == "SKIP":
                        continue
                        
                    model = item['model']
                    print(f"Building {model.name} in Staging...")
                    
                    # Use dynamic context (Target vs Staging)
                    self._execute_model(model, conn, context_map, catalog, staging_schema)
            except Exception as e:
                print(f"Execution failed. Aborting and cleaning up staging. Error: {e}")
                with conn.cursor() as cursor:
                    cursor.execute(f"DROP SCHEMA IF EXISTS {catalog}.{staging_schema} CASCADE")
                raise

            # Phase 3: Atomic Swap / Update
            print("Promoting successful models to Target...")
            for item in execution_plan:
                if item['action'] == "SKIP":
                    continue
                
                model = item['model']
                fqn_target = f"{catalog}.{schema}.{model.name}"
                fqn_staging = f"{catalog}.{staging_schema}.{model.name}"
                
                with conn.cursor() as cursor:
                    if model.materialized == 'view':
                        # Ensure target is clean (handle case where it was a table)
                        self._safe_drop(cursor, fqn_target)
                        
                        # Generate SQL for Target
                        target_context_all = {m: f"{catalog}.{schema}.{model_map[m].name}" for m in model_map}
                        final_sql = self._render_sql(model.sql, target_context_all)
                        cursor.execute(f"CREATE OR REPLACE VIEW {fqn_target} AS {final_sql}")
                        
                    elif model.materialized == 'table':
                        # Ensure target is clean
                        self._safe_drop(cursor, fqn_target)
                        cursor.execute(f"ALTER TABLE {fqn_staging} RENAME TO {fqn_target}")
                    
                    elif model.materialized == 'ddl':
                         # Treat as table-like artifact for promotion
                         self._safe_drop(cursor, fqn_target)
                         try:
                             cursor.execute(f"ALTER TABLE {fqn_staging} RENAME TO {fqn_target}")
                         except Exception as e:
                             print(f"Warning: Could not rename DDL artifact {fqn_staging}. Error: {e}")

                    # Phase 4: Update Metadata
                    self._update_metadata(conn, catalog, schema, model.name, item['hash'], model.materialized, self.execution_id)

            # Cleanup
            with conn.cursor() as cursor:
                 cursor.execute(f"DROP SCHEMA IF EXISTS {catalog}.{staging_schema} CASCADE")
                 
        print("All models executed and promoted successfully.")

    def _safe_drop(self, cursor, fqn):
        """Drops table or view, handling type mismatches."""
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {fqn}")
        except Exception as e:
            # Check for "is a VIEW" or similar error codes
            # SQLSTATE 42809 is common for Wrong Object Type
            msg = str(e).lower()
            if "view" in msg or "42809" in msg:
                 cursor.execute(f"DROP VIEW IF EXISTS {fqn}")
            else:
                raise

    def _ensure_metadata_table(self, conn, catalog, schema):
        with conn.cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {catalog}.{schema}._dbx_model_metadata (
                    model_name STRING,
                    sql_hash STRING,
                    materialized STRING,
                    last_executed_at TIMESTAMP,
                    execution_id BIGINT
                )
            """)
            
            # Schema Evolution: Add execution_id column if it doesn't exist
            # Databricks SQL doesn't support "ADD COLUMN IF NOT EXISTS" cleanly in all versions or simple syntax
            # But we can try to add it and ignore error if it exists.
            try:
                cursor.execute(f"ALTER TABLE {catalog}.{schema}._dbx_model_metadata ADD COLUMNS (execution_id BIGINT)")
            except Exception:
                # Column likely exists or table doesn't exist yet (create processed above would handle new table)
                pass

    def _get_metadata(self, conn, catalog, schema):
        meta = {}
        try:
            with conn.cursor() as cursor:
                # Deduplicate by taking latest
                cursor.execute(f"SELECT model_name, sql_hash, materialized, execution_id FROM {catalog}.{schema}._dbx_model_metadata ORDER BY last_executed_at ASC")
                rows = cursor.fetchall()
                for row in rows:
                    meta[row[0]] = {
                        "sql_hash": row[1], 
                        "materialized": row[2],
                        "execution_id": row[3] if len(row) > 3 else None
                    }
        except Exception:
            # Table might not exist or be empty/corrupt, ignore
            pass
        return meta

    def _get_next_execution_id(self, conn, catalog, schema):
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT MAX(execution_id) FROM {catalog}.{schema}._dbx_model_metadata")
                row = cursor.fetchone()
                if row and row[0] is not None:
                    try:
                        return int(row[0]) + 1
                    except ValueError:
                         # Handle case where it might be string UUID from before
                         return 1
                return 1
        except Exception:
             # If table doesn't exist or query fails
             return 1

    def _update_metadata(self, conn, catalog, schema, model_name, sql_hash, materialized, execution_id):
        with conn.cursor() as cursor:
            # Delta Lake allows MERGE or DELETE/INSERT. Simple INSERT for log history is fine? 
            # Or overwrite? User said "logs the current execution... checks if changed".
            # We can keep history or just latest state. Let's keep history for now but check latest in logic.
            # Actually, let's just Insert.
            cursor.execute(f"""
                INSERT INTO {catalog}.{schema}._dbx_model_metadata 
                VALUES ('{model_name}', '{sql_hash}', '{materialized}', current_timestamp(), {execution_id})
            """)

    def _execute_model(self, model, conn, context, catalog, schema):
        # Render the SQL using regex substitution instead of format() to avoid conflicts with JSON
        rendered_sql = self._render_sql(model.sql, context)
            
        # Validate schema before execution
        self._validate_model(model, rendered_sql, conn)
        
        fqn = f"{catalog}.{schema}.{model.name}"
        
        partition_clause = ""
        if model.partition_by:
            cols = ", ".join(model.partition_by)
            partition_clause = f"PARTITIONED BY ({cols})"
        
        if model.materialized == 'view':
            ddl = f"CREATE OR REPLACE VIEW {fqn} AS {rendered_sql}"
        elif model.materialized == 'table':
            ddl = f"CREATE OR REPLACE TABLE {fqn} {partition_clause} AS {rendered_sql}"
        elif model.materialized == 'ddl':
            ddl = rendered_sql
        else:
            print(f"Unknown materialization '{model.materialized}' for {model.name}, defaulting to view")
            ddl = f"CREATE OR REPLACE VIEW {fqn} AS {rendered_sql}"

        with conn.cursor() as cursor:
            cursor.execute(ddl)

    def _validate_model(self, model, rendered_sql, conn):
        print(f"Validating schema for {model.name}...")
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"EXPLAIN {rendered_sql}")
        except Exception as e:
            print(f"Validation failed for model {model.name}: {e}")
            raise ValueError(f"Schema validation failed for model {model.name}. Please check if referenced tables and columns exist.") from e

    def _render_sql(self, sql_body, context):
        # Replace {var} with value from context.
        # Unknown vars are left as is (or we could raise error if strict?)
        # For now, safe replacement: ONLY replace if key is in context.
        # This handles {"json": "value"} correctly as it doesn't match {var} if var is alphanumeric.
        # But wait, {"key"} DOES NOT match {\w+}. " is not \w.
        # So {\w+} is safe for quoted json keys.
        def replace(match):
            key = match.group(1)
            return context.get(key, match.group(0))
        
        return re.sub(r"\{(\w+)\}", replace, sql_body)

    def _parse_model_file(self, path):
        with open(path, 'r') as f:
            lines = f.readlines()
        meta = {"depends_on": [], "partition_by": []}
        sql_lines = []
        for line in lines:
            if line.startswith("--"):
                if "name:" in line:
                    meta["name"] = line.split("name:")[1].strip()
                elif "materialized:" in line:
                    meta["materialized"] = line.split("materialized:")[1].strip()
                elif "depends_on:" in line:
                    deps = line.split("depends_on:")[1].strip()
                    meta["depends_on"] = [d.strip() for d in deps.split(",") if d.strip()]
                elif "partition_by:" in line:
                    parts = line.split("partition_by:")[1].strip()
                    meta["partition_by"] = [p.strip() for p in parts.split(",") if p.strip()]
            else:
                sql_lines.append(line)
        
        sql_body = ''.join(sql_lines)
        
        # Inference: Find all {variable} patterns and add them as dependencies
        # This allows users to write "FROM {upstream_model}" without explicit Depends On
        inferred_deps = re.findall(r"\{(\w+)\}", sql_body)
        for dep in inferred_deps:
            if dep not in meta["depends_on"]:
                meta["depends_on"].append(dep)

        return Model(
            name=meta.get("name", os.path.basename(path).replace(".sql", "")),
            materialized=meta.get("materialized", self.config.get("materialized", "ddl")),
            sql_body=sql_body,
            depends_on=meta.get("depends_on", []),
            partition_by=meta.get("partition_by", [])
        )

class Model:
    def __init__(self, name, materialized, sql_body, depends_on, partition_by):
        self.name = name
        self.materialized = materialized
        self.sql = sql_body
        self.depends_on = depends_on
        self.partition_by = partition_by

def load_config_from_yaml(path):
    with open(path, 'r') as f:
        return yaml.safe_load(f)
