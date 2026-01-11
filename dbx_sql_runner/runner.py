import hashlib
import re
from typing import Dict, Any
from .models import Model
from .adapters.base import BaseAdapter
from .project import ProjectLoader, DependencyGraph

class DbxRunner:
    def __init__(self, project_loader: ProjectLoader, adapter: BaseAdapter, config: Dict[str, Any]):
        self.loader = project_loader
        self.adapter = adapter
        self.config = config
        self.catalog = config.get('catalog')
        self.schema = config.get('schema')
        self.staging_schema = f"{self.schema}_staging"

    def run(self, preview=False):
        # Load and Sort Models
        models = self.loader.load_models()
        graph = DependencyGraph(models)
        sorted_models = graph.get_execution_order()
        
        print(f"Found {len(sorted_models)} models.")
        
        # Get Metadata / Context
        all_meta = self.adapter.get_metadata(self.catalog, self.schema)
        
        # Generate Execution ID (Incremental)
        execution_id = self.adapter.get_next_execution_id(self.catalog, self.schema)
        print(f"Run Execution ID: {execution_id}")

        # Plan Execution
        execution_plan = []
        context_map = {} # model_name -> fqn (target or staging)
        model_map = {m.name: m for m in models}

        # Need to iterate in sorted order to build context map
        for model in sorted_models:
             # Calculate generic hash (using target context)
            target_context = {m: f"{self.catalog}.{self.schema}.{model_map[m].name}" for m in model_map}
            current_sql_content = self._render_sql(model.sql, target_context)
            current_hash = hashlib.sha256(current_sql_content.encode('utf-8')).hexdigest()
            
            last_hash = all_meta.get(model.name, {}).get("sql_hash")
            
            action = "EXECUTE"
            if model.materialized == 'view' and last_hash == current_hash:
                action = "SKIP"
            
            execution_plan.append({
                "name": model.name,
                "action": action,
                "model": model,
                "hash": current_hash
            })
            
            if action == "EXECUTE":
                context_map[model.name] = f"{self.catalog}.{self.staging_schema}.{model.name}"
            else:
                context_map[model.name] = f"{self.catalog}.{self.schema}.{model.name}"

        # Print Plan
        print("Execution Plan:")
        for item in execution_plan:
            print(f" - {item['name']}: {item['action']}")

        if preview:
            return

        # Execute
        self.adapter.ensure_schema_exists(self.catalog, self.staging_schema)
        
        try:
            for item in execution_plan:
                if item['action'] == "SKIP":
                    continue
                
                model = item['model']
                print(f"Building {model.name} in Staging...")
                self._execute_model(model, context_map, self.staging_schema)
                
            # Promote / Atomic Swap
            print("Promoting successful models to Target...")
            for item in execution_plan:
                if item['action'] == "SKIP":
                    continue
                
                model = item['model']
                self._promote_model(model, self.staging_schema)
                self.adapter.update_metadata(self.catalog, self.schema, model.name, item['hash'], model.materialized, execution_id)
                
        except Exception as e:
            print(f"Execution failed. Aborting. Error: {e}")
            self.adapter.drop_schema_cascade(self.catalog, self.staging_schema)
            raise
        
        # Cleanup
        self.adapter.drop_schema_cascade(self.catalog, self.staging_schema)
        print("All models executed and promoted successfully.")

    def _execute_model(self, model: Model, context: Dict[str, str], schema: str):
        rendered_sql = self._render_sql(model.sql, context)
        # Validation could go here (explain)
        
        fqn = f"{self.catalog}.{schema}.{model.name}"
        
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
             ddl = f"CREATE OR REPLACE VIEW {fqn} AS {rendered_sql}"
        
        self.adapter.execute(ddl)

    def _promote_model(self, model: Model, staging_schema: str):
        fqn_target = f"{self.catalog}.{self.schema}.{model.name}"
        fqn_staging = f"{self.catalog}.{self.staging_schema}.{model.name}"
        
        # Helper to drop target before swap (idempotency)
        self._safe_drop_target(fqn_target)

        if model.materialized == 'view':
             # For views, we simply re-create them in the Target schema.
             # We MUST re-render the SQL using the Target schema context so the view definition points to production tables.
             # Re-build context map for ALL models pointing to target
             # TODO: Optimize by computing this once.
             target_context = {m.name: f"{self.catalog}.{self.schema}.{m.name}" for m in self.loader.load_models()}
             
             final_sql = self._render_sql(model.sql, target_context)
             self.adapter.execute(f"CREATE OR REPLACE VIEW {fqn_target} AS {final_sql}")
             
        elif model.materialized == 'table':
            # Atomic Swap (Rename)
            # Since we dropped target above, we just rename Staging -> Target
            self.adapter.execute(f"ALTER TABLE {fqn_staging} RENAME TO {fqn_target}")
        
        elif model.materialized == 'ddl':
             # Treat like a table for promotion
             try:
                 self.adapter.execute(f"ALTER TABLE {fqn_staging} RENAME TO {fqn_target}")
             except Exception as e:
                 print(f"Warning: Could not rename DDL artifact {fqn_staging}. Error: {e}")

    def _safe_drop_target(self, fqn: str):
        # We try to drop TABLE first, if it fails because it's a VIEW, we drop VIEW.
        # Or check metadata? Simpler to try/except or use adapter helper if robust.
        # Adapter's execute method raises error?
        # Let's try DROP TABLE IF EXISTS.
        try:
             self.adapter.execute(f"DROP TABLE IF EXISTS {fqn}")
        except Exception:
             # Likely it's a view
             self.adapter.execute(f"DROP VIEW IF EXISTS {fqn}")

    def _render_sql(self, sql_body, context):
        def replace(match):
            key = match.group(1)
            return context.get(key, match.group(0))
        return re.sub(r"\{(\w+)\}", replace, sql_body)
