import hashlib
import re
from typing import Dict, Any, List
import time
import logging
from .models import Model
from .adapters.base import BaseAdapter
from .project import ProjectLoader, DependencyGraph

    
logger = logging.getLogger(__name__)

class DbxRunner:
    def __init__(self, project_loader: ProjectLoader, adapter: BaseAdapter, config: Dict[str, Any]):
        self.loader = project_loader
        self.adapter = adapter
        self.config = config
        self.catalog = config.get('catalog')
        self.schema = config.get('schema')
        self.sources = config.get('sources', {})
        # Staging schema removed; using suffixes instead

    def run(self, preview=False):
        # Load and Sort Models
        models = self.loader.load_models()
        graph = DependencyGraph(models)
        sorted_models = graph.get_execution_order()
        
        # Get Metadata / Context
        all_meta = self.adapter.get_metadata(self.catalog, self.schema)
        
        # Generate Execution ID (Incremental)
        execution_id = self.adapter.get_next_execution_id(self.catalog, self.schema)
        
        if not self.config.get('silent'):
            logger.info(f"SQL RUNNER | Run Execution ID: {execution_id}")
            logger.info(f"Found {len(sorted_models)} models")

        # Plan Execution
        execution_plan = []
        context_map = {} # model_name -> fqn (target or staging)
        
        # Initialize context mapping with Configured Sources
        # This allows {source_name} to be resolved to their configured FQN
        context_map.update(self.sources)

        model_map = {m.name: m for m in models}

        # Need to iterate in sorted order to build context map
        for model in sorted_models:
             # Calculate generic hash (using target context)
            target_context = {m: f"{self.catalog}.{self.schema}.{model_map[m].name}" for m in model_map}
            # Add sources to target context as well
            target_context.update(self.sources)
            
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
                # Staging FQN: suffix with __staging
                context_map[model.name] = f"{self.catalog}.{self.schema}.{model.name}__staging"
            else:
                context_map[model.name] = f"{self.catalog}.{self.schema}.{model.name}"

        # Print Plan
        if not self.config.get('silent'):
            logger.info("Execution Plan:")
            for item in execution_plan:
                logger.info(f" - {item['name']}: {item['action']}")

        if preview:
            return

        # Execute
        results = {"PASS": 0, "WARN": 0, "ERROR": 0, "SKIP": 0}
        model_status = {} # model_name -> status
        
        total_models = len([i for i in execution_plan if i['action'] == "EXECUTE"])
        current_idx = 0
        
        for item in execution_plan:
            model = item['model']
            
            if item['action'] == "SKIP":
                model_status[model.name] = "SKIP"
                results["SKIP"] += 1
                continue
            
            # Check Upstream Dependencies
            upstream_failed = False
            for dep in model.depends_on:
                if dep in model_status and model_status[dep] in ["ERROR", "SKIP_UPSTREAM"]:
                    upstream_failed = True
                    break
            
            if upstream_failed:
                model_status[model.name] = "SKIP_UPSTREAM"
                logger.info(f"Skipping {model.name} due to upstream failure")
                results["SKIP"] += 1
                continue

            current_idx += 1
            self._log_start(current_idx, total_models, model)
            start_time = time.time()
            
            try:
                # Pass suffix-based FQN directly or let execute handle it?
                # _execute_model logic needs update to handle FQN construction
                staging_fqn = f"{self.catalog}.{self.schema}.{model.name}__staging"
                self._execute_model(model, context_map, staging_fqn)
                
                duration = time.time() - start_time
                self._log_end(current_idx, total_models, model, duration)
                model_status[model.name] = "SUCCESS"
                results["PASS"] += 1
                
            except Exception as e:
                logger.error(f"Error executing {model.name}: {e}")
                model_status[model.name] = "ERROR"
                results["ERROR"] += 1
            
        # Promote / Atomic Swap
        # print("Promoting models...") 
        for item in execution_plan:
            model = item['model']
            
            # Only promote if SUCCESS (skip SKIPPED, ERROR, and originally SKIP)
            if model_status.get(model.name) != "SUCCESS":
                continue

            try:
                self._promote_model(model)
                self.adapter.update_metadata(self.catalog, self.schema, model.name, item['hash'], model.materialized, execution_id)
            except Exception as e:
                logger.error(f"Error promoting {model.name}: {e}")
                results["ERROR"] += 1 # Should we count promotion error as error? Yes.
                # Adjust PASS count? Technically it executed but didn't promote.
                # Let's just increment ERROR.

        # Cleanup
        self._cleanup_staging(execution_plan)
        logger.info(f"Done. PASS={results['PASS']} WARN={results['WARN']} ERROR={results['ERROR']} SKIP={results['SKIP']} TOTAL={results['PASS']+results['ERROR']+results['SKIP']}")

    def _log_start(self, idx, total, model):
        # Timestamp handled by logging formatter
        logger.info(f"{idx} of {total} START sql {model.materialized} model {self.catalog}.{self.schema}.{model.name} ... [RUN]")

    def _log_end(self, idx, total, model, duration):
        logger.info(f"{idx} of {total} OK created {model.materialized} model {self.catalog}.{self.schema}.{model.name} ... [OK in {duration:.2f}s]")

    def _execute_model(self, model: Model, context: Dict[str, str], fqn: str):
        # Inject {this} to point to the current FQN (staging or target)
        local_context = context.copy()
        local_context["this"] = fqn
        
        rendered_sql = self._render_sql(model.sql, local_context)
        
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

    def _promote_model(self, model: Model):
        fqn_target = f"{self.catalog}.{self.schema}.{model.name}"
        fqn_staging = f"{self.catalog}.{self.schema}.{model.name}__staging"
        
        # Helper to drop target before swap (idempotency)
        self._safe_drop_target(fqn_target)

        if model.materialized == 'view':
             # For views, we simply re-create them in the Target schema.
             # We MUST re-render the SQL using the Target schema context so the view definition points to production tables.
             # TODO: Optimize context loading
             target_context = {m.name: f"{self.catalog}.{self.schema}.{m.name}" for m in self.loader.load_models()}
             
             final_sql = self._render_sql(model.sql, target_context)
             self.adapter.execute(f"CREATE OR REPLACE VIEW {fqn_target} AS {final_sql}")
             
        elif model.materialized == 'table':
            # Atomic Swap (Rename)
            self.adapter.execute(f"ALTER TABLE {fqn_staging} RENAME TO {fqn_target}")
        
        elif model.materialized == 'ddl':
              try:
                  self.adapter.execute(f"ALTER TABLE {fqn_staging} RENAME TO {fqn_target}")
              except Exception as e:
                  logger.warning(f"Warning: Could not rename DDL artifact {fqn_staging}. Error: {e}")

    def _cleanup_staging(self, execution_plan: List[Dict]):
        # print("Cleaning up...") # staging artifacts...")
        for item in execution_plan:
            # We cleanup everything, skipped or not (though skipped won't exist usually)
            # Actually only need to cleanup things we executed.
            if item['action'] == "EXECUTE":
                fqn_staging = f"{self.catalog}.{self.schema}.{item['name']}__staging"
                self._safe_drop_target(fqn_staging)

    def _safe_drop_target(self, fqn: str):
        try:
             self.adapter.execute(f"DROP TABLE IF EXISTS {fqn}")
        except Exception:
             self.adapter.execute(f"DROP VIEW IF EXISTS {fqn}")

    def _render_sql(self, sql_body, context):
        def replace(match):
            key = match.group(1)
            return context.get(key, match.group(0))
        return re.sub(r"\{(\w+)\}", replace, sql_body)
