import os
import networkx as nx
import yaml
import re
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

    def run(self):
        # Topological sort
        try:
            sorted_models = list(nx.topological_sort(self.dag))
        except nx.NetworkXUnfeasible:
            raise ValueError("Cyclic dependency detected in models")

        model_map = {m.name: m for m in self.models}
        
        catalog = self.config.get('catalog')
        schema = self.config.get('schema')
        
        if not catalog or not schema:
            raise ValueError("Config must include 'catalog' and 'schema'")

        # Create context for SQL formatting: {model_name: catalog.schema.table_name}
        context = {name: f"{catalog}.{schema}.{model.name}" for name, model in model_map.items()}

        print(f"Connecting to Databricks host: {self.config.get('server_hostname')}")
        with sql.connect(**self.config) as conn:
            # Ensure catalog and schema exist
            with conn.cursor() as cursor:
                print(f"Ensuring schema {catalog}.{schema} exists...")
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

            for name in sorted_models:
                model = model_map[name]
                print(f"Running {model.name}...")
                self._execute_model(model, conn, context, catalog, schema)
        print("All models executed successfully.")

    def _execute_model(self, model, conn, context, catalog, schema):
        # Render the SQL using Python's format syntax
        try:
            rendered_sql = model.sql.format(**context)
        except KeyError as e:
            print(f"Error rendering model {model.name}: Missing dependency {e}")
            raise
            
        fqn = f"{catalog}.{schema}.{model.name}"
        
        if model.materialized == 'view':
            ddl = f"CREATE OR REPLACE VIEW {fqn} AS {rendered_sql}"
        elif model.materialized == 'table':
            ddl = f"CREATE OR REPLACE TABLE {fqn} AS {rendered_sql}"
        elif model.materialized == 'ddl':
            # For raw DDL, the user is responsible for writing the full statement.
            # They should use the {model_name} placeholder (which maps to fqn) if they want 
            # to reference the current table dynamically.
            # Only variable substitution is performed.
            ddl = rendered_sql
        else:
            # Fallback (maybe error?)
            print(f"Unknown materialization '{model.materialized}' for {model.name}, defaulting to view")
            ddl = f"CREATE OR REPLACE VIEW {fqn} AS {rendered_sql}"

        with conn.cursor() as cursor:
            cursor.execute(ddl)

class Model:
    def __init__(self, name, materialized, sql_body, depends_on):
        self.name = name
        self.materialized = materialized
        self.sql = sql_body
        self.depends_on = depends_on

def load_config_from_yaml(path):
    with open(path, 'r') as f:
        return yaml.safe_load(f)
