import os
import networkx as nx
import yaml
from databricks import sql

class DbxRunnerProject:
    def __init__(self, models_dir, config):
        self.models_dir = models_dir
        self.config = config
        self.models = self._load_models()
        self.dag = self._build_dag()

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
        
        return Model(
            name=meta.get("name", os.path.basename(path).replace(".sql", "")),
            materialized=meta.get("materialized", "view"),
            sql_body=''.join(sql_lines),
            depends_on=meta.get("depends_on", [])
        )

    def _build_dag(self):
        dag = nx.DiGraph()
        model_map = {m.name: m for m in self.models}
        
        for m in self.models:
            dag.add_node(m.name, model=m)
            for dep in m.depends_on:
                if dep not in model_map:
                    print(f"Warning: Dependency {dep} not found for model {m.name}")
                    continue
                dag.add_edge(dep, m.name)
        return dag

    def run(self):
        # Topological sort
        try:
            sorted_models = list(nx.topological_sort(self.dag))
        except nx.NetworkXUnfeasible:
            raise ValueError("Cyclic dependency detected in models")

        model_map = {m.name: m for m in self.models}
        
        print(f"Connecting to Databricks host: {self.config.get('server_hostname')}")
        with sql.connect(**self.config) as conn:
            for name in sorted_models:
                model = model_map[name]
                print(f"Running {model.name}...")
                self._execute_model(model, conn)
        print("All models executed successfully.")

    def _execute_model(self, model, conn):
        ddl = f"CREATE OR REPLACE {'VIEW' if model.materialized == 'view' else 'TABLE'} {model.name} AS {model.sql}"
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
