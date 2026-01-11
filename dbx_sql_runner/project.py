import os
import re
import networkx as nx
from typing import List, Dict
from .models import Model

class ProjectLoader:
    def __init__(self, models_dir: str):
        self.models_dir = models_dir

    def load_models(self) -> List[Model]:
        models = []
        if not os.path.exists(self.models_dir):
            raise ValueError(f"Models directory not found: {self.models_dir}")
            
        for f in os.listdir(self.models_dir):
            if f.endswith(".sql"):
                models.append(self._parse_model_file(os.path.join(self.models_dir, f)))
        return models

    def _parse_model_file(self, path: str) -> Model:
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
        inferred_deps = re.findall(r"\{(\w+)\}", sql_body)
        for dep in inferred_deps:
            if dep not in meta["depends_on"]:
                meta["depends_on"].append(dep)

        return Model(
            name=meta.get("name", os.path.basename(path).replace(".sql", "")),
            materialized=meta.get("materialized", "view"), # Default to View? Or config default?
            sql=sql_body,
            depends_on=meta.get("depends_on", []),
            partition_by=meta.get("partition_by", [])
        )

class DependencyGraph:
    def __init__(self, models: List[Model]):
        self.models = models
        self.dag = self._build_dag()

    def _build_dag(self) -> nx.DiGraph:
        dag = nx.DiGraph()
        model_map = {m.name: m for m in self.models}
        
        for m in self.models:
            dag.add_node(m.name, model=m)
            for dep in m.depends_on:
                if dep in model_map:
                    dag.add_edge(dep, m.name)
        return dag

    def get_execution_order(self) -> List[Model]:
        try:
            sorted_names = list(nx.topological_sort(self.dag))
        except nx.NetworkXUnfeasible:
            raise ValueError("Cyclic dependency detected in models")
        
        model_map = {m.name: m for m in self.models}
        return [model_map[name] for name in sorted_names]
