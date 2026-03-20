import os
import re
import networkx as nx
from typing import List
from .models import Model
from .exceptions import DbxModelLoadingError, DbxDependencyError


class ProjectLoader:
    def __init__(self, model_paths: List[str]):
        """
        :param model_paths: List of directories containing SQL model files,
                            sourced from the ``model_paths`` key in profiles.yml.
                            Each entry must be a valid directory path (e.g. "models/").
                            All .sql files inside each directory will be loaded.
        """
        if not model_paths:
            raise DbxModelLoadingError(
                "'model_paths' must be set in profiles.yml and contain at least one directory."
            )
        self.model_paths = model_paths

    def _resolve_sql_files(self) -> List[str]:
        """Return a deduplicated, ordered list of absolute .sql file paths."""
        seen = set()
        files = []
        for directory in self.model_paths:
            directory = os.path.normpath(directory)
            if not os.path.isdir(directory):
                raise DbxModelLoadingError(
                    f"model_paths entry is not a valid directory: '{directory}'"
                )
            for f in sorted(os.listdir(directory)):
                if f.endswith(".sql"):
                    abs_path = os.path.abspath(os.path.join(directory, f))
                    if abs_path not in seen:
                        seen.add(abs_path)
                        files.append(abs_path)

        if not files:
            raise DbxModelLoadingError(
                f"No SQL files found in model_paths directories: {self.model_paths}"
            )
        return files

    def load_models(self) -> List[Model]:
        return [self._parse_model_file(path) for path in self._resolve_sql_files()]

    def _parse_model_file(self, path: str) -> Model:
        with open(path, "r") as f:
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
                    meta["depends_on"] = [
                        d.strip() for d in deps.split(",") if d.strip()
                    ]
                elif "partition_by:" in line:
                    parts = line.split("partition_by:")[1].strip()
                    meta["partition_by"] = [
                        p.strip() for p in parts.split(",") if p.strip()
                    ]
            else:
                sql_lines.append(line)

        sql_body = "".join(sql_lines)

        # Inference: Find all {variable} patterns and add them as dependencies
        inferred_deps = re.findall(r"\{(\w+)\}", sql_body)
        for dep in inferred_deps:
            if dep not in meta["depends_on"]:
                meta["depends_on"].append(dep)

        return Model(
            name=meta.get("name", os.path.basename(path).replace(".sql", "")),
            materialized=meta.get(
                "materialized", "view"
            ),  # Default to View? Or config default?
            sql=sql_body,
            depends_on=meta.get("depends_on", []),
            partition_by=meta.get("partition_by", []),
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
            raise DbxDependencyError("Cyclic dependency detected in models")

        model_map = {m.name: m for m in self.models}
        return [model_map[name] for name in sorted_names]
