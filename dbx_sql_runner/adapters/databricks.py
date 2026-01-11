import time
from typing import Dict, Any, List
from databricks import sql
from .base import BaseAdapter

class DatabricksAdapter(BaseAdapter):
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._ensure_connection_params()

    def _ensure_connection_params(self):
        # Validate config contains required keys
        required = ['server_hostname', 'http_path', 'access_token']
        for req in required:
            if req not in self.config:
                raise ValueError(f"Databricks config missing required key: {req}")

    def execute(self, sql_statement: str) -> None:
        with sql.connect(**self.config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_statement)

    def fetch_result(self, sql_statement: str) -> List[Any]:
        with sql.connect(**self.config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_statement)
                return cursor.fetchall()
    
    def ensure_schema_exists(self, catalog: str, schema: str) -> None:
        self.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    def drop_schema_cascade(self, catalog: str, schema: str) -> None:
        self.execute(f"DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE")

    def get_metadata(self, catalog: str, schema: str) -> Dict[str, Any]:
        self._ensure_metadata_table(catalog, schema)
        meta = {}
        try:
             # Logic from old core.py
            rows = self.fetch_result(
                f"SELECT model_name, sql_hash, materialized, execution_id FROM {catalog}.{schema}._dbx_model_metadata ORDER BY last_executed_at ASC"
            )
            for row in rows:
                meta[row.model_name] = {  # Row objects in databricks-sql usually accessible by attribute or index?
                    # databricks-sql connector returns Row objects which are tuple-like but also named.
                    # Safest is index for now based on query order.
                    "sql_hash": row[1],
                    "materialized": row[2],
                    "execution_id": row[3] if len(row) > 3 else None
                }
        except Exception as e:
            # print(f"Metadata read error (ignoring): {e}") 
            pass
        return meta

    def update_metadata(self, catalog: str, schema: str, model_name: str, sql_hash: str, materialized: str, execution_id: int) -> None:
        sql = f"""
            INSERT INTO {catalog}.{schema}._dbx_model_metadata 
            VALUES ('{model_name}', '{sql_hash}', '{materialized}', current_timestamp(), {execution_id})
        """
        self.execute(sql)

    def get_next_execution_id(self, catalog: str, schema: str) -> int:
        try:
            rows = self.fetch_result(f"SELECT MAX(execution_id) FROM {catalog}.{schema}._dbx_model_metadata")
            if rows and rows[0][0] is not None:
                try:
                    return int(rows[0][0]) + 1
                except ValueError:
                    return 1
            return 1
        except Exception:
            return 1

    def _ensure_metadata_table(self, catalog: str, schema: str):
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {catalog}.{schema}._dbx_model_metadata (
                model_name STRING,
                sql_hash STRING,
                materialized STRING,
                last_executed_at TIMESTAMP,
                execution_id BIGINT
            )
        """
        self.execute(create_sql)
        
        # Schema evolution (dumb)
        try:
            self.execute(f"ALTER TABLE {catalog}.{schema}._dbx_model_metadata ADD COLUMNS (execution_id BIGINT)")
        except Exception:
            pass
