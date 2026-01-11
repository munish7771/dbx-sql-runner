from abc import ABC, abstractmethod
from typing import Dict, Any, List

class BaseAdapter(ABC):
    @abstractmethod
    def execute(self, sql: str) -> None:
        pass

    @abstractmethod
    def fetch_result(self, sql: str) -> List[Any]:
        pass

    @abstractmethod
    def get_metadata(self, catalog: str, schema: str) -> Dict[str, Any]:
        """Returns valid metadata for models in the schema."""
        pass

    @abstractmethod
    def update_metadata(self, catalog: str, schema: str, model_name: str, sql_hash: str, materialized: str, execution_id: int) -> None:
        pass

    @abstractmethod
    def ensure_schema_exists(self, catalog: str, schema: str) -> None:
        pass

    @abstractmethod
    def drop_schema_cascade(self, catalog: str, schema: str) -> None:
        pass
        
    @abstractmethod
    def get_next_execution_id(self, catalog: str, schema: str) -> int:
        pass
