from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class Model:
    name: str
    materialized: str
    sql: str
    depends_on: List[str] = field(default_factory=list)
    partition_by: List[str] = field(default_factory=list)
    execution_result: Optional[str] = None # 'EXECUTE', 'SKIP', 'FAIL'
    sql_hash: Optional[str] = None
