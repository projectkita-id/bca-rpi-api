from pydantic import BaseModel
from typing import List, Optional

class StartBatchRequest(BaseModel):
    scanner_used: List[int]
    batch_code: Optional[str] = None
