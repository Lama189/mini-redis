from dataclasses import dataclass
from typing import Any

@dataclass
class Entry:
    value: Any
    expire_at: float

