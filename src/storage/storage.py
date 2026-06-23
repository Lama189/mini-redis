import time
from typing import Any


class Storage:
    def __init__(self):
        self._storage: dict[str, dict[str, Any]] = {}

    async def set(self, key: str, value: str) -> None:
        self._storage[key] = {
            "value": value
        }

    async def get(self, key: str) -> str | None:
        if key not in self._storage:
            return None
        
        item = self._storage[key]
        return item["value"]
    
    async def delete(self, keys: list[str]) -> int:
        deleted_count = 0

        for key in keys:
            if await self.get(key) is not None:
                del self._storage[key]
                deleted_count += 1
            elif key in self._storage:
                self._storage.pop(key, None)

        return deleted_count
