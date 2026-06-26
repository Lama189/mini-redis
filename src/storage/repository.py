import time
from typing import Any

from src.domain.entry import Entry
from src.interfaces.repository import IEntryRepository


class RedisRepository(IEntryRepository):
    def __init__(self):
        self._storage: dict[str, Entry] = {}

    async def set(self, key: str, entry: Entry) -> None:
        self._storage[key] = entry
        
    async def get(self, key: str) -> Entry | None:
        if key not in self._storage:
            return None
        
        item = self._storage[key]

        if item.expire_at is not None and time.time() > item.expire_at:
            del self._storage[key]
            return None

        return item
    
    async def delete(self, keys: list[str]) -> int:
        deleted_count = 0

        for key in keys:
            if await self.get(key) is not None:
                del self._storage[key]
                deleted_count += 1
            elif key in self._storage:
                self._storage.pop(key, None)

        return deleted_count
