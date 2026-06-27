import time
from typing import Any

from src.domain.entities.entry import Entry
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
    
    async def hget(self, key: str, field: str) -> str | None:
        entry = await self.get(key)
        if entry is None:
            return None

        try:
            actual_dict = entry.value.value
            return actual_dict.get(field)
        except AttributeError:
            return None
        
    async def hdel(self, key: str, fields: list[str] | None) -> int:
        entry = await self.get(key)
        if entry is None:
            return 0
        
        try:
            actual_dict = entry.value.value
        except AttributeError:
            return 0
        
        if fields is None or len(fields) == 0:
            del self._storage[key]
            return len(actual_dict)

        deleted_count = 0
        for field in fields:
            if field in actual_dict:
                del actual_dict[field]
                deleted_count += 1
        
        if len(actual_dict) == 0:
            del self._storage[key]

        return deleted_count

