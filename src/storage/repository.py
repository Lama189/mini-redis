import time
import random

from src.domain.entities.entry import Entry
from src.domain.values.hash import RedisHash
from src.domain.exceptions import WrongTypeException
from src.interfaces.repository import IEntryRepository


class RedisRepository(IEntryRepository):
    def __init__(self):
        self._storage: dict[str, Entry] = {}

    async def set(self, key: str, entry: Entry) -> None:
        self._storage[key] = entry
        
    async def get(self, key: str) -> Entry | None:
        if key not in self._storage:
            return None
        
        return self._storage[key]
    
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
        
        if not isinstance(entry.value, RedisHash):
            raise WrongTypeException()

        try:
            actual_dict = entry.value.value
            return actual_dict.get(field)
        except AttributeError:
            return None
        
    async def hdel(self, key: str, fields: list[str] | None) -> int:
        entry = await self.get(key)
        if entry is None:
            return 0
        
        if not isinstance(entry.value, RedisHash):
            raise WrongTypeException()
        
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

    async def hexists(self, key: str, field: str) -> int:
        value = await self.hget(key, field)
        if not isinstance(value, RedisHash):
            raise WrongTypeException()

        if value is None:
            return 0
        return 1
    
    async def hlen(self, key: str) -> int:
        entry = await self.get(key)
        if entry is None:
            return 0
        
        if not isinstance(entry.value, RedisHash):
            raise WrongTypeException()
        
        try:
            actual_dict = entry.value.value
        except AttributeError:
            return 0
        
        return len(actual_dict)
    
    async def hgetall(self, key: str) -> dict:
        entry = await self.get(key)
        if entry is None:
            return {}
        
        if not isinstance(entry.value, RedisHash):
            raise WrongTypeException()
        
        try:
            actual_dict = entry.value.value
        except AttributeError:
            return {}
        
        return actual_dict
    
    async def expire_active_step(self) -> None:
        current_time = time.time()

        ttl_keys = [k for k, entry in self._storage.items() if entry.expire_at is not None]
        if not ttl_keys:
            return
        
        while True:
            sample_size = min(20, len(ttl_keys))
            sampled_keys = random.sample(ttl_keys, sample_size)

            expired_count = 0
            for key in sampled_keys:
                entry = self._storage.get(key)
                if entry and entry.expire_at is not None and current_time > entry.expire_at:
                    self._storage.pop(key, None)
                    ttl_keys.remove(key) 
                    expired_count += 1

            if sample_size == 0 or (expired_count / sample_size) <= 0.25:
                break

