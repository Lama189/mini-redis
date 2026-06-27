import time

from src.domain.entities.entry import Entry
from src.interfaces.repository import IEntryRepository
from src.domain.values.string import RedisString
from src.domain.values.hash import RedisHash


class RedisService:
    def __init__(self, repository: IEntryRepository) -> None:
        self._repo = repository

    async def set(self, key: str, value: str, ttl: int | None = None) -> None:
        expire_at = None

        if ttl is not None:
            expire_at = time.time() + ttl

        entry = Entry(
            value=RedisString(value),
            expire_at=expire_at
        )

        await self._repo.set(key, entry)

    async def get(self, key: str) -> str | None:
        entry = await self._repo.get(key)

        if entry is None:
            return None
        
        return entry.value.value
    
    async def delete(self, keys: list[str]) -> int:
        return await self._repo.delete(keys)
    
    async def hset(self, key: str, fields: dict, ttl: int | None = None) -> int:
        expire_at = None

        if ttl is not None:
            expire_at = time.time() + ttl

        entry = Entry(
            value=RedisHash(fields),
            expire_at=expire_at
        )

        await self._repo.set(key, entry)
        return len(fields)

    async def hget(self, key: str, field: str) -> str | None:
        value = await self._repo.hget(key, field)

        if value is None:
            return None
        
        return value
    
    async def hdel(self, key: str, fields: list[str] | None) -> int:
        return await self._repo.hdel(key, fields)
    
    async def hexists(self, key: str, field: str) -> int:
        return await self._repo.hexists(key, field)
    
    async def hlen(self, key: str) -> int:
        return await self._repo.hlen(key)
    
    async def hgetall(self, key: str) -> list:
        data_dict = await self._repo.hgetall(key)

        flat_list = []
        for k, v in data_dict.items():
            flat_list.append(str(k))
            flat_list.append(str(v))

        return flat_list
    
    async def hkeys(self, key: str) -> list:
        data_dict = await self._repo.hgetall(key)

        flat_list = []
        for k, v in data_dict.items():
            flat_list.append(str(k))

        return flat_list

    async def hvals(self, key: str) -> list:
        data_dict = await self._repo.hgetall(key)

        flat_list = []
        for k, v in data_dict.items():
            flat_list.append(str(v))

        return flat_list