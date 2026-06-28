import time
import asyncio

from src.services.aof_service import AofService
from src.interfaces.repository import IEntryRepository
from src.domain.entities.entry import Entry
from src.domain.values.string import RedisString
from src.domain.values.hash import RedisHash


class RedisService:
    def __init__(self, repository: IEntryRepository, aof: AofService | None = None) -> None:
        self._repo = repository
        self.aof = aof

    async def set(
        self, 
        key: str, 
        value: str, 
        raw_data: bytes,
        ttl: int | None = None
    ) -> None:
        expire_at = None

        if ttl is not None:
            expire_at = time.time() + ttl

        entry = Entry(
            value=RedisString(value),
            expire_at=expire_at
        )

        await self._repo.set(key, entry)
        if self.aof and raw_data:
            await self.aof.append(raw_data)

    async def get(self, key: str) -> str | None:
        entry = await self._repo.get(key)

        if entry is None:
            return None
        
        return entry.value.value
    
    async def delete(self, keys: list[str], raw_data: bytes) -> int:
        count = await self._repo.delete(keys)
        if count > 0 and self.aof and raw_data:
            await self.aof.append(raw_data)
        return count
    
    async def hset(
        self, 
        key: str, 
        fields: dict, 
        raw_data: bytes,
        ttl: int | None = None
    ) -> int:
        expire_at = None

        if ttl is not None:
            expire_at = time.time() + ttl

        entry = Entry(
            value=RedisHash(fields),
            expire_at=expire_at
        )

        await self._repo.set(key, entry)
        if self.aof and raw_data:
            await self.aof.append(raw_data)

        return len(fields)

    async def hget(self, key: str, field: str) -> str | None:
        value = await self._repo.hget(key, field)

        if value is None:
            return None
        
        return value
    
    async def hdel(self, key: str, fields: list[str] | None, raw_data: bytes) -> int:
        count = await self._repo.hdel(key, fields)
        if count > 0 and self.aof and raw_data:
            await self.aof.append(raw_data)
        return count
    
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
    
    async def start_aof_rewrite(self) -> None:
        if self.aof is not None:
            snapshot = self._repo.get_all_entries()
            asyncio.create_task(self.aof.background_rewrite(snapshot))