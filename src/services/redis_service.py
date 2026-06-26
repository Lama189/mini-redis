import time

from src.domain.entry import Entry
from src.interfaces.repository import IEntryRepository


class RedisService:
    def __init__(self, repository: IEntryRepository) -> None:
        self._repo = repository

    async def set(self, key: str, value: str, ttl: int | None = None) -> None:
        expire_at = None

        if ttl is not None:
            expire_at = time.time() + ttl

        entry = Entry(
            value=str(value),
            expire_at=expire_at
        )

        await self._repo.set(key, entry)

    async def get(self, key: str) -> str | None:
        entry = await self._repo.get(key)

        if entry is None:
            return None
        
        return entry.value
    
    async def delete(self, keys: list[str]) -> int:
        return await self._repo.delete(keys)