from abc import ABC, abstractmethod

from src.domain.entities.entry import Entry


class IEntryRepository(ABC):
    @abstractmethod
    async def get(self, key: str) -> Entry | None:
        raise NotImplementedError
    
    @abstractmethod
    async def set(self, key: str, entry: Entry) -> None:
        raise NotImplementedError
    
    @abstractmethod
    async def delete(self, keys: list[str]) -> int:
        raise NotImplementedError
    
    @abstractmethod
    async def hget(self, key: str, field: str) -> str | None:
        raise NotImplementedError
    
    @abstractmethod
    async def hdel(self, key: str, fields: list[str] | None) -> int:
        raise NotImplementedError
    
    @abstractmethod
    async def hexists(self, key: str, field: str) -> int:
        raise NotImplementedError
    
    @abstractmethod
    async def hlen(self, key: str) -> int:
        raise NotImplementedError
    
    @abstractmethod
    async def hgetall(self, key: str) -> dict:
        raise NotImplementedError