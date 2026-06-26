from abc import ABC, abstractmethod

from src.domain.entry import Entry


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