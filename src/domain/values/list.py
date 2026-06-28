from collections import deque
from typing import Any


class RedisList:
    def __init__(self, items: list[str] | None = None) -> None:
        self._value: deque[str] = deque(items) if items else deque()

    @property
    def value(self) -> deque[str]:
        return self._value
    
    def lpush(self, items: list[str]) -> int:
        for item in items:
            self._value.appendleft(item)
        return len(self._value)
    
    def rpush(self, items: list[str]) -> int:
        for item in items:
            self._value.append(item)
        return len(items)
    
    def lpop(self) -> str | None:
        if not self._value:
            return None
        return self._value.popleft()
    
    def rpop(self) -> str | None:
        if not self._value:
            return None
        return self._value.pop()
    
    def __len__(self) -> int:
        return len(self._value)