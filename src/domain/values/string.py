from dataclasses import dataclass

from .base import RedisValue


@dataclass(slots=True)
class RedisString(RedisValue):
    value: str