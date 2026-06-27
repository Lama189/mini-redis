from dataclasses import dataclass

from .base import RedisValue


@dataclass(slots=True)
class RedisHash(RedisValue):
    value: dict[str, str]