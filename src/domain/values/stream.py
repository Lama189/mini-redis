import time
from typing import Any


class RedisStream:
    def __init__(self, entries: dict[str, dict[str, str]] | None = None) -> None:
        self._value: dict[str, dict[str, str]] = entries if entries else {}
        self._last_id_str: str = list(self._value.keys())[-1] if self._value else "0-0"

    @property
    def value(self) -> dict[str, dict[str, str]]:
        return self._value

    @property
    def last_id_str(self) -> str:
        return self._last_id_str

    def parse_id(self, id_str) -> tuple[int, int]:
        parts = id_str.split("-")
        if len(parts) != 2:
            raise ValueError("-ERR Invalid stream ID format\r\n")
        try:
            return int(parts[0]), int(parts[1])
        except ValueError:
            raise ValueError("-ERR Invalid stream ID format\r\n")
        
    def _generate_id(self) -> str:
        now_ms = int(time.time() * 1000)
        last_ms, last_seq = self.parse_id(self.last_id_str)

        if now_ms > last_ms:
            return f"{now_ms}-0"
        elif now_ms == last_ms:
            return f"{now_ms}-{last_seq + 1}"
        else:
            return f"{last_ms}-{last_seq + 1}"
        
    def add(self, id_str: str, fields: dict[str, str]) -> str:
        if id_str == "*":
            final_id = self._generate_id()
        else:
            new_ms, new_seq = self.parse_id(id_str)
            last_ms, last_seq = self.parse_id(self.last_id_str)

            if (new_ms < last_ms) or (new_ms == last_ms and new_seq <= last_seq):
                raise ValueError("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
            
            if new_ms == 0 and new_seq == 0:
                raise ValueError("-ERR The ID specified in XADD must be greater than 0-0\r\n")
            
            final_id = id_str

        self._value[final_id] = fields
        self._last_id_str = final_id
        return final_id
    
    def __len__(self) -> int:
        return len(self._value)