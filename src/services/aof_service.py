import os
import io
import asyncio

from src.protocol.parser import parse_resp


class AofService:
    def __init__(self, filepath: str = "appendonly.aof") -> None:
        self.filepath = filepath
        self._file = None
        self._lock = asyncio.Lock()

    def _open_file_if_needed(self) -> None:
        if self._file is None or self._file.closed:
            self._file = open(self.filepath, "ab", buffering=0)

    def _write_and_sync(self, raw_resp_bytes: bytes) -> None:
        self._open_file_if_needed()

        if self._file:
            self._file.write(raw_resp_bytes)
            os.fsync(self._file.fileno())

    async def append(self, raw_resp_bytes: bytes) -> None:
        async with self._lock:
            await asyncio.to_thread(self._write_and_sync, raw_resp_bytes)

    def close(self) -> None:
        if self._file and not self._file.closed:
            self._file.close()

    def read_aff_commands_from_file(self) -> list[list[str]]:
        if not os.path.exists(self.filepath):
            return []
        
        commands = []
        with open(self.filepath, "rb") as f:
            content = f.read()
            buffer = io.BytesIO(content)

            while buffer.tell() < len(content):
                try: 
                    cmd_parts = parse_resp(buffer)
                    if cmd_parts:
                        commands.append(cmd_parts)
                except Exception as e:
                    print(f"[*] Чтение AOF завершено или достигнут конец файла: {e}")
                    break
        
        return commands