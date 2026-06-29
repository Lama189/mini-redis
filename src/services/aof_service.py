import os
import io
import asyncio
import time

from src.protocol.parser import parse_resp
from src.domain.entities.entry import Entry
from src.domain.values.string import RedisString
from src.domain.values.hash import RedisHash


class AofService:
    def __init__(self, filepath: str = "appendonly.aof") -> None:
        self._file = None
        self.filepath = filepath
        self.tmp_filepath = filepath + ".tmp"
        self._lock = asyncio.Lock()
        self._rewrite_buffer: list[bytes] = []
        self._is_rewriting = False

        self._buffer = bytearray()
        self._flusher_task: asyncio.Task | None = None

    def _open_file_if_needed(self) -> None:
        if self._file is None or self._file.closed:
            self._file = open(self.filepath, "ab")

    def start(self) -> None:
        if self._flusher_task is None or self._flusher_task.done():
            self._flusher_task = asyncio.create_task(self._flusher_loop())

    async def _flusher_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(1.0)
                await self.flush()
            except asyncio.CancelledError:
                await self.flush()
                break
            except Exception as e:
                print(f"[-] Ошибка во время фонового сброса AOF: {e}")

    async def flush(self) -> None:
        if not self._buffer:
            return

        async with self._lock:
            data_to_write = bytes(self._buffer)
            self._buffer.clear()

        await asyncio.to_thread(self._sync_buffer_to_disk, data_to_write)

    def _sync_buffer_to_disk(self, data: bytes) -> None:
        self._open_file_if_needed()
        if self._file:
            self._file.write(data)
            self._file.flush()
            os.fsync(self._file.fileno())

    async def append(self, raw_resp_bytes: bytes) -> None:
        if not raw_resp_bytes:
            return
        
        async with self._lock:
            if self._is_rewriting:
                self._rewrite_buffer.append(raw_resp_bytes)

            self._buffer.extend(raw_resp_bytes)

    def close(self) -> None:
        if self._flusher_task:
            self._flusher_task.cancel()
        if self._file and not self._file.closed:
            self._file.close()

    def read_all_commands_from_file(self) -> list[list[str]]:
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
    
    def _build_resp_array(self, parts: list[str]) -> bytes:
        res = f"*{len(parts)}\r\n"
        for part in parts:
            res += f"${len(part)}\r\n{part}\r\n"
        return res.encode()
    
    def _serialize_entry_to_resp(self, key: str, entry: Entry) -> bytes | None:
        now = time.time()
        if entry.expire_at is not None:
            if entry.expire_at <= now:
                return None
            remaining_ttl = int(max(1, entry.expire_at - now))
        else:
            remaining_ttl = None

        if isinstance(entry.value, RedisString):
            cmd_parts = ["SET", key, entry.value.value]
            if remaining_ttl is not None:
                cmd_parts.extend(["EX", str(remaining_ttl)])
            return self._build_resp_array(cmd_parts)
        
        if isinstance(entry.value, RedisHash):
            cmd_parts = ["HSET", key]
            hash_data = entry.value.value
            for h_key, h_val in hash_data.items():
                cmd_parts.extend([h_key, h_val])
            if remaining_ttl is not None:
                cmd_parts.extend(["EX", str(remaining_ttl)])
            return self._build_resp_array(cmd_parts)
        
    def _write_snapshot_to_tmp_file(self, snapshot: dict[str, Entry], tmp_filepath: str) -> None:
        f = open(tmp_filepath, "wb")
        try:
            buffer: list[bytes] = []
            for key, entry in snapshot.items():
                resp_bytes = self._serialize_entry_to_resp(key, entry)
                if resp_bytes:
                    buffer.append(resp_bytes)

            if buffer:
                f.write(b"".join(buffer))
            f.flush()
            os.fsync(f.fileno())
        finally:
            f.close()

    def _write_tail_to_tmp_file(self, tmp_filepath: str, tail_data: bytes) -> None:
        with open(tmp_filepath, "ab") as f:
            f.write(tail_data)
            f.flush()
            os.fsync(f.fileno())

    async def background_rewrite(self, snapshot: dict[str, Entry]) -> None:
        if self._is_rewriting:
            print("[*] АОF Rewrite уже запущен!")
            return
        
        print("[*] Старт фонового АОF Rewrite...")
        self._is_rewriting = True
        self._rewrite_buffer.clear()

        try:
            await asyncio.to_thread(self._write_snapshot_to_tmp_file, snapshot, self.tmp_filepath)

            async with self._lock:
                tail_data = b"".join(self._rewrite_buffer)
                if tail_data:
                    await asyncio.to_thread(self._write_tail_to_tmp_file, self.tmp_filepath, tail_data)
                
                if self._file and not self._file.closed:
                    self._file.close()
                    self._file = None
                
                await asyncio.to_thread(os.replace, self.tmp_filepath, self.filepath)
                print("[*] AOF файл успешно компактизирован и заменен!")

        except Exception as e:
            print(f"[-] Ошибка во время AOF Rewrite: {e}")
            if os.path.exists(self.tmp_filepath):
                os.remove(self.tmp_filepath)
        finally:
            self._is_rewriting = False
            self._rewrite_buffer.clear()