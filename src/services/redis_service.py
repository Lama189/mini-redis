import time
import asyncio

from src.services.aof_service import AofService
from src.services.wait_manager import WaitManager

from src.domain.entities.entry import Entry
from src.domain.entities.session import ClientSession
from src.domain.values.string import RedisString
from src.domain.values.hash import RedisHash
from src.domain.values.list import RedisList
from src.domain.values.stream import RedisStream

from src.domain.exceptions import WrongTypeException
from src.interfaces.repository import IEntryRepository


class RedisService:
    def __init__(
        self, 
        repository: IEntryRepository, 
        wait_manager: WaitManager,
        aof: AofService | None = None
    ) -> None:
        self._repo = repository
        self.aof = aof
        self.wait_manager = wait_manager


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
    

    async def incr(self, key: str, raw_data: bytes) -> int:
        entry = await self._repo.get(key)
        if entry is None:
            return 0

        if entry.value is None:
            new_value = 1
        else:
            try:
                raw_str_value = entry.value.value if hasattr(entry.value, 'value') else entry.value
                new_value = int(raw_str_value) + 1
            except ValueError:
                raise ValueError("ERR value is not an integer or out of range")
        
        if hasattr(entry.value, 'value'):
            entry.value.value = str(new_value)
        else:
            entry.value = str(new_value)
            
        await self._repo.set(key, entry)
        if self.aof and raw_data:
            await self.aof.append(raw_data)

        return new_value
    

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


    async def lpush(self, key: str, items: list[str], raw_data: bytes, ttl: int | None = None) -> int:
        while items:
            waiting_session = self.wait_manager.pop_waiter(key)
            if waiting_session is None:
                break

            waiting_session.blocking_result = items.pop(0) 

            if waiting_session.blocking_event is not None:
                waiting_session.blocking_event.set()

            if self.aof and raw_data:
                await self.aof.append(raw_data)

                if not items:
                    entry = await self._repo.get(key)
                    return len(entry.value) if entry and isinstance(entry.value, RedisList) else 0
        
        entry = await self._repo.get(key)
        expire_at = time.time() + ttl if ttl is not None else None

        if entry is None:
            redis_list = RedisList()
            redis_list.lpush(items)

            entry = Entry(value=redis_list, expire_at=expire_at)
            await self._repo.set(key, entry)

            if self.aof and raw_data:
                await self.aof.append(raw_data)
            return len(redis_list)

        if isinstance(entry.value, RedisString):
            redis_list = RedisList()
            redis_list.rpush([entry.value.value])
            redis_list.lpush(items)

            entry = Entry(value=redis_list, expire_at=expire_at)
            await self._repo.set(key, entry)

            if self.aof and raw_data:
                await self.aof.append(raw_data)
            return len(redis_list)

        if not isinstance(entry.value, RedisList):
            raise WrongTypeException()

        length = entry.value.lpush(items)

        if expire_at is not None:
            entry.expire_at = expire_at

        await self._repo.set(key, entry)

        if self.aof and raw_data:
            await self.aof.append(raw_data)

        return length


    async def rpush(self, key: str, items: list[str], raw_data: bytes, ttl: int | None = None) -> int:
        while items:
            waiting_session = self.wait_manager.pop_waiter(key)
            if waiting_session is None:
                break

            waiting_session.blocking_result = items.pop(0) 

            if waiting_session.blocking_event is not None:
                waiting_session.blocking_event.set()
                
            if self.aof and raw_data:
                await self.aof.append(raw_data)

                if not items:
                    entry = await self._repo.get(key)
                    return len(entry.value) if entry and isinstance(entry.value, RedisList) else 0
        
        entry = await self._repo.get(key)
        expire_at = time.time() + ttl if ttl is not None else None

        if entry is None:
            redis_list = RedisList()
            redis_list.rpush(items)

            entry = Entry(value=redis_list, expire_at=expire_at)
            await self._repo.set(key, entry)

            if self.aof and raw_data:
                await self.aof.append(raw_data)
            return len(redis_list)

        if isinstance(entry.value, RedisString):
            redis_list = RedisList()
            redis_list.rpush([entry.value.value])
            redis_list.rpush(items)

            entry = Entry(value=redis_list, expire_at=expire_at)
            await self._repo.set(key, entry)

            if self.aof and raw_data:
                await self.aof.append(raw_data)
            return len(redis_list)

        if not isinstance(entry.value, RedisList):
            raise WrongTypeException()

        length = entry.value.rpush(items)

        if expire_at is not None:
            entry.expire_at = expire_at

        await self._repo.set(key, entry)

        if self.aof and raw_data:
            await self.aof.append(raw_data)

        return length
    

    async def lpop(self, key: str, raw_data: bytes) -> str | None:
        entry = await self._repo.get(key)
        if entry is None:
            return None
        
        if not isinstance(entry.value, RedisList):
            raise WrongTypeException()
        
        item = entry.value.lpop()

        if len(entry.value) == 0:
            await self._repo.delete([key])
        else:
            await self._repo.set(key, entry)

        if self.aof and raw_data:
            await self.aof.append(raw_data) 
        return item    
    

    async def rpop(self, key: str, raw_data: bytes) -> str | None:
        entry = await self._repo.get(key)
        if entry is None:
            return None
        
        if not isinstance(entry.value, RedisList):
            raise WrongTypeException()
        
        item = entry.value.rpop()

        if len(entry.value) == 0:
            await self._repo.delete([key])
        else:
            await self._repo.set(key, entry)

        if self.aof and raw_data:
            await self.aof.append(raw_data) 

        return item  


    async def blpop(
        self, 
        key: str, 
        timeout: int, 
        session: ClientSession,
        raw_data: bytes
    ) -> str | None:
        if session is None:
            raise ValueError("BLPOP требует контекст сессии")

        fast_item = await self.lpop(key, raw_data)
        if fast_item is not None:
            return fast_item
        
        event = asyncio.Event()
        session.blocking_event = event
        session.blocking_result = None

        self.wait_manager.add_waiter(key, session)

        try:
            if timeout == 0:
                await event.wait()
            else:
                try:
                    await asyncio.wait_for(event.wait(), timeout=float(timeout))
                except asyncio.TimeoutError:
                    self.wait_manager.remove_waiter(key, session)
                    return None
            
            return session.blocking_result
        
        finally:
            session.blocking_event = None
            session.blocking_result = None

    
    async def brpop(
        self, 
        key: str, 
        timeout: int, 
        session: ClientSession,
        raw_data: bytes
    ) -> str | None:
        if session is None:
            raise ValueError("BRPOP требует контекст сессии")
        
        fast_item = await self.rpop(key, raw_data)
        if fast_item is not None:
            return fast_item
        
        event = asyncio.Event()
        session.blocking_event = event
        session.blocking_result = None

        self.wait_manager.add_waiter(key, session)

        try:
            if timeout == 0:
                await event.wait()
            else:
                try:
                    await asyncio.wait_for(event.wait(), timeout=float(timeout))
                except asyncio.TimeoutError:
                    self.wait_manager.remove_waiter(key, session)
                    return None
            
            return session.blocking_result
        
        finally:
            session.blocking_event = None
            session.blocking_result = None

    
    async def xadd(
        self,
        key: str,
        id_str: str,
        fields: dict[str, str],
        raw_data: bytes
    ) -> str:
        entry = await self._repo.get(key)

        if entry is None:
            stream = RedisStream()
            try:
                final_id = stream.add(id_str, fields)
            except ValueError as e:
                return str(e)
            
            entry = Entry(value=stream, expire_at=None)
            await self._repo.set(key, entry)
        
        elif isinstance(entry.value, RedisStream):
            try:
                final_id = entry.value.add(id_str, fields)
            except ValueError as e:
                return str(e)
            
            await self._repo.set(key, entry)
        
        else:
            raise WrongTypeException()
        
        if self.aof and raw_data:
            await self.aof.append(raw_data) 

        waiters = self.wait_manager.get_and_clear_stream_waiters(key)
        if waiters:
            new_id_cmp = entry.value.parse_id(final_id)

            for waiting_session, after_id_str in waiters:
                if after_id_str == "$":
                    should_wake = True
                else:
                    client_id_cmp = entry.value.parse_id(after_id_str)
                    should_wake = new_id_cmp > client_id_cmp
                
                if should_wake:
                    waiting_session.blocking_result = [(final_id, fields)]
                    if waiting_session.blocking_event is not None:
                        waiting_session.blocking_event.set()
                else:
                    self.wait_manager.add_stream_waiter(key, waiting_session, after_id_str)

        return final_id
    

    async def xrange(self, key: str, start_str: str, end_str: str) -> list[tuple[str, dict[str, str]]] | None:
        entry = await self._repo.get(key)
        if entry is None:
            return None
        
        if not isinstance(entry.value, RedisStream):
            raise WrongTypeException()
        
        stream_data = entry.value.value
        if not stream_data:
            return []
        
        def to_cmp_tuple(id_str: str) -> tuple[int, int]:
            if id_str == "-":
                return (0, 0)
            if id_str == "+":
                return (9223372036854775807, 9223372036854775807)
            
            return entry.value.parse_id(id_str)
        
        start_cmp = to_cmp_tuple(start_str)
        end_cmp = to_cmp_tuple(end_str)

        result = []
        for item_id, fields in stream_data.items():
            item_cmp = entry.value.parse_id(item_id)

            if start_cmp <= item_cmp <= end_cmp:
                result.append((item_id, fields))
        
        return result
    

    async def xread_sync(self, key: str, after_id_str: str) -> list[tuple[str, dict[str, str]]] | None:
        entry = await self._repo.get(key)
        if entry is None:
            return None
        
        if not isinstance(entry.value, RedisStream):
            raise WrongTypeException()
        
        stream_data = entry.value.value
        if not stream_data:
            return []
        
        if after_id_str == "$":
            target_cmp = entry.value.parse_id(entry.value.last_id_str)
        else:
            target_cmp = entry.value.parse_id(after_id_str)

        result = []
        for item_id, fields in stream_data.items():
            item_cmp = entry.value.parse_id(item_id)

            if item_cmp > target_cmp:
                result.append((item_id, fields))

        return result
    

    async def xread(
        self,
        key: str,
        after_id_str: str,
        timeout_ms: int,
        session: ClientSession
    ) -> list[tuple[str, dict[str, str]]] | None:
        if after_id_str != "$":
            fast_items = await self.xread_sync(key, after_id_str)
            if fast_items:
                return fast_items
            
        if timeout_ms is None:
            return []
        
        event = asyncio.Event()
        session.blocking_event = event
        session.blocking_result = None

        actual_after_id = after_id_str
        if after_id_str == "$":
            entry = await self._repo.get(key)
           
            if entry and isinstance(entry.value, RedisStream):
                actual_after_id = entry.value.last_id_str
            else:
                actual_after_id = "0-0"

        self.wait_manager.add_stream_waiter(key, session, actual_after_id)

        try:
            if timeout_ms == 0:
                await event.wait()
            else:
                timeout_sec = float(timeout_ms) / 1000.0
                try:
                    await asyncio.wait_for(event.wait(), timeout=timeout_sec)
                except asyncio.TimeoutError:
                    self.wait_manager.remove_stream_waiter(key, session)
                    return []
            
            if session.blocking_result and isinstance(session.blocking_result, list):
                return session.blocking_result
            return []
        
        finally:
            session.blocking_event = None
            session.blocking_result = None