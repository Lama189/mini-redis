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

        return final_id