import asyncio
from typing import Union, Tuple

from src.services.redis_service import RedisService
from src.services.pubsub_service import PubSubService
from src.domain.exceptions import WrongTypeException
from src.domain.entities.session import ClientSession


async def dispatch_command(
    payload: list[str],
    redis_service: RedisService,
    pubsub_service: PubSubService,
    raw_data: bytes,
    disable_aof: bool = False,
    session: ClientSession | None = None,
) -> Union[str, Tuple[str, asyncio.Queue, str]]:
    command = payload[0].upper()
    args = payload[1:]

    try:
        match command, args:
            case "SET", [key, value]:
                await redis_service.set(key, value, raw_data)
                return "+OK\r\n"

            case "SET", [key, value, "EX", ttl_str]:
                try:
                    ttl = int(ttl_str)
                except ValueError:
                    return "-ERR value is not an integer or out of range\r\n"

                await redis_service.set(key, value, raw_data, ttl)
                return "+OK\r\n"

            case "GET", [key]:
                value = await redis_service.get(key)

                if value is None:
                    return "$-1\r\n"

                encoded_value = value.encode('utf-8')
                return f"${len(encoded_value)}\r\n{value}\r\n"

            case "DEL", [*keys] if keys:
                deleted = await redis_service.delete(keys, raw_data)
                return f":{deleted}\r\n"
            
            case "INCR", [key]:
                try: 
                    new_val = await redis_service.incr(key, raw_data)
                    return f":{new_val}\r\n"
                except ValueError as e:
                    return f"-{str(e)}\r\n"
            
            case "LPUSH", [key, *items] if items:
                length = await redis_service.lpush(key, items, raw_data)
                return f":{length}\r\n"

            case "RPUSH", [key, *items] if items:
                length = await redis_service.rpush(key, items, raw_data)
                return f":{length}\r\n"

            case "LPOP", [key]:
                value = await redis_service.lpop(key, raw_data)
                if value is None:
                    return "$-1\r\n"
                encoded_value = value.encode('utf-8')
                return f"${len(encoded_value)}\r\n{value}\r\n"

            case "RPOP", [key]:
                value = await redis_service.rpop(key, raw_data)
                if value is None:
                    return "$-1\r\n"
                encoded_value = value.encode('utf-8')
                return f"${len(encoded_value)}\r\n{value}\r\n"
            
            case "HSET", [key, *field_values, "EX", ttl_str] if len(field_values) >= 2 and len(field_values) % 2 == 0:
                try:
                    ttl = int(ttl_str)
                except ValueError:
                    return "-ERR value is not an integer or out of range\r\n"
                
                it = iter(field_values)
                fields = dict(zip(it, it))

                added = await redis_service.hset(key, fields, raw_data, ttl)
                return f":{added}\r\n"
            
            case "HGET", [key, field]:
                value = await redis_service.hget(key, field)

                if value is None:
                    return "$-1\r\n"

                encoded_value = value.encode('utf-8')
                return f"${len(encoded_value)}\r\n{value}\r\n"
            
            case "HDEL", [key, *fields]:
                deleted = await redis_service.hdel(key, fields, raw_data)
                return f":{deleted}\r\n"
            
            case "HEXISTS", [key, field]:
                value = await redis_service.hexists(key, field)
                return f":{value}\r\n"
            
            case "HLEN", [key]:
                count = await redis_service.hlen(key)
                return f":{count}\r\n"
            
            case "HGETALL", [key]:
                flat_list = await redis_service.hgetall(key)

                response_parts = [f"*{len(flat_list)}\r\n"]
                for item in flat_list:
                    encoded_item = item.encode('utf-8')
                    response_parts.append(f"${len(encoded_item)}\r\n{item}\r\n")
                    
                return "".join(response_parts)
            
            case "HKEYS", [key]:
                flat_list = await redis_service.hkeys(key)

                response_parts = [f"*{len(flat_list)}\r\n"]
                for item in flat_list:
                    encoded_item = item.encode('utf-8')
                    response_parts.append(f"${len(encoded_item)}\r\n{item}\r\n")
                    
                return "".join(response_parts)
            
            case "HVALS", [key]:
                flat_list = await redis_service.hvals(key)

                response_parts = [f"*{len(flat_list)}\r\n"]
                for item in flat_list:
                    encoded_item = item.encode('utf-8')
                    response_parts.append(f"${len(encoded_item)}\r\n{item}\r\n")
                    
                return "".join(response_parts)
            
            case "PUBLISH", [channel, message]:
                receiver_count = await pubsub_service.publish(channel, message)
                return f":{receiver_count}\r\n"
            
            case "SUBSCRIBE", [channel]:
                client_queue = await pubsub_service.subscribe(channel)
                return "SUBSCRIBE_SIGNAL", client_queue, channel
            
            case "BGREWRITEAOF", []:
                if redis_service.aof is None:
                    return "-ERR AOF is disabled\r\n"
                
                await redis_service.start_aof_rewrite()
                return "+Background append only file rewriting started\r\n"
            
            case "MULTI", []:
                if session is None:
                    return "-ERR MULTI context missing\r\n"
                if session.in_transaction:
                    return "-ERR MULTI calls can not be nested\r\n"
                
                session.in_transaction = True
                session.tx_queue.clear()
                return "+OK\r\n"
            
            case "DISCARD", []:
                if session is None or not session.in_transaction:
                    return "-ERR DISCARD without MULTI\r\n"
                
                session.in_transaction = False
                session.tx_queue.clear()
                return "+OK\r\n"
            
            case "EXEC", []:
                if session is None or not session.in_transaction:
                    return "-ERR EXEC without MULTI\r\n"
                
                if not session.tx_queue:
                    session.in_transaction = False
                    return "*0\r\n"
                
                session.in_transaction = False
                commands_to_run = session.tx_queue.copy()
                session.tx_queue.clear()

                tx_responses = []
                tx_aof_bytes = []

                from src.network.tcp_server import build_resp_bytes

                for cmd in commands_to_run:
                    cmd_bytes = build_resp_bytes(cmd)
                    result = await dispatch_command(
                        cmd, redis_service, pubsub_service, cmd_bytes, disable_aof=True, session=None
                    )

                    if isinstance(result, str):
                        tx_responses.append(result)

                        if not result.startswith("-ERR"):
                            tx_aof_bytes.append(cmd_bytes)

                if tx_aof_bytes and redis_service.aof is not None:
                    all_tx_bytes = b"".join(tx_aof_bytes)
                    await redis_service.aof.append(all_tx_bytes)

                final_resp = f"*{len(tx_responses)}\r\n" + "".join(tx_responses)
                return final_resp

            case _:
                return f"-ERR unknown command '{command}'\r\n"
        
    except WrongTypeException:
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    except Exception as e:
        return f"-ERR internal server error: {str(e)}\r\n"