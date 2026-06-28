import asyncio
from typing import Union, Tuple

from src.services.redis_service import RedisService
from src.services.pubsub_service import PubSubService
from src.domain.exceptions import WrongTypeException


async def dispatch_command(
    payload: list[str], 
    redis_service: RedisService,
    pubsub_service: PubSubService,
    raw_data: bytes
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

                return f"${len(value)}\r\n{value}\r\n"

            case "DEL", [*keys] if keys:
                deleted = await redis_service.delete(keys, raw_data)
                return f":{deleted}\r\n"
            
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

                return f"${len(value)}\r\n{value}\r\n"
            
            case "HDEL", [key, *fields]:
                fields_arg = fields if len(fields) > 0 else None

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
                    response_parts.append(f"${len(item)}\r\n{item}\r\n")
                    
                return "".join(response_parts)
            
            case "HKEYS", [key]:
                flat_list = await redis_service.hkeys(key)

                response_parts = [f"*{len(flat_list)}\r\n"]
                for item in flat_list:
                    response_parts.append(f"${len(item)}\r\n{item}\r\n")
                    
                return "".join(response_parts)
            
            case "HVALS", [key]:
                flat_list = await redis_service.hvals(key)

                response_parts = [f"*{len(flat_list)}\r\n"]
                for item in flat_list:
                    response_parts.append(f"${len(item)}\r\n{item}\r\n")
                    
                return "".join(response_parts)
            
            case "PUBLISH", [channel, message]:
                receiver_count = await pubsub_service.publish(channel, message)
                return f":{receiver_count}\r\n"
            
            case "SUBSCRIBE", [channel]:
                client_queue = await pubsub_service.subscribe(channel)
                subscribe_ack = f"*3\r\n$9\r\nsubscribe\r\n${len(channel)}\r\n{channel}\r\n:1\r\n"
                return "SUBSCRIBE_SIGNAL", client_queue, channel
            
            case "BGREWRITEAOF", []:
                if redis_service.aof is None:
                    return "-ERR AOF is disabled\r\n"
                
                await redis_service.start_aof_rewrite()
                return "+Background append only file rewriting started\r\n"

            case _:
                return f"-ERR unknown command '{command}'\r\n"
        
    except WrongTypeException:
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    except Exception as e:
        return f"-ERR internal server error: {str(e)}\r\n"