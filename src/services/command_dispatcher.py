from src.services.redis_service import RedisService

from src.domain.exceptions import WrongTypeException


async def dispatch_command(payload: list[str], service: RedisService) -> str:
    command = payload[0].upper()
    args = payload[1:]

    try:
        match command, args:
            case "SET", [key, value]:
                await service.set(key, value)
                return "+OK\r\n"

            case "SET", [key, value, "EX", ttl_str]:
                try:
                    ttl = int(ttl_str)
                except ValueError:
                    return "-ERR value is not an integer or out of range\r\n"

                await service.set(key, value, ttl)
                return "+OK\r\n"

            case "GET", [key]:
                value = await service.get(key)

                if value is None:
                    return "$-1\r\n"

                return f"${len(value)}\r\n{value}\r\n"

            case "DEL", [*keys] if keys:
                deleted = await service.delete(keys)
                return f":{deleted}\r\n"
            
            case "HSET", [key, *field_values, "EX", ttl_str] if len(field_values) >= 2 and len(field_values) % 2 == 0:
                try:
                    ttl = int(ttl_str)
                except ValueError:
                    return "-ERR value is not an integer or out of range\r\n"
                
                it = iter(field_values)
                fields = dict(zip(it, it))

                added = await service.hset(key, fields, ttl)
                return f":{added}\r\n"
            
            case "HGET", [key, field]:
                value = await service.hget(key, field)

                if value is None:
                    return "$-1\r\n"

                return f"${len(value)}\r\n{value}\r\n"
            
            case "HDEL", [key, *fields]:
                fields_arg = fields if len(fields) > 0 else None

                deleted = await service.hdel(key, fields) 
                return f":{deleted}\r\n"
            
            case "HEXISTS", [key, field]:
                value = await service.hexists(key, field)
                return f":{value}\r\n"
            
            case "HLEN", [key]:
                count = await service.hlen(key)
                return f":{count}\r\n"

            case _:
                return f"-ERR unknown command '{command}'\r\n"
        
    except WrongTypeException:
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    except Exception as e:
        return f"-ERR internal server error: {str(e)}\r\n"