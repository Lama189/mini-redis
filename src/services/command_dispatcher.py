from src.services.redis_service import RedisService


async def dispatch_command(payload: list[str], service: RedisService) -> str:
    command = payload[0].upper()
    args = payload[1:]

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

        case _:
            return f"-ERR unknown command '{command}'\r\n"