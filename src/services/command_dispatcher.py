from src.storage.storage import Storage


async def dispatch_command(payload: list[str], storage: Storage) -> str:
    command = payload[0].upper()
    args = payload[1:]

    match command, args:
        case "PING", []:
            return "+PONG\r\n"
        
        case "PING", [message]:
            return f"${len(message)}\r\n{message}\r\n"
        
        case "SET", [key, value]:
            await storage.set(key, value)
            return "+OK\r\n"
        
        case "SET", [key, value, "EX", ttl_str]:
            try:
                ttl = int(ttl_str)
                await storage.set(key, value)
                return "+OK\r\n"
            except ValueError:
                return "-ERR value is not an integer or out of range\r\n"
        
        case "GET", [key]:
            value = await storage.get(key)
            if value is None:
                return "$-1\r\n"
            
            return f"${len(value)}\r\n{value}\r\n"
        
        case "DEL", [*keys] if len(keys) > 0:
            deleted_count = await storage.delete(keys)
            return f":{deleted_count}\r\n"

        case _:
            return f"-ERR unknown command '{command}'\r\n"