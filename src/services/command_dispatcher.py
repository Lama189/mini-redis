async def dispatch_command(payload: list[str]) -> str:
    command = payload[0].upper()
    args = payload[1:]

    match command, args:
        case "PING", []:
            return "+PONG\r\n"
        
        case "PING", [message]:
            return f"${len(message)}\r\n{message}\r\n"
        
        case _:
            return f"-ERR unknown command '{command}'\r\n"