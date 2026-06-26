import asyncio
import io

from src.protocol.parser import parse_resp
from src.services.command_dispatcher import dispatch_command
from src.services.redis_service import RedisService


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    service: RedisService,
):
    addr = writer.get_extra_info("peername")
    print(f"[*] Новое подключение от {addr}")

    try:
        while True:
            data = await reader.read(1024)

            if not data:
                break

            buffer = io.BytesIO(data)
            command = parse_resp(buffer)

            if not isinstance(command, list):
                writer.write(b"-ERR protocol error\r\n")
                await writer.drain()
                continue

            response = await dispatch_command(command, service)

            writer.write(response.encode())
            await writer.drain()

    finally:
        writer.close()
        await writer.wait_closed()