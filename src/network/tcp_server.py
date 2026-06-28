import asyncio
import io

from src.protocol.parser import parse_resp
from src.services.command_dispatcher import dispatch_command
from src.services.redis_service import RedisService
from src.services.pubsub_service import PubSubService


async def publish_loop(queue: asyncio.Queue,writer: asyncio.StreamWriter, channel: str):
    while True:
        msg = await queue.get()

        resp = (
            f"*3\r\n"
            f"$7\r\nmessage\r\n"
            f"${len(channel)}\r\n{channel}\r\n"
            f"${len(msg)}\r\n{msg}\r\n"
        )

        writer.write(resp.encode())
        await writer.drain()


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    service: RedisService,
    pubsub_service: PubSubService,
):
    addr = writer.get_extra_info("peername")
    print(f"[*] Новое подключение от {addr}")

    subscribed_channel = None
    client_queue = None
    publisher_task = None

    try:
        while True:
            data = await reader.read(1024)

            if not data:
                print(f"[*] Клиент {addr} отключился")
                break

            buffer = io.BytesIO(data)
            command = parse_resp(buffer)

            if not isinstance(command, list):
                writer.write(b"-ERR protocol error\r\n")
                await writer.drain()
                continue

            result = await dispatch_command(command, service, pubsub_service, data)

            if isinstance(result, str):
                writer.write(result.encode())
                await writer.drain()
                continue

            if isinstance(result, tuple) and result[0] == "SUBSCRIBE_SIGNAL":
                _, queue, channel = result

                subscribed_channel = channel
                client_queue = queue

                ack = (
                    f"*3\r\n"
                    f"$9\r\nsubscribe\r\n"
                    f"${len(channel)}\r\n{channel}\r\n"
                    f":1\r\n"
                )

                writer.write(ack.encode())
                await writer.drain()

                if publisher_task is not None:
                    publisher_task.cancel()

                    try:
                        await publisher_task
                    except asyncio.CancelledError:
                        pass

                publisher_task = asyncio.create_task(publish_loop(queue, writer, channel))

    except (ConnectionError, asyncio.CancelledError):
        print(f"[*] Клиент {addr} разорвал соединение")

    finally:
        if publisher_task is not None:
            publisher_task.cancel()

            try:
                await publisher_task
            except asyncio.CancelledError:
                pass

        if subscribed_channel and client_queue:
            await pubsub_service.unsubscribe(subscribed_channel,client_queue)
            print(f"[*] Очередь подписчика удалена из канала {subscribed_channel}")

        writer.close()

        try:
            await writer.wait_closed()
        except (ConnectionError, BrokenPipeError):
            pass