import asyncio
import io
from typing import Optional

from src.protocol.parser import parse_resp
from src.services.command_dispatcher import dispatch_command
from src.services.redis_service import RedisService
from src.services.pubsub_service import PubSubService
from src.domain.entities.session import ClientSession


async def publish_loop(queue: asyncio.Queue, writer: asyncio.StreamWriter, channel: str):
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


def build_resp_bytes(command_parts: list[str]) -> bytes:
    res = f"*{len(command_parts)}\r\n"
    for part in command_parts:
        encoded = part.encode('utf-8')
        res += f"${len(encoded)}\r\n{part}\r\n"

    return res.encode('utf-8')


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    service: RedisService,
    pubsub_service: PubSubService,
):
    addr = writer.get_extra_info("peername")
    print(f"[*] Новое подключение от {addr}")

    session = ClientSession()
    publisher_task = None

    try:
        while True:
            data = await reader.read(65536)

            if not data:
                print(f"[*] Клиент {addr} отключился")
                break

            buffer = io.BytesIO(data)
            pipeline_responses = []
            len_data = len(data)

            while buffer.tell() < len_data:
                try:
                    command = parse_resp(buffer)
                except Exception:
                    pipeline_responses.append(b"-ERR protocol error\r\n")
                    break

                if not isinstance(command, list):
                    pipeline_responses.append(b"-ERR protocol error\r\n")
                    continue 

                cmd_name = ""
                if command and isinstance(command[0], str):
                    cmd_name = command[0].upper()

                if session.in_transaction and cmd_name not in ("EXEC", "DISCARD", "MULTI"):
                    session.tx_queue.append(command)
                    pipeline_responses.append(b"+QUEUED\r\n")
                    continue

                raw_cmd_bytes = build_resp_bytes(command)
                result = await dispatch_command(command, service, pubsub_service, raw_cmd_bytes, session=session)

                if isinstance(result, str):
                    pipeline_responses.append(result.encode())
                    continue

                if isinstance(result, tuple) and result[0] == "SUBSCRIBE_SIGNAL":
                    _, queue, channel = result

                    session.subscribed_channel = channel
                    session.client_queue = queue

                    ack = (
                        f"*3\r\n"
                        f"$9\r\nsubscribe\r\n"
                        f"${len(channel)}\r\n{channel}\r\n"
                        f":1\r\n"
                    )
                    pipeline_responses.append(ack.encode())

                    if publisher_task is not None:
                        publisher_task.cancel()
                        try:
                            await publisher_task
                        except asyncio.CancelledError:
                            pass

                    publisher_task = asyncio.create_task(publish_loop(queue, writer, channel))
                    break

            if pipeline_responses:
                writer.write(b"".join(pipeline_responses))
                await writer.drain()

    except (ConnectionError, asyncio.CancelledError):
        print(f"[*] Клиент {addr} разорвал соединение")

    finally:
        if publisher_task is not None:
            publisher_task.cancel()
            try:
                await publisher_task
            except asyncio.CancelledError:
                pass

        if session.subscribed_channel and session.client_queue:
            await pubsub_service.unsubscribe(session.subscribed_channel, session.client_queue)
            print(f"[*] Очередь подписчика удалена из канала {session.subscribed_channel}")

        writer.close()
        try:
            await writer.wait_closed()
        except (ConnectionError, BrokenPipeError):
            pass