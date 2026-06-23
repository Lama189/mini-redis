import asyncio
from functools import partial

from src.storage.storage import Storage
from src.network.tcp_server import handle_client


async def main():
    host = "127.0.0.1"
    port = 6379

    storage = Storage()
    client_callback = partial(handle_client, storage=storage)

    server = await asyncio.start_server(client_callback, host, port)

    addrs = ', '.join([str(sock.getsockname()) for sock in server.sockets])
    print(f"[*] Сервер запущен и слушает на {addrs}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[*] Сервер остановлен вручную.")