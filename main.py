import asyncio
from functools import partial

from src.services.redis_service import RedisService
from src.services.ttl_service import active_expire_worker
from src.storage.repository import RedisRepository
from src.network.tcp_server import handle_client


async def main():
    host = "127.0.0.1"
    port = 6379

    repository = RedisRepository()
    service = RedisService(repository)
    ttl_task = asyncio.create_task(active_expire_worker(repository, 1.0))
    

    client_callback = partial(handle_client, service=service)
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