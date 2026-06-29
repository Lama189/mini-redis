import asyncio
from functools import partial

from src.services.redis_service import RedisService
from src.services.pubsub_service import PubSubService
from src.services.aof_service import AofService
from src.services.wait_manager import WaitManager
from src.services.ttl_service import active_expire_worker
from src.services.command_dispatcher import dispatch_command
from src.storage.repository import RedisRepository
from src.network.tcp_server import handle_client


async def main():
    host = "127.0.0.1"
    port = 6379

    repository = RedisRepository()
    aof = AofService()
    pubsub_service = PubSubService()
    wait_manager = WaitManager()

    replay_service = RedisService(repository, aof=None, wait_manager=wait_manager)

    print("[*] Старт восстановления из AOF...")
    saved_commands = aof.read_all_commands_from_file()

    for cmd_parts in saved_commands:
        await dispatch_command(cmd_parts, replay_service, pubsub_service, b"")
    print(f"[*] База успешно восстановлена. Команд накатано: {len(saved_commands)}")

    main_service = RedisService(repository, aof=aof, wait_manager=wait_manager)
    ttl_task = asyncio.create_task(active_expire_worker(repository, 1.0))

    client_callback = partial(
        handle_client, 
        service=main_service, 
        pubsub_service=pubsub_service
    )
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