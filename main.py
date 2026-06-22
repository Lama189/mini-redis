import asyncio

from src.network.tcp_server import handle_client


async def main():
    host = "127.0.0.1"
    port = 6379

    server = await asyncio.start_server(handle_client, host, port)

    addrs = ', '.join([str(sock.getsockname()) for sock in server.sockets])
    print(f"[*] Сервер запущен и слушает на {addrs}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[*] Сервер остановлен вручную.")