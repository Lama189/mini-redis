import asyncio

from src.interfaces.repository import IEntryRepository


async def active_expire_worker(repo: IEntryRepository, interval_seconds: float = 1.0):
    print("[*] Фоновый воркер очистки TTL успешно запущен.")

    try:
        while True:
            await asyncio.sleep(interval_seconds)
            await repo.expire_active_step()

    except asyncio.CancelledError:
        print("[*] Фоновый воркер очистки TTL остановлен.")
    except Exception as e:
        print(f"[!] Критическая ошибка в воркере TTL: {e}")
