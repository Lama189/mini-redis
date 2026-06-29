from collections import defaultdict

from src.domain.entities.session import ClientSession


class WaitManager:
    def __init__(self) -> None:
        self._waiting_clients: dict[str, list[ClientSession]] = defaultdict(list)

    def add_waiter(self, key: str, session: ClientSession) -> None:
        if session not in self._waiting_clients[key]:
            self._waiting_clients[key].append(session)

    def remove_waiter(self, key: str, session: ClientSession) -> None:
        if key in self._waiting_clients and session in self._waiting_clients[key]:
            self._waiting_clients[key].remove(session)
            if not self._waiting_clients[key]:
                del self._waiting_clients[key]

    def pop_waiter(self, key: str) -> ClientSession | None:
        if key in self._waiting_clients and self._waiting_clients[key]:
            session = self._waiting_clients[key].pop(0)
            if not self._waiting_clients[key]:
                del self._waiting_clients[key]

            return session
        
        return None