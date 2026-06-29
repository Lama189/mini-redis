from collections import defaultdict

from src.domain.entities.session import ClientSession


class WaitManager:
    def __init__(self) -> None:
        self._waiting_clients: dict[str, list[ClientSession]] = defaultdict(list)
        self._waiting_streams: dict[str, list[tuple[ClientSession, str]]] = defaultdict(list)

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
    
    def add_stream_waiter(self, key: str, session: ClientSession, after_id: str) -> None:
        if not any(s == session for s, _ in self._waiting_streams[key]):
            self._waiting_streams[key].append((session, after_id))

    def remove_stream_waiter(self, key: str, session: ClientSession) -> None:
        if key in self._waiting_streams:
            self._waiting_streams[key] = [
                (s, r_id) for s, r_id in self._waiting_streams[key] if s != session
            ]

            if not self._waiting_streams[key]:
                del self._waiting_streams[key]

    def get_and_clear_stream_waiters(self, key: str) -> list[tuple[ClientSession, str]]:
        if key in self._waiting_streams:
            waiters = self._waiting_streams[key]
            del self._waiting_streams[key]
            return waiters
        return []
    