import asyncio
from typing import Any
from dataclasses import dataclass, field


@dataclass
class ClientSession:
    subscribed_channel: str | None = None
    client_queue: Any | None = None
    in_transaction: bool = False
    tx_queue: list[list[str]] = field(default_factory=list)

    blocking_event: asyncio.Event | None = None
    blocking_result: str | None = None