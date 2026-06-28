from dataclasses import dataclass, field
from typing import Any


@dataclass
class ClientSession:
    subscribed_channel: str | None = None
    client_queue: Any | None = None
    in_transaction: bool = False
    tx_queue: list[list[str]] = field(default_factory=list)