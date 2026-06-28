import asyncio
from typing import Dict, List


class PubSubService:
    def __init__(self) -> None:
        self._channels: Dict[str, List[asyncio.Queue]] = {}
    
    async def subscribe(self, channel: str) -> asyncio.Queue:
        queue = asyncio.Queue()

        if channel not in self._channels:
            self._channels[channel] = []
        self._channels[channel].append(queue)
        return queue
    
    async def unsubscribe(self, channel: str, queue: asyncio.Queue) -> None:
        if channel in self._channels:
            if queue in self._channels[channel]:
                self._channels[channel].remove(queue)

            if not self._channels[channel]:
                del self._channels[channel]

    async def publish(self, channel: str, message: str) -> int:
        if channel not in self._channels:
            return 0
        
        subscribers = self._channels[channel]
        for queue in subscribers:
            await queue.put(message)

        return len(subscribers)