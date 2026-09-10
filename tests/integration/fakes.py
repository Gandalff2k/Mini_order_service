from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass, field


@dataclass(frozen=True)
class SentMessage:
    topic: str
    key: bytes
    value: bytes
    headers: tuple[tuple[str, bytes], ...]

    @property
    def event_id(self) -> str:
        return dict(self.headers)["event_id"].decode()


class BrokerUnavailableError(RuntimeError):
    pass


@dataclass
class FakeProducer:
    fail_event_ids: frozenset[str] = frozenset()
    fail_all: bool = False
    delay_seconds: float = 0.0
    sent: list[SentMessage] = field(default_factory=list)

    async def send_and_wait(
        self,
        topic: str,
        value: bytes,
        key: bytes,
        headers: Sequence[tuple[str, bytes]],
    ) -> None:
        if self.delay_seconds:
            await asyncio.sleep(self.delay_seconds)
        message = SentMessage(topic=topic, key=key, value=value, headers=tuple(headers))
        if self.fail_all or message.event_id in self.fail_event_ids:
            raise BrokerUnavailableError("broker unavailable")
        self.sent.append(message)
