from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxMessage, OutboxStatus


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, message: OutboxMessage) -> None:
        self._session.add(message)

    async def claim_batch(self, limit: int, now: datetime) -> Sequence[OutboxMessage]:
        result = await self._session.execute(
            select(OutboxMessage)
            .where(
                OutboxMessage.status == OutboxStatus.PENDING,
                OutboxMessage.available_at <= now,
            )
            .order_by(OutboxMessage.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return result.scalars().all()
