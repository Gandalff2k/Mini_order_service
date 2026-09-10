from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Notification


class NotificationRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add_unless_seen(
        self,
        *,
        event_id: UUID,
        order_id: UUID,
        customer_id: UUID,
        payload: dict[str, Any],
    ) -> bool:
        statement = (
            insert(Notification)
            .values(
                id=uuid4(),
                event_id=event_id,
                order_id=order_id,
                customer_id=customer_id,
                payload=payload,
            )
            .on_conflict_do_nothing(index_elements=[Notification.event_id])
            .returning(Notification.id)
        )
        return await self._session.scalar(statement) is not None
