from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Uuid
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, CreatedAtMixin


class Notification(CreatedAtMixin, Base):
    __tablename__ = "notifications"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    event_id: Mapped[UUID] = mapped_column(Uuid, nullable=False, unique=True)
    order_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    customer_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
