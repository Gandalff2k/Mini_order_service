from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from sqlalchemy import CheckConstraint, Numeric, String, UniqueConstraint, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, CreatedAtMixin

if TYPE_CHECKING:
    from app.models.order_item import OrderItem


class Order(CreatedAtMixin, Base):
    __tablename__ = "orders"
    __table_args__ = (
        UniqueConstraint("customer_id", "idempotency_key"),
        CheckConstraint("total_amount >= 0", name="total_amount_non_negative"),
        CheckConstraint("length(idempotency_key) > 0", name="idempotency_key_not_blank"),
        CheckConstraint("length(request_hash) = 64", name="request_hash_is_sha256_hex"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    customer_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    total_amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    items: Mapped[list[OrderItem]] = relationship(
        back_populates="order",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
