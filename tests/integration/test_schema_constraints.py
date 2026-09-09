from __future__ import annotations

from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Notification, Order, OrderItem, OutboxMessage, OutboxStatus, Product

SHA256_PLACEHOLDER = "a" * 64


def make_product(
    name: str = "Mechanical keyboard",
    price: Decimal = Decimal("19.99"),
    stock: int = 10,
) -> Product:
    return Product(name=name, price=price, stock=stock)


def make_order(
    customer_id: UUID | None = None,
    total_amount: Decimal = Decimal("19.99"),
    idempotency_key: str = "idempotency-key-1",
    request_hash: str = SHA256_PLACEHOLDER,
) -> Order:
    return Order(
        customer_id=customer_id or uuid4(),
        total_amount=total_amount,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
    )


def make_outbox_message(
    aggregate_id: UUID | None = None,
    event_id: UUID | None = None,
) -> OutboxMessage:
    order_id = aggregate_id or uuid4()
    return OutboxMessage(
        event_id=event_id or uuid4(),
        aggregate_type="order",
        aggregate_id=order_id,
        event_type="order.created",
        payload={"order_id": str(order_id)},
    )


class TestProductConstraints:
    async def test_stock_cannot_be_driven_below_zero(self, session: AsyncSession) -> None:
        product = make_product(stock=1)
        session.add(product)
        await session.commit()

        with pytest.raises(IntegrityError) as failure:
            await session.execute(
                text("UPDATE products SET stock = stock - 2 WHERE id = :id"),
                {"id": product.id},
            )
            await session.commit()

        assert "ck_products_stock_non_negative" in str(failure.value)

    async def test_price_cannot_be_negative(self, session: AsyncSession) -> None:
        session.add(make_product(price=Decimal("-0.01")))

        with pytest.raises(IntegrityError) as failure:
            await session.commit()

        assert "ck_products_price_non_negative" in str(failure.value)

    async def test_name_cannot_be_whitespace_only(self, session: AsyncSession) -> None:
        session.add(make_product(name="   "))

        with pytest.raises(IntegrityError) as failure:
            await session.commit()

        assert "ck_products_name_not_blank" in str(failure.value)

    async def test_price_round_trips_as_an_exact_decimal(self, session: AsyncSession) -> None:
        product = make_product(price=Decimal("1234567.89"))
        session.add(product)
        await session.commit()
        session.expunge_all()

        stored = await session.get(Product, product.id)

        assert stored is not None
        assert stored.price == Decimal("1234567.89")
        assert isinstance(stored.price, Decimal)


class TestOrderIdempotencyConstraints:
    async def test_one_customer_cannot_reuse_an_idempotency_key(
        self, session: AsyncSession
    ) -> None:
        customer_id = uuid4()
        session.add(make_order(customer_id=customer_id, idempotency_key="checkout-1"))
        await session.commit()

        session.add(make_order(customer_id=customer_id, idempotency_key="checkout-1"))

        with pytest.raises(IntegrityError) as failure:
            await session.commit()

        assert "uq_orders_customer_id_idempotency_key" in str(failure.value)

    async def test_different_customers_may_use_the_same_idempotency_key(
        self, session: AsyncSession
    ) -> None:
        session.add(make_order(customer_id=uuid4(), idempotency_key="checkout-1"))
        session.add(make_order(customer_id=uuid4(), idempotency_key="checkout-1"))

        await session.commit()

        orders = (await session.execute(select(Order))).scalars().all()
        assert len(orders) == 2

    async def test_request_hash_must_be_a_sha256_digest(self, session: AsyncSession) -> None:
        session.add(make_order(request_hash="too-short"))

        with pytest.raises(IntegrityError) as failure:
            await session.commit()

        assert "ck_orders_request_hash_is_sha256_hex" in str(failure.value)


class TestOrderItemConstraints:
    async def test_quantity_must_be_positive(self, session: AsyncSession) -> None:
        product = make_product()
        order = make_order()
        session.add_all([product, order])
        await session.commit()

        session.add(
            OrderItem(
                order_id=order.id,
                product_id=product.id,
                quantity=0,
                unit_price=product.price,
            )
        )

        with pytest.raises(IntegrityError) as failure:
            await session.commit()

        assert "ck_order_items_quantity_positive" in str(failure.value)

    async def test_a_product_cannot_appear_twice_in_one_order(self, session: AsyncSession) -> None:
        product = make_product()
        order = make_order()
        session.add_all([product, order])
        await session.commit()

        session.add_all(
            [
                OrderItem(
                    order_id=order.id, product_id=product.id, quantity=1, unit_price=product.price
                ),
                OrderItem(
                    order_id=order.id, product_id=product.id, quantity=2, unit_price=product.price
                ),
            ]
        )

        with pytest.raises(IntegrityError) as failure:
            await session.commit()

        assert "uq_order_items_order_id_product_id" in str(failure.value)

    async def test_deleting_an_order_removes_its_items(self, session: AsyncSession) -> None:
        product = make_product()
        order = make_order()
        session.add_all([product, order])
        await session.commit()
        session.add(
            OrderItem(
                order_id=order.id, product_id=product.id, quantity=1, unit_price=product.price
            )
        )
        await session.commit()

        await session.execute(text("DELETE FROM orders WHERE id = :id"), {"id": order.id})
        await session.commit()

        remaining = (await session.execute(select(OrderItem))).scalars().all()
        assert remaining == []

    async def test_a_product_referenced_by_an_order_cannot_be_deleted(
        self, session: AsyncSession
    ) -> None:
        product = make_product()
        order = make_order()
        session.add_all([product, order])
        await session.commit()
        session.add(
            OrderItem(
                order_id=order.id, product_id=product.id, quantity=1, unit_price=product.price
            )
        )
        await session.commit()

        with pytest.raises(IntegrityError) as failure:
            await session.execute(text("DELETE FROM products WHERE id = :id"), {"id": product.id})
            await session.commit()

        assert "fk_order_items_product_id_products" in str(failure.value)


class TestOutboxConstraints:
    async def test_event_id_is_unique(self, session: AsyncSession) -> None:
        event_id = uuid4()
        session.add(make_outbox_message(event_id=event_id))
        await session.commit()

        session.add(make_outbox_message(event_id=event_id))

        with pytest.raises(IntegrityError) as failure:
            await session.commit()

        assert "uq_outbox_messages_event_id" in str(failure.value)

    async def test_a_new_message_starts_pending_and_immediately_claimable(
        self, session: AsyncSession
    ) -> None:
        message = make_outbox_message()
        session.add(message)
        await session.commit()
        session.expunge_all()

        stored = await session.get(OutboxMessage, message.id)

        assert stored is not None
        assert stored.status is OutboxStatus.PENDING
        assert stored.attempts == 0
        assert stored.schema_version == 1
        assert stored.published_at is None
        assert stored.failed_at is None
        assert stored.available_at is not None

    async def test_status_rejects_a_value_outside_the_enum(self, session: AsyncSession) -> None:
        with pytest.raises(DBAPIError) as failure:
            await session.execute(
                text(
                    "INSERT INTO outbox_messages "
                    "(event_id, aggregate_type, aggregate_id, event_type, payload, status) "
                    "VALUES (gen_random_uuid(), 'order', gen_random_uuid(), 'order.created', "
                    "'{}'::jsonb, 'sent')"
                )
            )

        assert "outbox_status" in str(failure.value)

    async def test_the_publisher_index_is_partial_on_pending_rows(
        self, session: AsyncSession
    ) -> None:
        definition = await session.scalar(
            text("SELECT indexdef FROM pg_indexes WHERE indexname = 'ix_outbox_messages_pending'")
        )

        assert definition is not None
        assert "WHERE (status = 'pending'::outbox_status)" in definition


class TestNotificationConstraints:
    async def test_event_id_is_unique(self, session: AsyncSession) -> None:
        event_id = uuid4()
        session.add(
            Notification(event_id=event_id, order_id=uuid4(), customer_id=uuid4(), payload={})
        )
        await session.commit()

        session.add(
            Notification(event_id=event_id, order_id=uuid4(), customer_id=uuid4(), payload={})
        )

        with pytest.raises(IntegrityError) as failure:
            await session.commit()

        assert "uq_notifications_event_id" in str(failure.value)

    async def test_redelivering_an_event_inserts_a_single_notification(
        self, session: AsyncSession
    ) -> None:
        insert = text(
            "INSERT INTO notifications (id, event_id, order_id, customer_id, payload) "
            "VALUES (gen_random_uuid(), :event_id, :order_id, :customer_id, '{}'::jsonb) "
            "ON CONFLICT (event_id) DO NOTHING RETURNING id"
        )
        parameters = {"event_id": uuid4(), "order_id": uuid4(), "customer_id": uuid4()}

        first = await session.scalar(insert, parameters)
        second = await session.scalar(insert, parameters)
        await session.commit()

        assert first is not None
        assert second is None
        stored = (await session.execute(select(Notification))).scalars().all()
        assert len(stored) == 1
