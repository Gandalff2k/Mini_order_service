"""initial schema

Revision ID: a65fcf9c091c
Revises:
Create Date: 2026-09-09 18:27:12.757942+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "a65fcf9c091c"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


outbox_status = postgresql.ENUM(
    "pending",
    "published",
    "failed",
    name="outbox_status",
    create_type=False,
)


def upgrade() -> None:
    outbox_status.create(op.get_bind(), checkfirst=False)

    op.create_table(
        "products",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("price", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column("stock", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("length(btrim(name)) > 0", name=op.f("ck_products_name_not_blank")),
        sa.CheckConstraint("price >= 0", name=op.f("ck_products_price_non_negative")),
        sa.CheckConstraint("stock >= 0", name=op.f("ck_products_stock_non_negative")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_products")),
    )

    op.create_table(
        "orders",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("customer_id", sa.Uuid(), nullable=False),
        sa.Column("total_amount", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "length(idempotency_key) > 0", name=op.f("ck_orders_idempotency_key_not_blank")
        ),
        sa.CheckConstraint(
            "length(request_hash) = 64", name=op.f("ck_orders_request_hash_is_sha256_hex")
        ),
        sa.CheckConstraint("total_amount >= 0", name=op.f("ck_orders_total_amount_non_negative")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_orders")),
        sa.UniqueConstraint(
            "customer_id", "idempotency_key", name=op.f("uq_orders_customer_id_idempotency_key")
        ),
    )

    op.create_table(
        "order_items",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("order_id", sa.Uuid(), nullable=False),
        sa.Column("product_id", sa.Uuid(), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("unit_price", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.CheckConstraint("quantity > 0", name=op.f("ck_order_items_quantity_positive")),
        sa.CheckConstraint("unit_price >= 0", name=op.f("ck_order_items_unit_price_non_negative")),
        sa.ForeignKeyConstraint(
            ["order_id"],
            ["orders.id"],
            name=op.f("fk_order_items_order_id_orders"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["product_id"],
            ["products.id"],
            name=op.f("fk_order_items_product_id_products"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_order_items")),
        sa.UniqueConstraint(
            "order_id", "product_id", name=op.f("uq_order_items_order_id_product_id")
        ),
    )

    op.create_table(
        "outbox_messages",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("aggregate_type", sa.String(length=50), nullable=False),
        sa.Column("aggregate_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("schema_version", sa.SmallInteger(), server_default=sa.text("1"), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", outbox_status, server_default=sa.text("'pending'"), nullable=False),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("attempts >= 0", name=op.f("ck_outbox_messages_attempts_non_negative")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_outbox_messages")),
        sa.UniqueConstraint("event_id", name=op.f("uq_outbox_messages_event_id")),
    )
    op.create_index(
        "ix_outbox_messages_pending",
        "outbox_messages",
        ["available_at", "id"],
        unique=False,
        postgresql_where=sa.text("status = 'pending'"),
    )

    op.create_table(
        "notifications",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("order_id", sa.Uuid(), nullable=False),
        sa.Column("customer_id", sa.Uuid(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_notifications")),
        sa.UniqueConstraint("event_id", name=op.f("uq_notifications_event_id")),
    )


def downgrade() -> None:
    op.drop_table("notifications")
    op.drop_index(
        "ix_outbox_messages_pending",
        table_name="outbox_messages",
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.drop_table("outbox_messages")
    op.drop_table("order_items")
    op.drop_table("orders")
    op.drop_table("products")

    outbox_status.drop(op.get_bind(), checkfirst=False)
