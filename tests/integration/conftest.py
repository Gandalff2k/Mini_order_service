from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from testcontainers.community.postgres import PostgresContainer

from app.infra.db import create_database_engine, create_session_factory
from app.infra.settings import DatabaseSettings, get_settings

PROJECT_ROOT = Path(__file__).resolve().parents[2]


TABLES = ("order_items", "orders", "products", "outbox_messages", "notifications")

_DATABASE_ENV_VARS = ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME")


@pytest.fixture(scope="session")
def database_settings() -> Iterator[DatabaseSettings]:
    with PostgresContainer("postgres:16-alpine") as container:
        os.environ["DB_HOST"] = container.get_container_host_ip()
        os.environ["DB_PORT"] = str(container.get_exposed_port(5432))
        os.environ["DB_USER"] = container.username
        os.environ["DB_PASSWORD"] = container.password
        os.environ["DB_NAME"] = container.dbname
        get_settings.cache_clear()

        config = Config(str(PROJECT_ROOT / "alembic.ini"))
        config.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
        command.upgrade(config, "head")

        try:
            yield get_settings().database
        finally:
            for variable in _DATABASE_ENV_VARS:
                os.environ.pop(variable, None)
            get_settings.cache_clear()


@pytest.fixture
async def engine(database_settings: DatabaseSettings) -> AsyncIterator[AsyncEngine]:
    engine = create_database_engine(database_settings)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture(autouse=True)
async def _clean_database(engine: AsyncEngine) -> None:
    async with engine.begin() as connection:
        await connection.execute(text(f"TRUNCATE {', '.join(TABLES)} RESTART IDENTITY CASCADE"))


@pytest.fixture
async def session(engine: AsyncEngine) -> AsyncIterator[AsyncSession]:
    session_factory = create_session_factory(engine)
    async with session_factory() as session:
        yield session
