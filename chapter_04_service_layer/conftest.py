import asyncio
import time
from datetime import datetime

import config
import pytest
import pytest_asyncio
from orm import metadata, start_mappers
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.sql import text


@pytest.fixture(scope="session")
def event_loop():
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session", autouse=True)
def mapper():
    start_mappers()


@pytest_asyncio.fixture(scope="session")
def async_engine():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    yield engine
    engine.sync_engine.dispose()


@pytest_asyncio.fixture
async def create(async_engine):
    async with async_engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    yield
    async with async_engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)


@pytest_asyncio.fixture
async def session(async_engine, create):
    async with AsyncSession(async_engine, expire_on_commit=False) as session:
        yield session


def wait_for_postgres_to_come_up(engine):
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            return engine.connect()
        except OperationalError:
            time.sleep(0.5)
    pytest.fail("Postgres never came up")


@pytest_asyncio.fixture(scope="session")
def postgres_async_engine():
    engine = create_async_engine(config.get_postgres_uri())
    yield engine
    engine.sync_engine.dispose()


@pytest_asyncio.fixture(scope="session")
async def postgres_create(postgres_async_engine):
    async with postgres_async_engine.begin() as conn:
        await conn.run_sync(metadata.create_all)


@pytest_asyncio.fixture
async def postgres_session(postgres_async_engine, postgres_create):
    async with AsyncSession(postgres_async_engine, expire_on_commit=False) as session:
        yield session


@pytest_asyncio.fixture
async def add_stock(postgres_session):
    batches_added = set()
    skus_added = set()

    async def _add_stock(lines):
        for ref, sku, qty, eta in lines:
            await postgres_session.execute(
                text(
                    "INSERT INTO batches (reference, sku, purchased_quantity, eta)"
                    " VALUES (:ref, :sku, :qty, :eta)"
                ),
                dict(
                    ref=ref,
                    sku=sku,
                    qty=qty,
                    eta=datetime.strptime(eta, "%Y-%m-%d").date() if eta else None,
                ),
            )
            [[batch_id]] = await postgres_session.execute(
                text("SELECT id FROM batches WHERE reference=:ref AND sku=:sku"),
                dict(ref=ref, sku=sku),
            )
            batches_added.add(batch_id)
            skus_added.add(sku)
        await postgres_session.commit()

    yield _add_stock

    for batch_id in batches_added:
        await postgres_session.execute(
            text("DELETE FROM allocations WHERE batch_id=:batch_id"),
            dict(batch_id=batch_id),
        )
        await postgres_session.execute(
            text("DELETE FROM batches WHERE id=:batch_id"),
            dict(batch_id=batch_id),
        )
    for sku in skus_added:
        await postgres_session.execute(
            text("DELETE FROM order_lines WHERE sku=:sku"),
            dict(sku=sku),
        )
        await postgres_session.commit()
