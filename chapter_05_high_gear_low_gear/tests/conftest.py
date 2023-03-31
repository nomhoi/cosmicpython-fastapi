import asyncio
import time

import config
import pytest
import pytest_asyncio
from adapters.orm import metadata, start_mappers
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine


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
    wait_for_postgres_to_come_up(engine)
    yield engine
    engine.sync_engine.dispose()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def postgres_create(postgres_async_engine):
    async with postgres_async_engine.begin() as conn:
        await conn.run_sync(metadata.create_all)


@pytest_asyncio.fixture
async def postgres_session(postgres_async_engine, postgres_create):
    async with AsyncSession(postgres_async_engine, expire_on_commit=False) as session:
        yield session
