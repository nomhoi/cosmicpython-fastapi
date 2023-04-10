# pylint: disable=redefined-outer-name
import asyncio
import shutil
import subprocess

import async_timeout
import pytest
import pytest_asyncio
import redis.asyncio as redis
from allocation import config
from allocation.adapters.orm import metadata, start_mappers
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

pytest.register_assert_rewrite("tests.e2e.api_client")


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
async def in_memory_db():
    return create_async_engine("sqlite+aiosqlite:///:memory:")


@pytest_asyncio.fixture
async def create_db(in_memory_db):
    async with in_memory_db.begin() as conn:
        await conn.run_sync(metadata.create_all)
    yield
    async with in_memory_db.begin() as conn:
        await conn.run_sync(metadata.drop_all)


@pytest.fixture
def sqlite_session_factory(in_memory_db, create_db):
    yield sessionmaker(bind=in_memory_db, expire_on_commit=False, class_=AsyncSession)


@pytest.fixture
def session(sqlite_session_factory):
    return sqlite_session_factory()


async def wait_for_postgres_to_come_up(engine):
    async with async_timeout.timeout(10):
        return engine.connect()


async def wait_for_redis_to_come_up():
    redis_client = redis.Redis(**config.get_redis_host_and_port())
    async with async_timeout.timeout(10):
        return await redis_client.ping()


@pytest.fixture(scope="session")
def postgres_async_engine():
    engine = create_async_engine(config.get_postgres_uri())
    yield engine
    engine.sync_engine.dispose()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def postgres_create(postgres_async_engine):
    await wait_for_postgres_to_come_up(postgres_async_engine)
    async with postgres_async_engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    yield
    async with postgres_async_engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)


@pytest.fixture
def postgres_session_factory(postgres_async_engine, postgres_create):
    yield sessionmaker(
        bind=postgres_async_engine, expire_on_commit=False, class_=AsyncSession
    )


@pytest.fixture
def postgres_session(postgres_session_factory):
    return postgres_session_factory()


@pytest_asyncio.fixture(scope="session")
async def restart_redis_pubsub():
    await wait_for_redis_to_come_up()
    if not shutil.which("docker-compose"):
        print("skipping restart, assumes running in container")
        return
    subprocess.run(
        ["docker-compose", "restart", "-t", "0", "redis_pubsub"],
        check=True,
    )
