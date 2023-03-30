import pytest_asyncio
from orm import metadata, start_mappers
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine


@pytest_asyncio.fixture(scope="session")
def async_engine():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    start_mappers()
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
