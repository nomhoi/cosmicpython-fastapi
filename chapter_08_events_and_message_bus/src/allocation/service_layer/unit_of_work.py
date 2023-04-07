# pylint: disable=attribute-defined-outside-init
from __future__ import annotations

import abc

from allocation import config
from allocation.adapters import repository
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from . import messagebus


class AbstractUnitOfWork(abc.ABC):
    products: repository.AbstractRepository

    async def __aenter__(self) -> AbstractUnitOfWork:
        return self

    async def __aexit__(self, *args):
        await self.rollback()

    async def commit(self):
        await self._commit()
        await self.publish_events()

    async def publish_events(self):
        for product in self.products.seen:
            while product.events:
                event = product.events.pop(0)
                await messagebus.handle(event)

    @abc.abstractmethod
    async def _commit(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def rollback(self):
        raise NotImplementedError


DEFAULT_SESSION_FACTORY = sessionmaker(
    bind=create_async_engine(
        config.get_postgres_uri(),
        isolation_level="REPEATABLE READ",
        future=True,
        echo=True,
    ),
    expire_on_commit=False,
    class_=AsyncSession,
)


class SqlAlchemyUnitOfWork(AbstractUnitOfWork):
    def __init__(self, session_factory=DEFAULT_SESSION_FACTORY):
        self.session_factory = session_factory

    async def __aenter__(self):
        self.session: AsyncSession = self.session_factory()
        self.products = repository.SqlAlchemyRepository(self.session)
        return await super().__aenter__()

    async def __aexit__(self, *args):
        await super().__aexit__(*args)
        await self.session.close()

    async def _commit(self):
        await self.session.commit()

    async def rollback(self):
        await self.session.rollback()
