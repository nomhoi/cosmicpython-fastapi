import abc
from typing import Set

from allocation.domain import model
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload


class AbstractRepository(abc.ABC):
    def __init__(self):
        self.seen: Set[model.Product] = set()

    async def add(self, product: model.Product):
        await self._add(product)
        self.seen.add(product)

    async def get(self, sku) -> model.Product:
        product = await self._get(sku)
        if product:
            self.seen.add(product)
        return product

    @abc.abstractmethod
    async def _add(self, product: model.Product):
        raise NotImplementedError

    @abc.abstractmethod
    async def _get(self, sku) -> model.Product:
        raise NotImplementedError


class SqlAlchemyRepository(AbstractRepository):
    def __init__(self, session: AsyncSession):
        super().__init__()
        self.session = session

    async def _add(self, product: model.Product):
        self.session.add(product)

    async def _get(self, sku: str) -> model.Product:
        return (
            (
                await self.session.execute(
                    select(model.Product)
                    .options(selectinload(model.Product.batches))
                    .filter_by(sku=sku)
                )
            )
            .scalars()
            .one_or_none()
        )

    async def list(self):
        return (
            (
                await self.session.execute(
                    select(model.Product).options(selectinload(model.Product.batches))
                )
            )
            .scalars()
            .all()
        )
