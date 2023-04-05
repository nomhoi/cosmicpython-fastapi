import abc

from allocation.domain import model
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload


class AbstractRepository(abc.ABC):
    @abc.abstractmethod
    async def add(self, product: model.Product):
        raise NotImplementedError

    @abc.abstractmethod
    async def get(self, sku: str) -> model.Product:
        raise NotImplementedError


class SqlAlchemyRepository(AbstractRepository):
    def __init__(self, session: AsyncSession):
        self.session = session

    async def add(self, product: model.Product):
        self.session.add(product)

    async def get(self, sku: str) -> model.Product:
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
