# pylint: disable=no-self-use
from datetime import date

import pytest
from allocation.adapters import repository
from allocation.domain import commands
from allocation.service_layer import handlers, messagebus, unit_of_work
from asynctest import mock


class FakeRepository(repository.AbstractRepository):
    def __init__(self, products):
        super().__init__()
        self._products = set(products)

    async def _add(self, product):
        self._products.add(product)

    async def _get(self, sku):
        return next((p for p in self._products if p.sku == sku), None)

    async def _get_by_batchref(self, batchref):
        return next(
            (p for p in self._products for b in p.batches if b.reference == batchref),
            None,
        )


class FakeUnitOfWork(unit_of_work.AbstractUnitOfWork):
    def __init__(self):
        self.products = FakeRepository([])
        self.committed = False

    async def _commit(self):
        self.committed = True

    async def rollback(self):
        pass


class TestAddBatch:
    @pytest.mark.asyncio
    async def test_for_new_product(self):
        uow = FakeUnitOfWork()
        await messagebus.handle(
            commands.CreateBatch("b1", "CRUNCHY-ARMCHAIR", 100, None), uow
        )
        assert await uow.products.get("CRUNCHY-ARMCHAIR") is not None
        assert uow.committed

    @pytest.mark.asyncio
    async def test_for_existing_product(self):
        uow = FakeUnitOfWork()
        await messagebus.handle(
            commands.CreateBatch("b1", "GARISH-RUG", 100, None), uow
        )
        await messagebus.handle(commands.CreateBatch("b2", "GARISH-RUG", 99, None), uow)
        assert "b2" in [
            b.reference for b in (await uow.products.get("GARISH-RUG")).batches
        ]


@pytest.fixture(autouse=True)
def fake_redis_publish():
    with mock.patch("allocation.adapters.redis_eventpublisher.publish"):
        yield


class TestAllocate:
    @pytest.mark.asyncio
    async def test_allocates(self):
        uow = FakeUnitOfWork()
        await messagebus.handle(
            commands.CreateBatch("batch1", "COMPLICATED-LAMP", 100, None), uow
        )
        results = await messagebus.handle(
            commands.Allocate("o1", "COMPLICATED-LAMP", 10), uow
        )
        assert results.pop(0) == "batch1"
        [batch] = (await uow.products.get("COMPLICATED-LAMP")).batches
        assert batch.available_quantity == 90

    @pytest.mark.asyncio
    async def test_errors_for_invalid_sku(self):
        uow = FakeUnitOfWork()
        await messagebus.handle(commands.CreateBatch("b1", "AREALSKU", 100, None), uow)

        with pytest.raises(handlers.InvalidSku, match="Invalid sku NONEXISTENTSKU"):
            await messagebus.handle(commands.Allocate("o1", "NONEXISTENTSKU", 10), uow)

    @pytest.mark.asyncio
    async def test_commits(self):
        uow = FakeUnitOfWork()
        await messagebus.handle(
            commands.CreateBatch("b1", "OMINOUS-MIRROR", 100, None), uow
        )
        await messagebus.handle(commands.Allocate("o1", "OMINOUS-MIRROR", 10), uow)
        assert uow.committed

    @pytest.mark.asyncio
    async def test_sends_email_on_out_of_stock_error(self):
        uow = FakeUnitOfWork()
        await messagebus.handle(
            commands.CreateBatch("b1", "POPULAR-CURTAINS", 9, None), uow
        )

        with mock.patch("allocation.adapters.email.send") as mock_send_mail:
            await messagebus.handle(
                commands.Allocate("o1", "POPULAR-CURTAINS", 10), uow
            )
            assert mock_send_mail.call_args == mock.call(
                "stock@made.com", "Out of stock for POPULAR-CURTAINS"
            )


class TestChangeBatchQuantity:
    @pytest.mark.asyncio
    async def test_changes_available_quantity(self):
        uow = FakeUnitOfWork()
        await messagebus.handle(
            commands.CreateBatch("batch1", "ADORABLE-SETTEE", 100, None), uow
        )
        [batch] = (await uow.products.get(sku="ADORABLE-SETTEE")).batches
        assert batch.available_quantity == 100

        await messagebus.handle(commands.ChangeBatchQuantity("batch1", 50), uow)

        assert batch.available_quantity == 50

    @pytest.mark.asyncio
    async def test_reallocates_if_necessary(self):
        uow = FakeUnitOfWork()
        history = [
            commands.CreateBatch("batch1", "INDIFFERENT-TABLE", 50, None),
            commands.CreateBatch("batch2", "INDIFFERENT-TABLE", 50, date.today()),
            commands.Allocate("order1", "INDIFFERENT-TABLE", 20),
            commands.Allocate("order2", "INDIFFERENT-TABLE", 20),
        ]
        for msg in history:
            await messagebus.handle(msg, uow)
        [batch1, batch2] = (await uow.products.get(sku="INDIFFERENT-TABLE")).batches
        assert batch1.available_quantity == 10
        assert batch2.available_quantity == 50

        await messagebus.handle(commands.ChangeBatchQuantity("batch1", 25), uow)

        # order1 or order2 will be deallocated, so we'll have 25 - 20
        assert batch1.available_quantity == 5
        # and 20 will be reallocated to the next batch
        assert batch2.available_quantity == 30
