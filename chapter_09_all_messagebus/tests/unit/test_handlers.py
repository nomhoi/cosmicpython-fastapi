# pylint: disable=no-self-use
from datetime import date

import pytest
from allocation.adapters import repository
from allocation.domain import events
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
            events.BatchCreated("b1", "CRUNCHY-ARMCHAIR", 100, None), uow
        )
        assert await uow.products.get("CRUNCHY-ARMCHAIR") is not None
        assert uow.committed

    @pytest.mark.asyncio
    async def test_for_existing_product(self):
        uow = FakeUnitOfWork()
        await messagebus.handle(events.BatchCreated("b1", "GARISH-RUG", 100, None), uow)
        await messagebus.handle(events.BatchCreated("b2", "GARISH-RUG", 99, None), uow)
        assert "b2" in [
            b.reference for b in (await uow.products.get("GARISH-RUG")).batches
        ]


class TestAllocate:
    @pytest.mark.asyncio
    async def test_returns_allocation(self):
        uow = FakeUnitOfWork()
        await messagebus.handle(
            events.BatchCreated("batch1", "COMPLICATED-LAMP", 100, None), uow
        )
        results = await messagebus.handle(
            events.AllocationRequired("o1", "COMPLICATED-LAMP", 10), uow
        )
        assert results.pop(0) == "batch1"

    @pytest.mark.asyncio
    async def test_errors_for_invalid_sku(self):
        uow = FakeUnitOfWork()
        await messagebus.handle(events.BatchCreated("b1", "AREALSKU", 100, None), uow)

        with pytest.raises(handlers.InvalidSku, match="Invalid sku NONEXISTENTSKU"):
            await messagebus.handle(
                events.AllocationRequired("o1", "NONEXISTENTSKU", 10), uow
            )

    @pytest.mark.asyncio
    async def test_commits(self):
        uow = FakeUnitOfWork()
        await messagebus.handle(
            events.BatchCreated("b1", "OMINOUS-MIRROR", 100, None), uow
        )
        await messagebus.handle(
            events.AllocationRequired("o1", "OMINOUS-MIRROR", 10), uow
        )
        assert uow.committed

    @pytest.mark.asyncio
    async def test_sends_email_on_out_of_stock_error(self):
        uow = FakeUnitOfWork()
        await messagebus.handle(
            events.BatchCreated("b1", "POPULAR-CURTAINS", 9, None), uow
        )

        with mock.patch("allocation.adapters.email.send") as mock_send_mail:
            await messagebus.handle(
                events.AllocationRequired("o1", "POPULAR-CURTAINS", 10), uow
            )
            assert mock_send_mail.call_args == mock.call(
                "stock@made.com", "Out of stock for POPULAR-CURTAINS"
            )


class TestChangeBatchQuantity:
    @pytest.mark.asyncio
    async def test_changes_available_quantity(self):
        uow = FakeUnitOfWork()
        await messagebus.handle(
            events.BatchCreated("batch1", "ADORABLE-SETTEE", 100, None), uow
        )
        [batch] = (await uow.products.get(sku="ADORABLE-SETTEE")).batches
        assert batch.available_quantity == 100

        await messagebus.handle(events.BatchQuantityChanged("batch1", 50), uow)

        assert batch.available_quantity == 50

    @pytest.mark.asyncio
    async def test_reallocates_if_necessary(self):
        uow = FakeUnitOfWork()
        event_history = [
            events.BatchCreated("batch1", "INDIFFERENT-TABLE", 50, None),
            events.BatchCreated("batch2", "INDIFFERENT-TABLE", 50, date.today()),
            events.AllocationRequired("order1", "INDIFFERENT-TABLE", 20),
            events.AllocationRequired("order2", "INDIFFERENT-TABLE", 20),
        ]
        for e in event_history:
            await messagebus.handle(e, uow)
        [batch1, batch2] = (await uow.products.get(sku="INDIFFERENT-TABLE")).batches
        assert batch1.available_quantity == 10
        assert batch2.available_quantity == 50

        await messagebus.handle(events.BatchQuantityChanged("batch1", 25), uow)

        # order1 or order2 will be deallocated, so we'll have 25 - 20
        assert batch1.available_quantity == 5
        # and 20 will be reallocated to the next batch
        assert batch2.available_quantity == 30
