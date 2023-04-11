# pylint: disable=no-self-use
from __future__ import annotations

from collections import defaultdict
from datetime import date

import pytest
from allocation import bootstrap
from allocation.adapters import notifications, repository
from allocation.domain import commands
from allocation.service_layer import handlers, unit_of_work


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


class FakeNotifications(notifications.AbstractNotifications):
    def __init__(self):
        self.sent = defaultdict(list)  # type: Dict[str, List[str]]

    def send(self, destination, message):
        self.sent[destination].append(message)


def bootstrap_test_app():
    return bootstrap.bootstrap(
        start_orm=False,
        uow=FakeUnitOfWork(),
        notifications=FakeNotifications(),
        publish=lambda *args: None,
    )


class TestAddBatch:
    @pytest.mark.asyncio
    async def test_for_new_product(self):
        bus = bootstrap_test_app()
        await bus.handle(commands.CreateBatch("b1", "CRUNCHY-ARMCHAIR", 100, None))
        assert await bus.uow.products.get("CRUNCHY-ARMCHAIR") is not None
        assert bus.uow.committed

    @pytest.mark.asyncio
    async def test_for_existing_product(self):
        bus = bootstrap_test_app()
        await bus.handle(commands.CreateBatch("b1", "GARISH-RUG", 100, None))
        await bus.handle(commands.CreateBatch("b2", "GARISH-RUG", 99, None))
        assert "b2" in [
            b.reference for b in (await bus.uow.products.get("GARISH-RUG")).batches
        ]


class TestAllocate:
    @pytest.mark.asyncio
    async def test_allocates(self):
        bus = bootstrap_test_app()
        await bus.handle(commands.CreateBatch("batch1", "COMPLICATED-LAMP", 100, None))
        await bus.handle(commands.Allocate("o1", "COMPLICATED-LAMP", 10))
        [batch] = (await bus.uow.products.get("COMPLICATED-LAMP")).batches
        assert batch.available_quantity == 90

    @pytest.mark.asyncio
    async def test_errors_for_invalid_sku(self):
        bus = bootstrap_test_app()
        await bus.handle(commands.CreateBatch("b1", "AREALSKU", 100, None))

        with pytest.raises(handlers.InvalidSku, match="Invalid sku NONEXISTENTSKU"):
            await bus.handle(commands.Allocate("o1", "NONEXISTENTSKU", 10))

    @pytest.mark.asyncio
    async def test_commits(self):
        bus = bootstrap_test_app()
        await bus.handle(commands.CreateBatch("b1", "OMINOUS-MIRROR", 100, None))
        await bus.handle(commands.Allocate("o1", "OMINOUS-MIRROR", 10))
        assert bus.uow.committed

    @pytest.mark.asyncio
    async def test_sends_email_on_out_of_stock_error(self):
        fake_notifs = FakeNotifications()
        bus = bootstrap.bootstrap(
            start_orm=False,
            uow=FakeUnitOfWork(),
            notifications=fake_notifs,
            publish=lambda *args: None,
        )
        await bus.handle(commands.CreateBatch("b1", "POPULAR-CURTAINS", 9, None))
        await bus.handle(commands.Allocate("o1", "POPULAR-CURTAINS", 10))
        assert fake_notifs.sent["stock@made.com"] == [
            "Out of stock for POPULAR-CURTAINS",
        ]


class TestChangeBatchQuantity:
    @pytest.mark.asyncio
    async def test_changes_available_quantity(self):
        bus = bootstrap_test_app()
        await bus.handle(commands.CreateBatch("batch1", "ADORABLE-SETTEE", 100, None))
        [batch] = (await bus.uow.products.get(sku="ADORABLE-SETTEE")).batches
        assert batch.available_quantity == 100

        await bus.handle(commands.ChangeBatchQuantity("batch1", 50))
        assert batch.available_quantity == 50

    @pytest.mark.asyncio
    async def test_reallocates_if_necessary(self):
        bus = bootstrap_test_app()
        history = [
            commands.CreateBatch("batch1", "INDIFFERENT-TABLE", 50, None),
            commands.CreateBatch("batch2", "INDIFFERENT-TABLE", 50, date.today()),
            commands.Allocate("order1", "INDIFFERENT-TABLE", 20),
            commands.Allocate("order2", "INDIFFERENT-TABLE", 20),
        ]
        for msg in history:
            await bus.handle(msg)
        [batch1, batch2] = (await bus.uow.products.get(sku="INDIFFERENT-TABLE")).batches
        assert batch1.available_quantity == 10
        assert batch2.available_quantity == 50

        await bus.handle(commands.ChangeBatchQuantity("batch1", 25))

        # order1 or order2 will be deallocated, so we'll have 25 - 20
        assert batch1.available_quantity == 5
        # and 20 will be reallocated to the next batch
        assert batch2.available_quantity == 30
