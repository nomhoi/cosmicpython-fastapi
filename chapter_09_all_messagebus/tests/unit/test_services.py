import pytest
from allocation.adapters import repository
from allocation.service_layer import services, unit_of_work
from asynctest import mock


class FakeRepository(repository.AbstractRepository):
    def __init__(self, products):
        super().__init__()
        self._products = set(products)

    async def _add(self, products):
        self._products.add(products)

    async def _get(self, sku):
        return next((p for p in self._products if p.sku == sku), None)

    async def list(self):
        return list(self._products)


class FakeUnitOfWork(unit_of_work.AbstractUnitOfWork):
    def __init__(self):
        self.products = FakeRepository([])
        self.committed = False

    async def _commit(self):
        self.committed = True

    async def rollback(self):
        pass


@pytest.mark.asyncio
async def test_add_batch_for_new_product():
    uow = FakeUnitOfWork()
    await services.add_batch("b1", "CRUNCHY-ARMCHAIR", 100, None, uow)
    assert await uow.products.get("CRUNCHY-ARMCHAIR") is not None
    assert uow.committed


@pytest.mark.asyncio
async def test_add_batch_for_existing_product():
    uow = FakeUnitOfWork()
    await services.add_batch("b1", "GARISH-RUG", 100, None, uow)
    await services.add_batch("b2", "GARISH-RUG", 99, None, uow)
    assert "b2" in [b.reference for b in (await uow.products.get("GARISH-RUG")).batches]


@pytest.mark.asyncio
async def test_allocate_returns_allocation():
    uow = FakeUnitOfWork()
    await services.add_batch("batch1", "COMPLICATED-LAMP", 100, None, uow)
    result = await services.allocate("o1", "COMPLICATED-LAMP", 10, uow)
    assert result == "batch1"


@pytest.mark.asyncio
async def test_allocate_errors_for_invalid_sku():
    uow = FakeUnitOfWork()
    await services.add_batch("b1", "AREALSKU", 100, None, uow)

    with pytest.raises(services.InvalidSku, match="Invalid sku NONEXISTENTSKU"):
        await services.allocate("o1", "NONEXISTENTSKU", 10, uow)


@pytest.mark.asyncio
async def test_allocate_commits():
    uow = FakeUnitOfWork()
    await services.add_batch("b1", "OMINOUS-MIRROR", 100, None, uow)
    await services.allocate("o1", "OMINOUS-MIRROR", 10, uow)
    assert uow.committed


@pytest.mark.asyncio
async def test_sends_email_on_out_of_stock_error():
    uow = FakeUnitOfWork()
    await services.add_batch("b1", "POPULAR-CURTAINS", 9, None, uow)

    with mock.patch("allocation.adapters.email.send_mail") as mock_send_mail:
        await services.allocate("o1", "POPULAR-CURTAINS", 10, uow)
        assert mock_send_mail.call_args == mock.call(
            "stock@made.com",
            "Out of stock for POPULAR-CURTAINS",
        )
