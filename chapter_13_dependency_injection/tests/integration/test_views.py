# pylint: disable=redefined-outer-name
from datetime import date
from unittest import mock

import pytest
from allocation import bootstrap, views
from allocation.domain import commands
from allocation.service_layer import unit_of_work

today = date.today()


@pytest.fixture
def sqlite_bus(sqlite_session_factory):
    bus = bootstrap.bootstrap(
        start_orm=False,
        uow=unit_of_work.SqlAlchemyUnitOfWork(sqlite_session_factory),
        notifications=mock.Mock(),
        publish=lambda *args: None,
    )
    yield bus


@pytest.mark.asyncio
async def test_allocations_view(sqlite_bus):
    await sqlite_bus.handle(commands.CreateBatch("sku1batch", "sku1", 50, None))
    await sqlite_bus.handle(commands.CreateBatch("sku2batch", "sku2", 50, today))
    await sqlite_bus.handle(commands.Allocate("order1", "sku1", 20))
    await sqlite_bus.handle(commands.Allocate("order1", "sku2", 20))
    # add a spurious batch and order to make sure we're getting the right ones
    await sqlite_bus.handle(commands.CreateBatch("sku1batch-later", "sku1", 50, today))
    await sqlite_bus.handle(commands.Allocate("otherorder", "sku1", 30))
    await sqlite_bus.handle(commands.Allocate("otherorder", "sku2", 10))

    assert await views.allocations("order1", sqlite_bus.uow) == [
        {"sku": "sku1", "batchref": "sku1batch"},
        {"sku": "sku2", "batchref": "sku2batch"},
    ]


@pytest.mark.asyncio
async def test_deallocation(sqlite_bus):
    await sqlite_bus.handle(commands.CreateBatch("b1", "sku1", 50, None))
    await sqlite_bus.handle(commands.CreateBatch("b2", "sku1", 50, today))
    await sqlite_bus.handle(commands.Allocate("o1", "sku1", 40))
    await sqlite_bus.handle(commands.ChangeBatchQuantity("b1", 10))

    assert await views.allocations("o1", sqlite_bus.uow) == [
        {"sku": "sku1", "batchref": "b2"},
    ]


# @pytest.mark.asyncio
# async def test_allocations_view(sqlite_session_factory):
#     uow = unit_of_work.SqlAlchemyUnitOfWork(sqlite_session_factory)
#     await messagebus.handle(commands.CreateBatch("sku1batch", "sku1", 50, None), uow)
#     await messagebus.handle(commands.CreateBatch("sku2batch", "sku2", 50, today), uow)
#     await messagebus.handle(commands.Allocate("order1", "sku1", 20), uow)
#     await messagebus.handle(commands.Allocate("order1", "sku2", 20), uow)
#     # add a spurious batch and order to make sure we're getting the right ones
#     await messagebus.handle(
#         commands.CreateBatch("sku1batch-later", "sku1", 50, today), uow
#     )
#     await messagebus.handle(commands.Allocate("otherorder", "sku1", 30), uow)
#     await messagebus.handle(commands.Allocate("otherorder", "sku2", 10), uow)

#     assert await views.allocations("order1", uow) == [
#         {"sku": "sku1", "batchref": "sku1batch"},
#         {"sku": "sku2", "batchref": "sku2batch"},
#     ]


# @pytest.mark.asyncio
# async def test_deallocation(sqlite_session_factory):
#     uow = unit_of_work.SqlAlchemyUnitOfWork(sqlite_session_factory)
#     await messagebus.handle(commands.CreateBatch("b1", "sku1", 50, None), uow)
#     await messagebus.handle(commands.CreateBatch("b2", "sku1", 50, today), uow)
#     await messagebus.handle(commands.Allocate("o1", "sku1", 40), uow)
#     await messagebus.handle(commands.ChangeBatchQuantity("b1", 10), uow)

#     assert await views.allocations("o1", uow) == [
#         {"sku": "sku1", "batchref": "b2"},
#     ]
