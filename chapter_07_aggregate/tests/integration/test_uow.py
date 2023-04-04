import asyncio
import time

import pytest
from allocation.domain import model
from allocation.service_layer import unit_of_work
from sqlalchemy.sql import text
from tests.random_refs import random_batchref, random_orderid, random_sku


async def insert_batch(session, ref, sku, qty, eta, product_version=1):
    await session.execute(
        text("INSERT INTO products (sku, version_number) VALUES (:sku, :version)"),
        dict(sku=sku, version=product_version),
    )
    await session.execute(
        text(
            "INSERT INTO batches (reference, sku, purchased_quantity, eta)"
            " VALUES (:ref, :sku, :qty, :eta)"
        ),
        dict(ref=ref, sku=sku, qty=qty, eta=eta),
    )


async def get_allocated_batch_ref(session, orderid, sku):
    [[orderlineid]] = await session.execute(
        text("SELECT id FROM order_lines WHERE orderid=:orderid AND sku=:sku"),
        dict(orderid=orderid, sku=sku),
    )
    [[batchref]] = await session.execute(
        text(
            "SELECT b.reference FROM allocations JOIN batches AS b ON batch_id = b.id"
            " WHERE orderline_id=:orderlineid"
        ),
        dict(orderlineid=orderlineid),
    )
    return batchref


@pytest.mark.asyncio
async def test_uow_can_retrieve_a_batch_and_allocate_to_it(session_factory):
    session = session_factory()
    await insert_batch(session, "batch1", "HIPSTER-WORKBENCH", 100, None)
    await session.commit()

    uow = unit_of_work.SqlAlchemyUnitOfWork(session_factory)
    async with uow:
        product = await uow.products.get(sku="HIPSTER-WORKBENCH")
        line = model.OrderLine("o1", "HIPSTER-WORKBENCH", 10)
        product.allocate(line)
        await uow.commit()

    batchref = await get_allocated_batch_ref(session, "o1", "HIPSTER-WORKBENCH")
    assert batchref == "batch1"


@pytest.mark.asyncio
async def test_rolls_back_uncommitted_work_by_default(session_factory):
    uow = unit_of_work.SqlAlchemyUnitOfWork(session_factory)
    async with uow:
        await insert_batch(uow.session, "batch1", "MEDIUM-PLINTH", 100, None)

    new_session = session_factory()
    rows = list(await new_session.execute(text('SELECT * FROM "batches"')))
    assert rows == []


@pytest.mark.asyncio
async def test_rolls_back_on_error(session_factory):
    class MyException(Exception):
        pass

    uow = unit_of_work.SqlAlchemyUnitOfWork(session_factory)
    with pytest.raises(MyException):
        async with uow:
            await insert_batch(uow.session, "batch1", "LARGE-FORK", 100, None)
            raise MyException()

    new_session = session_factory()
    rows = list(await new_session.execute(text('SELECT * FROM "batches"')))
    assert rows == []


async def try_to_allocate(orderid, sku):
    line = model.OrderLine(orderid, sku, 10)
    async with unit_of_work.SqlAlchemyUnitOfWork() as uow:
        product = await uow.products.get(sku=sku)
        product.allocate(line)
        time.sleep(0.2)
        await uow.commit()


@pytest.mark.asyncio
async def test_concurrent_updates_to_version_are_not_allowed(postgres_session_factory):
    sku, batch = random_sku(), random_batchref()
    session = postgres_session_factory()
    async with session.begin():
        await insert_batch(session, batch, sku, 100, eta=None, product_version=1)

    results = await asyncio.gather(
        try_to_allocate(random_orderid(1), sku),
        try_to_allocate(random_orderid(2), sku),
        return_exceptions=True,
    )
    for exception in results:
        if exception:
            assert "could not serialize access due to concurrent update" in str(
                exception
            )
        else:
            assert True

    async with session.begin():
        [[version]] = await session.execute(
            text("SELECT version_number FROM products WHERE sku=:sku"),
            dict(sku=sku),
        )
        assert version == 2

        orders = await session.execute(
            text(
                "SELECT orderid FROM allocations"
                " JOIN batches ON allocations.batch_id = batches.id"
                " JOIN order_lines ON allocations.orderline_id = order_lines.id"
                " WHERE order_lines.sku=:sku"
            ),
            dict(sku=sku),
        )
        assert orders.rowcount == 1

    async with unit_of_work.SqlAlchemyUnitOfWork() as uow:
        await uow.session.execute(text("select 1"))
