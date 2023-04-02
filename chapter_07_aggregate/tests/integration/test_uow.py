import pytest
from allocation.domain import model
from allocation.service_layer import unit_of_work
from sqlalchemy.sql import text


async def insert_batch(session, ref, sku, qty, eta):
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
        batch = await uow.batches.get(reference="batch1")
        line = model.OrderLine("o1", "HIPSTER-WORKBENCH", 10)
        batch.allocate(line)
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
