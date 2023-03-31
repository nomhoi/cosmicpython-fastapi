from datetime import date

import pytest
from domain import model
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import text


@pytest.mark.asyncio
async def test_orderline_mapper_can_load_lines(session):
    await session.execute(
        text(
            "INSERT INTO order_lines (orderid, sku, qty) VALUES "
            '("order1", "RED-CHAIR", 12),'
            '("order1", "RED-TABLE", 13),'
            '("order2", "BLUE-LIPSTICK", 14)'
        )
    )

    expected = [
        model.OrderLine("order1", "RED-CHAIR", 12),
        model.OrderLine("order1", "RED-TABLE", 13),
        model.OrderLine("order2", "BLUE-LIPSTICK", 14),
    ]
    assert (await session.execute(select(model.OrderLine))).scalars().all() == expected


@pytest.mark.asyncio
async def test_orderline_mapper_can_save_lines(session):
    new_line = model.OrderLine("order1", "DECORATIVE-WIDGET", 12)
    session.add(new_line)
    await session.commit()

    rows = list(
        await session.execute(text('SELECT orderid, sku, qty FROM "order_lines"'))
    )
    assert rows == [("order1", "DECORATIVE-WIDGET", 12)]


@pytest.mark.asyncio
async def test_retrieving_batches(session: AsyncSession):
    await session.execute(
        text(
            "INSERT INTO batches (reference, sku, purchased_quantity, eta)"
            ' VALUES ("batch1", "sku1", 100, null)'
        )
    )
    await session.execute(
        text(
            "INSERT INTO batches (reference, sku, purchased_quantity, eta)"
            ' VALUES ("batch2", "sku2", 200, "2011-04-11")'
        )
    )

    expected = [
        model.Batch("batch1", "sku1", 100, eta=None),
        model.Batch("batch2", "sku2", 200, eta=date(2011, 4, 11)),
    ]
    assert (await session.execute(select(model.Batch))).scalars().all() == expected


@pytest.mark.asyncio
async def test_saving_batches(session):
    batch = model.Batch("batch1", "sku1", 100, eta=None)
    session.add(batch)
    await session.commit()

    rows = await session.execute(
        text('SELECT reference, sku, purchased_quantity, eta FROM "batches"')
    )
    assert list(rows) == [("batch1", "sku1", 100, None)]


@pytest.mark.asyncio
async def test_saving_allocations(session):
    batch = model.Batch("batch1", "sku1", 100, eta=None)
    line = model.OrderLine("order1", "sku1", 10)
    batch.allocate(line)
    session.add(batch)
    await session.commit()

    rows = list(
        await session.execute(text('SELECT orderline_id, batch_id FROM "allocations"'))
    )
    assert rows == [(batch.id, line.id)]


@pytest.mark.asyncio
async def test_retrieving_allocations(session):
    await session.execute(
        text(
            'INSERT INTO order_lines (orderid, sku, qty) VALUES ("order1", "sku1", 12)'
        )
    )
    [[olid]] = await session.execute(
        text("SELECT id FROM order_lines WHERE orderid=:orderid AND sku=:sku"),
        dict(orderid="order1", sku="sku1"),
    )
    await session.execute(
        text(
            "INSERT INTO batches (reference, sku, purchased_quantity, eta)"
            ' VALUES ("batch1", "sku1", 100, null)'
        )
    )
    [[bid]] = await session.execute(
        text("SELECT id FROM batches WHERE reference=:ref AND sku=:sku"),
        dict(ref="batch1", sku="sku1"),
    )
    await session.execute(
        text("INSERT INTO allocations (orderline_id, batch_id) VALUES (:olid, :bid)"),
        dict(olid=olid, bid=bid),
    )

    batch = (
        (
            await session.execute(
                select(model.Batch).options(selectinload(model.Batch.allocations))
            )
        )
        .scalars()
        .one()
    )
    assert batch.allocations == {model.OrderLine("order1", "sku1", 12)}
