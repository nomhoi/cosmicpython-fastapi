# pylint: disable=unused-argument
from __future__ import annotations

from dataclasses import asdict
from typing import TYPE_CHECKING

from allocation.adapters import email, redis_eventpublisher
from allocation.domain import commands, events, model
from allocation.domain.model import OrderLine
from sqlalchemy.sql import text

if TYPE_CHECKING:
    from . import unit_of_work


class InvalidSku(Exception):
    pass


async def add_batch(
    cmd: commands.CreateBatch,
    uow: unit_of_work.AbstractUnitOfWork,
):
    async with uow:
        product = await uow.products.get(sku=cmd.sku)
        if product is None:
            product = model.Product(cmd.sku, batches=[])
            await uow.products.add(product)
        product.batches.append(model.Batch(cmd.ref, cmd.sku, cmd.qty, cmd.eta))
        await uow.commit()


async def allocate(
    cmd: commands.Allocate,
    uow: unit_of_work.AbstractUnitOfWork,
):
    line = OrderLine(cmd.orderid, cmd.sku, cmd.qty)
    async with uow:
        product = await uow.products.get(sku=line.sku)
        if product is None:
            raise InvalidSku(f"Invalid sku {line.sku}")
        product.allocate(line)
        await uow.commit()


async def reallocate(
    event: events.Deallocated,
    uow: unit_of_work.AbstractUnitOfWork,
):
    async with uow:
        product = await uow.products.get(sku=event.sku)
        product.events.append(commands.Allocate(**asdict(event)))
        await uow.commit()


async def change_batch_quantity(
    cmd: commands.ChangeBatchQuantity,
    uow: unit_of_work.AbstractUnitOfWork,
):
    async with uow:
        product = await uow.products.get_by_batchref(batchref=cmd.ref)
        product.change_batch_quantity(ref=cmd.ref, qty=cmd.qty)
        await uow.commit()


# pylint: disable=unused-argument


async def send_out_of_stock_notification(
    event: events.OutOfStock,
    uow: unit_of_work.AbstractUnitOfWork,
):
    await email.send(
        "stock@made.com",
        f"Out of stock for {event.sku}",
    )


async def publish_allocated_event(
    event: events.Allocated,
    uow: unit_of_work.AbstractUnitOfWork,
):
    await redis_eventpublisher.publish("line_allocated", event)


async def add_allocation_to_read_model(
    event: events.Allocated,
    uow: unit_of_work.SqlAlchemyUnitOfWork,
):
    async with uow:
        await uow.session.execute(
            text(
                """
            INSERT INTO allocations_view (orderid, sku, batchref)
            VALUES (:orderid, :sku, :batchref)
            """
            ),
            dict(orderid=event.orderid, sku=event.sku, batchref=event.batchref),
        )
        await uow.commit()


async def remove_allocation_from_read_model(
    event: events.Deallocated,
    uow: unit_of_work.SqlAlchemyUnitOfWork,
):
    async with uow:
        await uow.session.execute(
            text(
                """
            DELETE FROM allocations_view
            WHERE orderid = :orderid AND sku = :sku
            """
            ),
            dict(orderid=event.orderid, sku=event.sku),
        )
        await uow.commit()
