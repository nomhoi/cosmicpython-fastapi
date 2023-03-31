from __future__ import annotations

from datetime import date
from typing import Optional

from adapters.repository import AbstractRepository
from domain import model


class InvalidSku(Exception):
    pass


def is_valid_sku(sku, batches):
    return sku in {b.sku for b in batches}


async def add_batch(
    ref: str,
    sku: str,
    qty: int,
    eta: Optional[date],
    repo: AbstractRepository,
    session,
) -> None:
    await repo.add(model.Batch(ref, sku, qty, eta))
    await session.commit()


async def allocate(
    orderid: str, sku: str, qty: int, repo: AbstractRepository, session
) -> str:
    line = model.OrderLine(orderid, sku, qty)
    batches = await repo.list()
    if not is_valid_sku(line.sku, batches):
        raise InvalidSku(f"Invalid sku {line.sku}")
    batchref = model.allocate(line, batches)
    await session.commit()
    return batchref
