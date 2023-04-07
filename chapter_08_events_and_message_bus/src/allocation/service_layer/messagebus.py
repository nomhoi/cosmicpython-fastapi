import asyncio

from allocation.adapters import email
from allocation.domain import events


async def handle(event: events.Event):
    for handler in HANDLERS[type(event)]:
        task = asyncio.create_task(handler(event))
        await task


async def send_out_of_stock_notification(event: events.OutOfStock):
    await email.send_mail(
        "stock@made.com",
        f"Out of stock for {event.sku}",
    )


HANDLERS = {
    events.OutOfStock: [send_out_of_stock_notification],
}  # type: Dict[Type[events.Event], List[Callable]]
