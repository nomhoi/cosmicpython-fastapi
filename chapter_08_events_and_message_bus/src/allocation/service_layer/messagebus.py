import asyncio
from typing import List

from allocation.adapters import email
from allocation.domain import events


async def handle_events(published_events: List[events.Event]):
    background_tasks = set()
    for event in published_events:
        for handler in HANDLERS[type(event)]:
            task = asyncio.create_task(handler(event))
            background_tasks.add(task)
            task.add_done_callback(background_tasks.discard)


async def send_out_of_stock_notification(event: events.OutOfStock):
    await email.send_mail(
        "stock@made.com",
        f"Out of stock for {event.sku}",
    )


HANDLERS = {
    events.OutOfStock: [send_out_of_stock_notification],
}  # type: Dict[Type[events.Event], List[Callable]]
