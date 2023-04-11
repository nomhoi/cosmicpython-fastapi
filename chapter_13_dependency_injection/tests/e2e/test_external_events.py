import asyncio
import json

import async_timeout
import pytest
from tests.e2e import api_client, redis_client
from tests.random_refs import random_batchref, random_orderid, random_sku


@pytest.mark.asyncio
async def test_change_batch_quantity_leading_to_reallocation():
    # start with two batches and an order allocated to one of them
    orderid, sku = random_orderid(), random_sku()
    earlier_batch, later_batch = random_batchref("old"), random_batchref("newer")
    api_client.post_to_add_batch(earlier_batch, sku, qty=10, eta="2011-01-01")
    api_client.post_to_add_batch(later_batch, sku, qty=10, eta="2011-01-02")
    r = api_client.post_to_allocate(orderid, sku, 10)
    assert r.ok
    response = api_client.get_allocation(orderid)
    assert response.json()[0]["batchref"] == earlier_batch

    subscription = await redis_client.subscribe_to("line_allocated")

    # change quantity on allocated batch so it's less than our order
    await redis_client.publish_message(
        "change_batch_quantity",
        {"batchref": earlier_batch, "qty": 5},
    )

    # wait until we see a message saying the order has been reallocated
    messages = []

    async with async_timeout.timeout(10):
        await asyncio.sleep(1)
        message = await subscription.get_message(timeout=1)
        if message:
            messages.append(message)
            print(messages)

    assert len(messages) == 1
    data = json.loads(messages[-1]["data"])
    assert data["orderid"] == orderid
    assert data["batchref"] == later_batch
