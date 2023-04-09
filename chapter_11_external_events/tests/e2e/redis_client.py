import json

import redis.asyncio as redis
from allocation import config

r = redis.Redis(**config.get_redis_host_and_port())


async def subscribe_to(channel):
    pubsub = r.pubsub()
    await pubsub.subscribe(channel)
    confirmation = await pubsub.get_message(timeout=3)
    assert confirmation["type"] == "subscribe"
    return pubsub


async def publish_message(channel, message):
    await r.publish(channel, json.dumps(message))
