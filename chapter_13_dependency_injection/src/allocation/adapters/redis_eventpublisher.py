import json
import logging
from dataclasses import asdict

import redis.asyncio as redis
from allocation import config
from allocation.domain import events

logger = logging.getLogger(__name__)

r = redis.Redis(**config.get_redis_host_and_port())


async def publish(channel: str, event: events.Event):
    logging.info("publishing: channel=%s, event=%s", channel, event)
    await r.publish(channel, json.dumps(asdict(event)))
