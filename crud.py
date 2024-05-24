import os

import aioredis
import json
import logging
from environs import Env

logger = logging.getLogger(__name__)

env = Env()
env.read_env()
broker_url = env.str('CELERY_BROKER_URL', 'redis://localhost:6379/0')


async def save_orders_to_redis(orders_data):
    redis = aioredis.from_url(broker_url, encoding="utf-8", decode_responses=True)
    for order in orders_data:
        order_number = order["order_number"]
        if order.get("pharmacy_status") == "InPharmacyReady":
            await redis.set(order_number, json.dumps(order))
            logger.info(f"Order {order_number} saved to Redis.")
    await redis.close()
    await redis.connection_pool.disconnect()


async def update_order_status_in_redis(order_number, new_status):
    redis = aioredis.from_url(broker_url, encoding="utf-8", decode_responses=True)
    redis_exists = await redis.exists(order_number)
    if redis_exists:
        order_data = json.loads(await redis.get(order_number))
        order_data['pharmacy_status'] = new_status
        await redis.set(order_number, json.dumps(order_data))
        logger.info(f"Order {order_number} status in Redis updated to {new_status}.")
        await redis.close()
        await redis.connection_pool.disconnect()
        return True
    logger.warning(f"Order {order_number} does not exist in Redis.")

    await redis.close()
    await redis.connection_pool.disconnect()
    return False