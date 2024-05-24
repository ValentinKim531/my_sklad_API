import logging
import asyncio
from contextlib import asynccontextmanager
from celery import Celery
from aiohttp import ClientSession
import aioredis
from crud import save_orders_to_redis, update_order_status_in_redis
from main import get_daribar_headers, get_mysklad_headers, BASE_URL_DARIBAR, BASE_URL_SKLAD, \
    create_customer_order_in_mysklad, extract_daribar_order_number_from_description, refresh_daribar_token, \
    initialize_tokens
import os


logger = logging.getLogger(__name__)

broker_url = os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0')
backend_url = os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')


# app = Celery('tasks', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')
app = Celery('tasks', broker="redis://redis:6379/0", backend="redis://redis:6379/0")

app.conf.beat_schedule = {
    'process-orders-every-10-seconds': {
        'task': 'celery_worker.process_orders',
        'schedule': 10.0,
    },
    'update-order-statuses-every-10-seconds': {
        'task': 'celery_worker.update_order_statuses',
        'schedule': 10.0,
    },
}

app.conf.timezone = 'UTC'


@asynccontextmanager
async def redis_lock(redis, key: str, timeout: int = 30):
    logger.info(f"Trying to acquire lock for key: {key}")
    if await redis.set(key, 'lock', nx=True, ex=timeout):
        try:
            logger.info(f"Lock acquired for key: {key}")
            yield
        finally:
            await redis.delete(key)
            logger.info(f"Lock released for key: {key}")
    else:
        logger.error(f"Unable to acquire lock for key: {key}")
        raise RuntimeError("Unable to acquire lock")


async def process_orders_async():
    await initialize_tokens()
    redis = aioredis.from_url("redis://localhost", encoding="utf-8", decode_responses=True)
    async with redis_lock(redis, "process_orders_lock", timeout=30):
        headers = await get_daribar_headers()
        url = f"{BASE_URL_DARIBAR}/public/api/v2/orders?limit=2000"
        async with ClientSession() as session:
            response = await session.get(url, headers=headers)

            if response.status == 401:  # Unauthorized
                await refresh_daribar_token()
                headers = await get_daribar_headers()
                response = await session.get(url, headers=headers)

            orders_data = await response.json()
            if orders_data and orders_data.get("result"):
                await save_orders_to_redis(orders_data["result"])
                logger.info("Orders processed and saved to Redis")

                for order_data in orders_data["result"]:
                    if order_data.get("pharmacy_status") != "InPharmacyPlaced":
                        continue
                    daribar_order_number = order_data["order_number"]

                    redis_key = f"daribar_order:{daribar_order_number}"
                    if await redis.exists(redis_key):
                        logger.info(f"Order {daribar_order_number} already exists in Redis.")
                        continue

                    created = await create_customer_order_in_mysklad(order_data)
                    if created:
                        logger.info(f"Order {daribar_order_number} created in MySklad.")
                        await redis.set(redis_key, daribar_order_number)
                        logger.info(f"Order {daribar_order_number} saved to Redis with MySklad order number {daribar_order_number}.")
                    else:
                        logger.info(f"Order {daribar_order_number} already exists in MySklad.")
    await redis.close()


async def update_order_statuses_async():
    redis = aioredis.from_url("redis://localhost", encoding="utf-8", decode_responses=True)
    async with redis_lock(redis, "update_order_statuses_lock", timeout=30):
        status_map = {
            "https://api.moysklad.ru/api/remap/1.2/entity/customerorder/metadata/states/62b75fc9-dbb2-11ee-0a80-165c00130555": "in_pharmacy_placed",
            "https://api.moysklad.ru/api/remap/1.2/entity/customerorder/metadata/states/5d757b07-06cd-11ef-0a80-03c300345ed3": "in_pharmacy_collecting",
            "https://api.moysklad.ru/api/remap/1.2/entity/customerorder/metadata/states/5d757ed2-06cd-11ef-0a80-03c300345ed4": "in_pharmacy_collected",
            "https://api.moysklad.ru/api/remap/1.2/entity/customerorder/metadata/states/5d757fa6-06cd-11ef-0a80-03c300345ed5": "in_pharmacy_ready",
            "https://api.moysklad.ru/api/remap/1.2/entity/customerorder/metadata/states/62b7634a-dbb2-11ee-0a80-165c0013055b": "canceled"
        }

        headers = get_mysklad_headers()
        headers_daribar = await get_daribar_headers()
        url = f"{BASE_URL_SKLAD}/entity/customerorder"

        async with ClientSession() as session:
            response = await session.get(url, headers=headers)

            if response.status == 401:
                logger.info(f"response status: {response.status}")
                await refresh_daribar_token()
                headers_daribar = await get_daribar_headers()
                response = await session.get(url, headers=headers)

            orders_data = await response.json()
            orders = orders_data.get('rows', [])
            logger.info(f"HTTP Request: GET {url} - Status: {response.status}")

            for order in orders:
                daribar_order_number = await extract_daribar_order_number_from_description(order.get('description', ''))
                state_href = order['state']['meta']['href']
                new_status = status_map.get(state_href)

                if not new_status:
                    logger.error(f"Order with name {daribar_order_number} has an unsupported state URL: {state_href}")
                    continue

                updated = await update_order_status_in_redis(daribar_order_number, new_status)

                if updated:
                    daribar_order_url = f"{BASE_URL_DARIBAR}/public/api/v2/orders/{daribar_order_number}"
                    daribar_response = await session.get(daribar_order_url, headers=headers_daribar)
                    if daribar_response.status == 200:
                        daribar_order = await daribar_response.json()
                        current_status = daribar_order.get('pharmacy_status')
                        if current_status in ['canceled', 'closed']:
                            logger.warning(f"Order {daribar_order_number} is already {current_status} on Daribar, skipping update.")
                            continue

                    update_url = f"{BASE_URL_DARIBAR}/public/api/v2/orders/{daribar_order_number}/status"
                    data = {
                        "orderID": daribar_order_number,
                        "status": new_status,
                        "message": ""
                    }

                    logger.info(f"Sending update request to Daribar: {data}")
                    update_response = await session.put(update_url, json=data, headers=headers_daribar)

                    if update_response.status != 200:
                        error_response = await update_response.text()
                        logger.error(f"Failed to update status for order {daribar_order_number} on Daribar server: {update_response.status}, response text: {error_response}")
                        logger.info(f"token {headers_daribar}")
                    else:
                        logger.info(f"Status for order {daribar_order_number} updated to {new_status} successfully")
                else:
                    logger.error(f"Failed to update status for order {daribar_order_number} in Redis.")
                    daribar_order_number = await extract_daribar_order_number_from_description(order.get('description', ''))
                    if daribar_order_number:
                        logger.info(f"Extracted Daribar order number {daribar_order_number} from description for MySklad order {daribar_order_number}.")
                        await redis.set(f"daribar_order:{daribar_order_number}", daribar_order_number)
                        logger.info(f"Restored order {daribar_order_number} in Redis with MySklad order number {daribar_order_number}")
    await redis.close()


@app.task(bind=True, default_retry_delay=5 * 60, max_retries=3)
def process_orders(self):
    try:
        asyncio.run(process_orders_async())
    except RuntimeError as exc:
        raise self.retry(exc=exc)


@app.task(bind=True, default_retry_delay=5 * 60, max_retries=3)
def update_order_statuses(self):
    try:
        asyncio.run(update_order_statuses_async())
    except RuntimeError as exc:
        raise self.retry(exc=exc)




