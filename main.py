import aioredis
import uvicorn
from fastapi import FastAPI, HTTPException
import httpx
import base64
from environs import Env
import logging
import pandas as pd
import io
from fastapi.responses import StreamingResponse

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
env = Env()
env.read_env()

BASE_URL_SKLAD = env.str("BASE_URL_SKLAD", "https://api.moysklad.ru/api/remap/1.2")
USERNAME = env.str("USERNAME", "")
PASSWORD = env.str("PASSWORD", "")

BASE_URL_DARIBAR = env.str("BASE_URL_DARIBAR", "https://prod-backoffice.daribar.com")
DARIBAR_ACCESS_TOKEN = ""
DARIBAR_REFRESH_TOKEN = env.str("DARIBAR_REFRESH_TOKEN")

broker_url = env.str('REDIS_URL')
backend_url = env.str('REDIS_URL')

def get_mysklad_headers() -> dict:
    credentials = base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode("utf-8")
    return {
        "Authorization": f"Basic {credentials}",
        "Content-Type": "application/json"
    }

async def get_daribar_headers() -> dict:
    await load_tokens_from_redis()
    return {
        "Authorization": f"Bearer {DARIBAR_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

async def initialize_tokens():
    await load_tokens_from_redis()

    if not DARIBAR_ACCESS_TOKEN or not DARIBAR_REFRESH_TOKEN:
        logger.info("Tokens not found in Redis, refreshing tokens...")
        await refresh_daribar_token()
    else:
        logger.info("Tokens successfully loaded from Redis")

async def refresh_daribar_token():
    global DARIBAR_REFRESH_TOKEN
    refresh_token = DARIBAR_REFRESH_TOKEN
    refresh_url = f"{BASE_URL_DARIBAR}/api/v1/auth/refresh"
    payload = {"refresh_token": refresh_token}
    async with httpx.AsyncClient() as client:
        response = await client.post(refresh_url, json=payload)
        if response.status_code == 200:
            tokens = response.json()
            DARIBAR_ACCESS_TOKEN = tokens['result']['access_token']
            await save_tokens_to_redis(DARIBAR_ACCESS_TOKEN)
            logger.info(f"Daribar token refreshed successfully {DARIBAR_ACCESS_TOKEN}")
        else:
            logger.error(f"Failed to refresh Daribar token: {response.text}")

async def save_tokens_to_redis(access_token):
    redis = await aioredis.from_url(broker_url, encoding="utf-8", decode_responses=True)
    await redis.set("daribar_access_token", access_token)
    await redis.close()

async def load_tokens_from_redis():
    redis = await aioredis.from_url(broker_url, encoding="utf-8", decode_responses=True)
    global DARIBAR_ACCESS_TOKEN
    DARIBAR_ACCESS_TOKEN = await redis.get("daribar_access_token")
    await redis.close()

@app.get("/get_orders")
async def get_orders(limit: int = 200000):
    headers = await get_daribar_headers()
    print(headers)
    url = f"{BASE_URL_DARIBAR}/public/api/v2/orders?limit={limit}"
    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
        if response.status_code == 200:
            logger.info(f"Response.json: {response.json()}")
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)

SKU_MAPPING_HREF = {
    "116864": "https://api.moysklad.ru/api/remap/1.2/entity/product/fa0878c2-e766-11ee-0a80-0bf2000b5bba",
    "116869": "https://api.moysklad.ru/api/remap/1.2/entity/product/a0c9549d-e6cf-11ee-0a80-0c270017d567",
    "116963": "https://api.moysklad.ru/api/remap/1.2/entity/product/c04d9acd-e767-11ee-0a80-0764000b33a5",
    "116866": "https://api.moysklad.ru/api/remap/1.2/entity/product/53acdf4b-dbca-11ee-0a80-0207001b341c",
    "116867": "https://api.moysklad.ru/api/remap/1.2/entity/product/53a95cb3-dbca-11ee-0a80-0207001b3419",
    "116871": "https://api.moysklad.ru/api/remap/1.2/entity/product/538ef7bf-dbca-11ee-0a80-0207001b33fe",
    "116873": "https://api.moysklad.ru/api/remap/1.2/entity/product/539237a0-dbca-11ee-0a80-0207001b3401",
    "116875": "https://api.moysklad.ru/api/remap/1.2/entity/product/53838e9c-dbca-11ee-0a80-0207001b33f4",
    "116970": "https://api.moysklad.ru/api/remap/1.2/entity/product/5398b252-dbca-11ee-0a80-0207001b3407",
    "116972": "https://api.moysklad.ru/api/remap/1.2/entity/product/538029b5-dbca-11ee-0a80-0207001b33f1",
    "116974": "https://api.moysklad.ru/api/remap/1.2/entity/product/538b8035-dbca-11ee-0a80-0207001b33fb",
    "116975": "https://api.moysklad.ru/api/remap/1.2/entity/product/53955af4-dbca-11ee-0a80-0207001b3404",
    "116965": "https://api.moysklad.ru/api/remap/1.2/entity/product/53a6067c-dbca-11ee-0a80-0207001b3416",
    "116962": "https://api.moysklad.ru/api/remap/1.2/entity/product/0c24eb95-e461-11ee-0a80-14090027d135",
    "116976": "https://api.moysklad.ru/api/remap/1.2/entity/product/53b04477-dbca-11ee-0a80-0207001b341f",
    "116961": "https://api.moysklad.ru/api/remap/1.2/entity/product/53a2c51f-dbca-11ee-0a80-0207001b3413",
    "116969": "https://api.moysklad.ru/api/remap/1.2/entity/product/539f8206-dbca-11ee-0a80-0207001b340f",
    "116860": "https://api.moysklad.ru/api/remap/1.2/entity/product/fa0e1a2f-dbbd-11ee-0a80-09c900157922",
    "116868": "https://api.moysklad.ru/api/remap/1.2/entity/product/539c2ce8-dbca-11ee-0a80-0207001b340a",
    "116964": "https://api.moysklad.ru/api/remap/1.2/entity/product/143d66dd-eb5e-11ee-0a80-0979000ffb46",
    "116966": "https://api.moysklad.ru/api/remap/1.2/entity/product/6b900a87-eb88-11ee-0a80-0ee50018a6c6",
    "116856": "https://api.moysklad.ru/api/remap/1.2/entity/product/b5986e63-f273-11ee-0a80-0105001069f0",
    "116857": "https://api.moysklad.ru/api/remap/1.2/entity/product/aa95bf2f-e530-11ee-0a80-0624003f862f",
    "116858": "https://api.moysklad.ru/api/remap/1.2/entity/product/fedeafc1-eb61-11ee-0a80-0f3b000fbc53",
    "116859": "https://api.moysklad.ru/api/remap/1.2/entity/product/0f231e99-f274-11ee-0a80-17e00010927b",
    "116967": "https://api.moysklad.ru/api/remap/1.2/entity/product/c327857b-fff0-11ee-0a80-0b5e002b88c2",
    "116968": "https://api.moysklad.ru/api/remap/1.2/entity/product/550f4198-0635-11ef-0a80-03c3002a8892",
    "116861": "https://api.moysklad.ru/api/remap/1.2/entity/product/b7f3894a-f800-11ee-0a80-008200135ef3",
    "116862": "https://api.moysklad.ru/api/remap/1.2/entity/product/744ac6e9-f273-11ee-0a80-100900104a00",
    "116863": "https://api.moysklad.ru/api/remap/1.2/entity/product/18d2d506-f504-11ee-0a80-00f000481b4c"
}

async def create_customer_order_in_mysklad(order_data: dict):
    headers = get_mysklad_headers()
    url = f"{BASE_URL_SKLAD}/entity/customerorder"
    order_name = str(order_data.get("order_number", ""))


    order_request = {
        "name": "",
        "description": f"Заказ №{order_name}",
        "organization": {
            "meta": {
                "href": "https://api.moysklad.ru/api/remap/1.2/entity/organization/86ee384f-dbbc-11ee-0a80-09c900152256",
                "type": "organization",
                "mediaType": "application/json"
            }
        },
        "agent": {
            "meta": {
                "href": "https://api.moysklad.ru/api/remap/1.2/entity/counterparty/875dd1f5-06df-11ef-0a80-1433003979ea",
                "type": "counterparty",
                "mediaType": "application/json"
            }
        },
        "contract": {
            "meta": {
                "href": "https://api.moysklad.ru/api/remap/1.2/entity/contract/df9e58f0-06e1-11ef-0a80-14330039e5af",
                "type": "contract",
                "mediaType": "application/json"
            }
        },
        "store": {
            "meta": {
                "href": "https://api.moysklad.ru/api/remap/1.2/entity/store/62a2bdde-dbb2-11ee-0a80-165c0013052c",
                "type": "store",
                "mediaType": "application/json"
            }
        },
        "positions": [
            {
                "quantity": item["quantity"],
                "price": item["price"] * 100,
                "assortment": {
                    "meta": {
                        "href": SKU_MAPPING_HREF.get(str(item["sku"]), ""),
                        "type": "product",
                        "mediaType": "application/json"
                    }
                }
            } for item in order_data.get("items", [])
        ]
    }
    logging.info(f"Order_request to Mysklad: {order_request}")

    async with httpx.AsyncClient() as client:
        response = await client.post(url, headers=headers, json=order_request)
        if response.status_code == 200:
            logger.info(f"Created in Mysklad: {order_request}")
            return True
        else:
            logger.error(f"Failed to create order in MySklad: {response.text}")
            return False


async def extract_daribar_order_number_from_description(description: str) -> str:
    logger.info(f"Extracting Daribar order number from description: {description}")
    if "Заказ №" in description:
        return description.split("Заказ №")[1].strip()
    return None



SKU_MAPPING = {
    "116864": "Паста Руккола-Креветки с пармезаном",
    "116869": "Паста АльФредо с курицей и грибами в сливочном соусе",
    "116963": "Брокколи-морковка с пастой арабьята, гратен с фокаччей",
    "116866": "Чикен-слайсы с пастой арабьята и сыром, гратен с фокаччей",
    "116867": "Чикен-слайсы с картофелем, грибной соус",
    "116871": "Куриный стейк, рис с мексиканской смесью и соусом чипотле",
    "116873": "Курица вок в кислосладком соусе",
    "116875": "Куриные котлетки, гречка с овощным миксом, грибной соус",
    "116970": "Острые куриные котлетки с рисом по-тайски, лепешка чапати, соус чимичурри",
    "116972": "Куриные стрипсы с картофелем фри и кисло сладким соусом",
    "116974": "Куриный стейк, гречка с овощным миксом, грибной соус",
    "116975": "Курица карри с рисом по-индийски",
    "116965": "Чизбургер с сыром, говяжьей котлетой, картошкой фри и фирменным соусом Tasty",
    "116962": "Бургер с картофельным хэшбрауном, овощами гриль и фирменным соусом Tasty",
    "116976": "Чикенбургер с сыром, куриной котлетой, картошкой фри и фирменным соусом Tasty",
    "116961": "Салат Цезарь",
    "116969": "Салат Оливье",
    "116860": "Том Ям с курицей и грибами",
    "116868": "Плов с говядиной",
    "116964": "Картофельные драники с фирменным соусом Tasty",
    "116966": "Французский гратен с овощами гриль с фирменным соусом Tasty",
    "116856": "Фито-сырники с клубничным джемом и шоколадным соусом",
    "116857": "Фито-сырники с карамельным соусом и яблочным джемом",
    "116858": "Блинчики с клубничным джемом и шоколадным соусом",
    "116859": "Блинчики с яблочным джемом и карамельным соусом",
    "116967": "Рисовая каша с карамелью и яблочным джемом",
    "116968": "Рисовая каша с клубничным джемом и шоколадным соусом",
    "116861": "Чизкейк с клубничным джемом",
    "116862": "Чизкейк с карамельным соусом",
    "116863": "Чизкейк (ПФ)"
}


@app.get("/export_orders")
async def export_orders():
    headers = await get_daribar_headers()
    url = f"{BASE_URL_DARIBAR}/public/api/v2/orders?limit=2000"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers)
            if response.status_code == 401:
                await refresh_daribar_token()
                headers = await get_daribar_headers()
                response = await client.get(url, headers=headers)

            response.raise_for_status()  # Проверка на успешный статус ответа

            orders_data = response.json()
            orders = orders_data.get("result", [])

            filtered_orders = [
                order for order in orders if order.get("pharmacy_status") == "InPharmacyReady"
            ]

            if not filtered_orders:
                raise HTTPException(status_code=404, detail="No orders found with status 'InPharmacyReady'")

            order_items = []
            for order in filtered_orders:
                items = order.get("items", [])
                for item in items:
                    sku = item.get("sku")
                    name = SKU_MAPPING.get(sku, "Unknown")
                    quantity = item.get("quantity")
                    order_items.append({"Наименование": name, "Количество": quantity})

            df = pd.DataFrame(order_items)

            bio = io.BytesIO()
            with pd.ExcelWriter(bio, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False)
            bio.seek(0)

            filename = "orders_export.xlsx"

            headers = {
                'Content-Disposition': f'attachment; filename="{filename}"',
                'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            }

            return StreamingResponse(bio, headers=headers)
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error occurred: {e.response.status_code} - {e.response.text}")
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


@app.on_event("startup")
async def startup_event():
    logger.info("Starting application...")
    await initialize_tokens()
    logger.info(f"token on start: {DARIBAR_ACCESS_TOKEN}")
    if not DARIBAR_ACCESS_TOKEN or not DARIBAR_REFRESH_TOKEN:
        await refresh_daribar_token()
    logger.info("Application startup complete and listening on port 8000")


@app.get("/")
async def read_root():
    return {"message": "Hello, World!"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)