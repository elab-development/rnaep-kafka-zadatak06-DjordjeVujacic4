from fastapi import FastAPI
from typing import List
from models import Notification
from aiokafka import AIOKafkaConsumer
from contextlib import asynccontextmanager
import asyncio, json

@asynccontextmanager
async def lifespan(app: FastAPI):
    confirmed_consumer = AIOKafkaConsumer(
        "order-confirmed",
        bootstrap_servers='kafka:9092',
        group_id="notifications-group",
        auto_offset_reset="earliest"
    )
    not_found_consumer = AIOKafkaConsumer(
        "product_not_found_events",
        bootstrap_servers='kafka:9092',
        group_id="notifications-group",
        auto_offset_reset="earliest"
    )
    out_of_stock_consumer = AIOKafkaConsumer(
        "out_of_stock_events",
        bootstrap_servers='kafka:9092',
        group_id="notifications-group",
        auto_offset_reset="earliest"
    )

    await confirmed_consumer.start()
    await not_found_consumer.start()
    await out_of_stock_consumer.start()

    tasks = [
        asyncio.create_task(consume_confirmed(confirmed_consumer)),
        asyncio.create_task(consume_error(not_found_consumer, "Proizvod ne postoji u katalogu")),
        asyncio.create_task(consume_error(out_of_stock_consumer, "Nedovoljna količina na stanju")),
    ]

    yield

    for task in tasks:
        task.cancel()
    await confirmed_consumer.stop()
    await not_found_consumer.stop()
    await out_of_stock_consumer.stop()

app = FastAPI(title="Notifications Service", lifespan=lifespan)

notifications_db: List[Notification] = []

async def consume_confirmed(consumer: AIOKafkaConsumer):
    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode('utf-8'))
            notification = Notification(
                order_id=data['order_id'],
                product_id=data['product_id'],
                message=f"Order {data['order_id']} for product {data['product_id']} has been placed."
            )
            notifications_db.append(notification)
            print(f"[NOTIFICATION] {notification.message}", flush=True)
    except asyncio.CancelledError:
        pass

async def consume_error(consumer: AIOKafkaConsumer, default_reason: str):
    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode('utf-8'))
            reason = data.get('error_reason', default_reason)
            notification = Notification(
                order_id=data['order_id'],
                product_id=data['product_id'],
                message=f"Order {data['order_id']} odbijena: {reason} (product_id={data['product_id']})."
            )
            notifications_db.append(notification)
            print(f"[NOTIFICATION] {notification.message}", flush=True)
    except asyncio.CancelledError:
        pass

@app.get("/notifications", response_model=List[Notification])
def get_notifications():
    return notifications_db
