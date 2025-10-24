import json
from aiokafka import AIOKafkaProducer
from app.core.config import settings

kafka_producer: AIOKafkaProducer | None = None


async def init_kafka():
    global kafka_producer
    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await kafka_producer.start()


async def send_message(topic: str, message: str):
    if kafka_producer:
        await kafka_producer.send_and_wait(topic, message)


async def close_kafka() -> None:
    if kafka_producer:
        await kafka_producer.stop()
