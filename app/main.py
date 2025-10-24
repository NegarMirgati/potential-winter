# app/main.py
import asyncio
from asyncio import create_task
from fastapi import FastAPI

from app.routers import users, cities
from app.cache.client import init_redis, close_redis
from app.kafka.producer import init_kafka, close_kafka

from app.core.config import settings
from app.db.session import engine
from app.db.base import Base
from app.seeder import loader

from app.db.utils import wait_for_db


app = FastAPI(title=settings.PROJECT_NAME, version=settings.PROJECT_VERSION)


async def init_kafka_safe():
    try:
        await init_kafka()
        print("Kafka connected!")
    except Exception as e:
        print(f"Kafka init failed: {e}")


def create_tables_and_seed():
    """
    Blocking function executed in a thread executor:
    - assumes DB is reachable
    - creates tables and runs seeder (synchronous)
    """
    Base.metadata.create_all(bind=engine)
    loader.seed_cities()


# Startup/shutdown events
@app.on_event("startup")
async def startup_event():
    loop = asyncio.get_running_loop()

    try:
        await loop.run_in_executor(None, lambda: wait_for_db(retries=30, delay=1.0))
    except Exception as exc:
        print("ERROR: Database did not become ready:", exc)
        raise

    try:
        await loop.run_in_executor(None, create_tables_and_seed)
        print("DB tables created and seeding complete.")
    except Exception as exc:
        print("ERROR during create/seed:", exc)
        raise

    init_redis(app)

    create_task(init_kafka_safe())


@app.on_event("shutdown")
async def shutdown_event():
    await close_redis(app)
    await close_kafka()


@app.get("/")
async def read_root():
    return {"message": "Welcome to fastAPI_project_template API"}


# routers
app.include_router(users.router)
app.include_router(cities.router)
