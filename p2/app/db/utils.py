# app/db/utils.py
import time
import psycopg2
from psycopg2 import OperationalError
from app.core.config import settings


def wait_for_db(retries: int = 15, delay: float = 1.0) -> None:
    """
    Block until Postgres accepts connections or raise OperationalError.
    Call this from startup if you need DB ready for seeding/migrations.
    """
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                dbname=settings.POSTGRES_DB,
                user=settings.POSTGRES_USER,
                password=settings.POSTGRES_PASSWORD,
                host=settings.POSTGRES_SERVER,
                port=settings.POSTGRES_PORT,
                connect_timeout=5,
            )
            conn.close()
            return
        except OperationalError as exc:
            last_exc = exc
            if attempt == retries:
                raise
            print(f"Postgres not ready ({attempt}/{retries}); sleeping {delay}s...")
            time.sleep(delay)
    if last_exc:
        raise last_exc
