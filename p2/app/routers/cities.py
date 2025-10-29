from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
import time

from app.dependencies import get_db, get_cache
from app.cache import get_cached, set_cached
from app.kafka import send_message
from app.core import settings
from app import models
from app import schemas

router = APIRouter()

HIT_COUNT_KEY = "cache_hit_count"
TOTAL_COUNT_KEY = "cache_total_count"


router = APIRouter(
    prefix="/cities",
    tags=["cities"],
    responses={404: {"description": "Not found"}},
)


@router.post("/", response_model=schemas.City)
async def upsert_country_code(
    city_data: schemas.CityCreate,
    db: Session = Depends(get_db),
    cache=Depends(get_cache),
):
    db_city = db.query(models.City).filter(models.City.city == city_data.city).first()
    if db_city:
        db_city.country_code = city_data.country_code
    else:
        db_city = models.City(city=city_data.city, country_code=city_data.country_code)
        db.add(db_city)

    db.commit()
    db.refresh(db_city)
    # NEW: Update the cache after upsert
    await set_cached(cache, db_city.city, db_city.country_code)
    return db_city


@router.get("/country-code/{city}")
async def get_country_code(
    city: str, db: Session = Depends(get_db), cache=Depends(get_cache)
):
    start_time = time.perf_counter()
    await cache.incr(TOTAL_COUNT_KEY)

    cached_record = await get_cached(cache, city)
    if cached_record:
        await cache.incr(HIT_COUNT_KEY)

        total = int(await cache.get(TOTAL_COUNT_KEY) or 1)
        hits = int(await cache.get(HIT_COUNT_KEY) or 0)
        hit_percentage = round(hits / total * 100, 2)

        duration = time.perf_counter() - start_time
        await send_message(
            topic=settings.KAFKA_TOPIC,
            message={
                "city": city,
                "event": "hit",
                "duration_ms": int(duration * 1000),
                "hit_percentage": hit_percentage,
            },
        )
        return {"country_code": cached_record, "hit_percentage": hit_percentage}

    db_city = db.query(models.City).filter(models.City.city == city).first()
    if not db_city:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="City not found"
        )

    await set_cached(cache, city, db_city.country_code)

    total = int(await cache.get(TOTAL_COUNT_KEY) or 1)
    hits = int(await cache.get(HIT_COUNT_KEY) or 0)
    hit_percentage = round(hits / total * 100, 2)

    duration = time.perf_counter() - start_time
    await send_message(
        topic=settings.KAFKA_TOPIC,
        message={
            "city": city,
            "event": "miss",
            "duration_ms": int(duration * 1000),
            "hit_percentage": hit_percentage,
        },
    )
    return {"country_code": db_city.country_code, "hit_percentage": hit_percentage}
