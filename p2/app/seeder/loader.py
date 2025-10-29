# scripts/seed_db.py
import os
import csv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.models import City, Base
from app.core.config import settings


engine = create_engine(settings.DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)


def seed_cities(csv_path="app/data/CountryCode-City.csv"):
    session = SessionLocal()
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            # Keep only the last occurrence in the CSV
            latest_cities = {}
            for row in reader:
                city_name = row["city"].strip()
                country_code = row["countyCode"].strip()
                key = (city_name.lower(), country_code.upper())
                latest_cities[key] = City(city=city_name, country_code=country_code)

            count = 0
            for city_obj in latest_cities.values():
                existing = session.query(City).filter_by(city=city_obj.city).first()
                if existing:
                    pass
                else:
                    session.add(city_obj)
                count += 1

            session.commit()
            print(f"Processed {count} cities (duplicates overwritten).")

    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    seed_cities()
