from sqlalchemy import Column, Integer, String
from app.db.base import Base


class City(Base):
    __tablename__ = "cities"
    id = Column(Integer, primary_key=True, index=True)
    city = Column(String, primary_key=False, index=True, unique=True)
    country_code = Column(String, primary_key=False, index=False)
