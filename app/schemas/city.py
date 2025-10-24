from pydantic import BaseModel


class CityBase(BaseModel):
    city: str
    country_code: str


class CityCreate(CityBase):
    pass


class City(CityBase):
    id: int

    class Config:
        from_attributes = True
