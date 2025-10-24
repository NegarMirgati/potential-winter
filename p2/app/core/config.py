import os
from dotenv import load_dotenv

load_dotenv()


class Settings:

    # --- Project info ---
    PROJECT_NAME: str = os.getenv("PROJECT_NAME", "fastAPI_project_template")
    PROJECT_VERSION: str = os.getenv("PROJECT_VERSION", "1.0.0")

    # --- Postgres ---
    POSTGRES_USER: str = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_SERVER: str = os.getenv("POSTGRES_SERVER", "postgres")
    POSTGRES_PORT: str = os.getenv("POSTGRES_PORT", 5432)
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "city_db")
    DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}"

    # --- Redis ---
    REDIS_HOST: str = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", 6379))
    REDIS_DB: int = int(os.getenv("REDIS_DB", 0))
    REDIS_PASSWORD: str | None = os.getenv("REDIS_PASSWORD")
    REDIS_PREFIX: str = os.getenv("REDIS_PREFIX", "myapp:")
    REDIS_DEFAULT_TTL: int = int(os.getenv("REDIS_DEFAULT_TTL", 3600))

    @property
    def REDIS_URL(self) -> str:
        """Builds a redis:// or rediss:// DSN dynamically."""
        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"

    # --- Kafka ---
    KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "city_requests")
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


settings = Settings()
