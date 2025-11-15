from pydantic import BaseModel, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    APCA_API_KEY_ID: str | None = None
    APCA_API_SECRET_KEY: str | None = None
    APCA_API_BASE_URL: str = "https://paper-api.alpaca.markets"
    APCA_DATA_FEED: str = "iex"


    DRY_RUN: bool = True
    TIMEZONE: str = "Europe/Warsaw"


    DEFAULT_TIF: str = "day"
    ORDER_CLIENT_PREFIX: str = "alpaca777"


    RISK_MAX_POSITION_PCT: float = 0.2
    COMMISSION_PCT: float = 0.0005


    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


    @field_validator("DEFAULT_TIF")
    @classmethod
    def _tif(cls, v: str) -> str:
        allowed = {"day", "gtc"}
        if v.lower() not in allowed:
            raise ValueError("DEFAULT_TIF must be 'day' or 'gtc'")
        return v.lower()


settings = Settings()