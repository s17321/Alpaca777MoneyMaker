# src/config/settings.py
from dotenv import load_dotenv
from pathlib import Path
import os

# wczytaj .env, jeśli istnieje
load_dotenv(dotenv_path=Path(__file__).resolve().parents[2] / ".env")

class Settings:
    API_KEY: str = os.getenv("ALPACA_API_KEY", "")
    SECRET_KEY: str = os.getenv("ALPACA_SECRET_KEY", "")
    BASE_URL: str = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

settings = Settings()
