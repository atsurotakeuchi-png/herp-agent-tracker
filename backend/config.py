import os
from dotenv import load_dotenv

load_dotenv()

HERP_API_TOKEN = os.getenv("HERP_API_TOKEN", "")
APP_PASSWORD = os.getenv("APP_PASSWORD", "blued2026")
SECRET_KEY = os.getenv("SECRET_KEY", "change-me")
_default_db = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "herp_tracker.db")
DB_PATH = os.getenv("DB_PATH", _default_db)
HERP_BASE_URL = "https://public-api.herp.cloud/hire/v1"
