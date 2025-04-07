import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

CLIENT_SECRET: str = os.getenv("TWITCH_BOT_APP_CLIENT_SECRET")
BOT_ID: str = os.getenv("TWITCH_BOT_ID")
OWNER_ID: str = os.getenv("TWITCH_OWNER_ID")
CLIENT_ID: str = os.getenv("TWITCH_BOT_APP_CLIENT_ID")

BASE_DIR = Path(__file__).parent

COMPONENTS_DIRECTORY = BASE_DIR / "components"
DATA_PATH = BASE_DIR / "data"
TOKENS_DATABASE_PATH = DATA_PATH / "tokens.db"
