import os

from dotenv import load_dotenv
from xata import XataClient

load_dotenv()

API_KEY = os.getenv("XATA_API_KEY")
DB_URL = os.getenv("DATABASE_URL")

if not API_KEY or not DB_URL:
    raise ValueError(
        "XATA_API_KEY and DATABASE_URL must be set in the environment variables."
    )

xata_client = XataClient(api_key=API_KEY, db_url=DB_URL)
