import os
from datetime import datetime

from atproto import Client
from dotenv import load_dotenv
from pytz import timezone
from xata import XataClient

load_dotenv()

XATA_API_KEY = os.getenv("XATA_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

BLUESKY_LOGIN = os.getenv("BLUESKY_LOGIN")
BLUESKY_APP_PASSWORD = os.getenv("BLUESKY_APP_PASSWORD")

at_client = Client()
at_client.login(BLUESKY_LOGIN, BLUESKY_APP_PASSWORD)

client = XataClient(api_key=XATA_API_KEY, db_url=DATABASE_URL)

# users_table_schema = {
#     "columns": [
#         {"name": "username", "type": "string", "unique": True},
#         {"name": "birthday", "type": "datetime", "notNull": False},
#     ],
# }

# assert client.table().create("users").is_success()

# resp = client.table().set_schema("users", users_table_schema)

# assert resp.is_success(), resp

# date_with_tz = (
#     datetime.strptime("2024-10-21 00:00:00", "%Y-%m-%d %H:%M:%S")
#     .astimezone(timezone("Australia/Melbourne"))
#     .isoformat()
# )

# Get profile's posts with pagination, filter by author handle, and sort by indexed_at
posts = sorted(
    [
        feed.post
        for feed in at_client.get_author_feed(actor=BLUESKY_LOGIN).feed
        if feed.post.author.handle == BLUESKY_LOGIN
    ],
    key=lambda post: post.indexed_at,
)

# Build a list with each post's id, date, and URL
posts = [
    {
        "id": post.uri.split("/")[-1],
        "date": post.indexed_at,
        "url": f"https://fxbsky.app/profile/valinmalach.bsky.social/post/{post.uri.split('/')[-1]}",
    }
    for post in posts
]

for record in posts:
    record_id = record.pop("id")
    resp = client.records().insert_with_id("bluesky", record_id, record)
