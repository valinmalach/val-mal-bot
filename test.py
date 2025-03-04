# import json
# import os

# from atproto import Client
# from dotenv import load_dotenv
# from xata import XataClient

# load_dotenv()

# BLUESKY_LOGIN = os.getenv("BLUESKY_LOGIN")
# BLUESKY_APP_PASSWORD = os.getenv("BLUESKY_APP_PASSWORD")

# at_client = Client()
# at_client.login(BLUESKY_LOGIN, BLUESKY_APP_PASSWORD)

# XATA_API_KEY = os.getenv("XATA_API_KEY")
# DATABASE_URL = os.getenv("DATABASE_URL")

# xata_client = XataClient(api_key=XATA_API_KEY, db_url=DATABASE_URL)


# last_sync_date_time = xata_client.data().query(
#     "bluesky",
#     {"columns": ["date"], "sort": {"date": "desc"}, "page": {"size": 1}},
# )["records"][0]["date"]

# # Get all posts, filter by author handle and last sync, and sort by indexed_at
# posts = sorted(
#     [
#         feed.post
#         for feed in at_client.get_author_feed(actor=BLUESKY_LOGIN).feed
#         if feed.post.author.handle == BLUESKY_LOGIN and feed.post.indexed_at > last_sync_date_time
#     ],
#     key=lambda post: post.indexed_at,
# )

# # Build a list with each post's id, date, and URL
# posts = [
#     {
#         "id": post.uri.split("/")[-1],
#         "date": post.indexed_at,
#         "url": f"https://fxbsky.app/profile/valinmalach.bsky.social/post/{post.uri.split('/')[-1]}",
#     }
#     for post in posts
# ]

# print(posts)

import datetime

now = (
    datetime.datetime.now(datetime.timezone.utc)
    .replace(second=0, microsecond=0, tzinfo=None)
    .isoformat()
) + ".000Z"
print(now)
