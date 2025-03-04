import datetime
import os

from atproto import Client
from discord.ext import commands, tasks
from dotenv import load_dotenv
from xata import XataClient

from send_discord_message import send_discord_message

load_dotenv()

BLUESKY_LOGIN = os.getenv("BLUESKY_LOGIN")
BLUESKY_APP_PASSWORD = os.getenv("BLUESKY_APP_PASSWORD")

at_client = Client()
at_client.login(BLUESKY_LOGIN, BLUESKY_APP_PASSWORD)

XATA_API_KEY = os.getenv("XATA_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

xata_client = XataClient(api_key=XATA_API_KEY, db_url=DATABASE_URL)


class Tasks(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.check_posts.start()
        self.check_birthdays.start()

    _quarter_hours = [
        datetime.time(hour, minute)
        for hour in range(24)
        for minute in (0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55)
    ]

    @tasks.loop(minutes=1)
    async def check_posts(self):
        last_sync_date_time = xata_client.data().query(
            "bluesky",
            {"columns": ["date"], "sort": {"date": "desc"}, "page": {"size": 1}},
        )["records"][0]["date"]

        # Get all posts, filter by author handle and last sync, and sort by indexed_at
        posts = sorted(
            [
                feed.post
                for feed in at_client.get_author_feed(actor=BLUESKY_LOGIN).feed
                if feed.post.author.handle == BLUESKY_LOGIN
                and feed.post.indexed_at > last_sync_date_time
            ],
            key=lambda post: post.indexed_at,
        )

        # Build a list with each post's id, date, and URL
        posts = [
            {
                "id": post.uri.split("/")[-1],
                "date": post.indexed_at,
                "url": f"https://bsky.app/profile/valinmalach.bsky.social/post/{post.uri.split('/')[-1]}",
            }
            for post in posts
        ]

        for post in posts:
            post_id = post.pop("id")
            resp = xata_client.records().insert_with_id("bluesky", post_id, post)
            if resp.is_success():
                await send_discord_message(
                    f"<@&1345584502805626973>\n\n{post["url"]}",
                    self.bot,
                    1345582916050354369,  # bluesky announcement channel
                )
            else:
                print(f"Failed to insert post {post_id}.")

    @tasks.loop(time=_quarter_hours)
    async def check_birthdays(self):
        now = (
            datetime.datetime.now(datetime.timezone.utc)
            .replace(year=1970, second=0, microsecond=0, tzinfo=None)
            .isoformat()
            + ".000Z"
        )
        print(now)

        records = xata_client.data().query(
            "users",
            {"columns": ["id", "birthday"], "filter": {"birthday": now}},
        )

        birthdays_now = records["records"]
        for record in birthdays_now:
            user_id = record["id"]
            await send_discord_message(
                f"Happy Birthday <@{user_id}>!",
                self.bot,
                1291026077287710751,  # shoutouts channel
            )

        while records.has_more_results():
            records = xata_client.data().query(
                "users",
                {
                    "columns": ["id", "birthday"],
                    "filter": {"birthday": now},
                    "page": {"after": records.get_cursor()},
                },
            )

            birthdays_now = records["records"]
            for record in birthdays_now:
                user_id = record["id"]
                await send_discord_message(
                    f"Happy Birthday <@{user_id}>!",
                    self.bot,
                    1291026077287710751,  # shoutouts channel
                )


async def setup(bot: commands.Bot):
    await bot.add_cog(Tasks(bot))
