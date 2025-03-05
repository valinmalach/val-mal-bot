import datetime
import os

from atproto import Client
from discord.ext import tasks
from discord.ext.commands import Bot, Cog
from dotenv import load_dotenv
from xata import XataClient
from xata.api_response import ApiResponse

from send_discord_message import send_discord_message

load_dotenv()

BLUESKY_LOGIN = os.getenv("BLUESKY_LOGIN")
BLUESKY_APP_PASSWORD = os.getenv("BLUESKY_APP_PASSWORD")

at_client = Client()
at_client.login(BLUESKY_LOGIN, BLUESKY_APP_PASSWORD)

XATA_API_KEY = os.getenv("XATA_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

xata_client = XataClient(api_key=XATA_API_KEY, db_url=DATABASE_URL)


class Tasks(Cog):
    def __init__(self, bot: Bot):
        self.bot = bot
        self.check_posts.start()
        self.check_birthdays.start()

    _quarter_hours = [
        datetime.time(hour, minute) for hour in range(24) for minute in (0, 15, 30, 45)
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
                    f"<@&1345584502805626973>\n\n{post['url']}",
                    self.bot,
                    1345582916050354369,  # bluesky announcement channel
                )
            else:
                await send_discord_message(
                    f"Failed to insert post {post_id} into database.",
                    self.bot,
                    1346408909442781237,  # bot-admin channel
                )

    @tasks.loop(time=_quarter_hours)
    async def check_birthdays(self):
        now = (
            datetime.datetime.now(datetime.timezone.utc)
            .replace(second=0, microsecond=0)
            .strftime("%Y-%m-%dT%H:%M:%S.000Z")
        )

        # TODO: If this year is not a leap year, and it is February 28th or March 1st, get all users with birthdays on February 29th on the next leap year as well
        records = xata_client.data().query(
            "users",
            {"columns": ["id", "birthday"], "filter": {"birthday": now}},
        )
        await self._process_birthday_records(records)

        while records.has_more_results():
            records = xata_client.data().query(
                "users",
                {
                    "columns": ["id", "birthday"],
                    "filter": {"birthday": now},
                    "page": {"after": records.get_cursor()},
                },
            )
            await self._process_birthday_records(records)

    async def _process_birthday_records(self, records: ApiResponse):
        birthdays_now = records["records"]
        for record in birthdays_now:
            user_id = record["id"]
            await send_discord_message(
                f"Happy Birthday <@{user_id}>!",
                self.bot,
                1291026077287710751,  # shoutouts channel
            )
            # TODO: Update user's birthday entry to next year
            # TODO: If user's birthday is on February 29th and it is not a leap year, do not update the birthday entry
            # TODO: If user's birthday is on February 29th and it is a leap year, update the birthday entry to the next leap year

    def _is_leap_year(self, year: int) -> bool:
        return (year % 400 == 0) or (year % 100 != 0) and (year % 4 == 0)


async def setup(bot: Bot):
    await bot.add_cog(Tasks(bot))
