import asyncio
import datetime
import os

from atproto import Client
from discord.ext import tasks
from discord.ext.commands import Bot, Cog
from dotenv import load_dotenv
from requests.exceptions import ConnectionError
from xata import XataClient
from xata.api_response import ApiResponse

from constants import (
    BLUESKY_CHANNEL,
    BLUESKY_ROLE,
    BOT_ADMIN_CHANNEL,
    SHOUTOUTS_CHANNEL,
)
from helper import get_next_leap, send_message, update_birthday

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
        try:
            try:
                last_sync_date_time = xata_client.data().query(
                    "bluesky",
                    {
                        "columns": ["date"],
                        "sort": {"date": "desc"},
                        "page": {"size": 1},
                    },
                )["records"][0]["date"]
            except ConnectionError as e:
                await send_message(
                    f"Failed to get last sync date time: {e}",
                    self.bot,
                    BOT_ADMIN_CHANNEL,
                )
                return

            try:
                author_feed = at_client.get_author_feed(actor=BLUESKY_LOGIN)
            except Exception as e:
                await send_message(
                    f"Failed to get author feed: {e}",
                    self.bot,
                    BOT_ADMIN_CHANNEL,
                )
                return

            posts = sorted(
                [
                    feed.post
                    for feed in author_feed.feed
                    if feed.post.author.handle == BLUESKY_LOGIN
                    and feed.post.indexed_at > last_sync_date_time
                ],
                key=lambda post: post.indexed_at,
            )

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
                try:
                    resp = xata_client.records().upsert("bluesky", post_id, post)
                    if resp.is_success():
                        await send_message(
                            f"<@&{BLUESKY_ROLE}>\n\n{post['url']}",
                            self.bot,
                            BLUESKY_CHANNEL,
                        )
                    else:
                        await send_message(
                            f"Failed to insert post {post_id} into database.",
                            self.bot,
                            BOT_ADMIN_CHANNEL,
                        )
                except Exception as e:
                    await send_message(
                        f"Failed to insert post {post_id} into database: {e}",
                        self.bot,
                        BOT_ADMIN_CHANNEL,
                    )
        except Exception as e:
            await send_message(
                f"Fatal error with Bluesky posts check: {e}",
                self.bot,
                BOT_ADMIN_CHANNEL,
            )

    @tasks.loop(time=_quarter_hours)
    async def check_birthdays(self):
        try:
            now = (
                datetime.datetime.now(datetime.timezone.utc)
                .replace(second=0, microsecond=0)
                .strftime("%Y-%m-%dT%H:%M:%S.000Z")
            )

            while True:
                try:
                    records = xata_client.data().query(
                        "users", {"filter": {"birthday": now}}
                    )
                    await self._process_birthday_records(records)

                    while records.has_more_results():
                        records = xata_client.data().query(
                            "users",
                            {
                                "filter": {"birthday": now},
                                "page": {"after": records.get_cursor()},
                            },
                        )
                        await self._process_birthday_records(records)
                    break
                except ConnectionError as e:
                    await asyncio.sleep(60)
        except Exception as e:
            await send_message(
                f"Fatal error with birthday check: {e}",
                self.bot,
                BOT_ADMIN_CHANNEL,
            )

    async def _process_birthday_records(self, records: ApiResponse):
        now = datetime.datetime.now()
        birthdays_now = records["records"]
        for record in birthdays_now:
            user_id = record["id"]
            user = self.bot.get_user(int(user_id))
            await send_message(
                f"Happy Birthday {user.mention}!",
                self.bot,
                SHOUTOUTS_CHANNEL,
            )
            if record["isBirthdayLeap"]:
                leap = True
                next_birthday = f"{get_next_leap(now.year)}{record['birthday'][4:]}"
            else:
                leap = False
                next_birthday = f"{now.year + 1}{record['birthday'][4:]}"
            updated_record = {
                "username": record["username"],
                "birthday": next_birthday,
                "isBirthdayLeap": leap,
            }
            success = update_birthday(xata_client, user_id, updated_record)
            if not success[0]:
                await send_message(
                    f"Failed to update birthday for {updated_record['username']}: {success[1]}",
                    self.bot,
                    BOT_ADMIN_CHANNEL,
                )


async def setup(bot: Bot):
    await bot.add_cog(Tasks(bot))
