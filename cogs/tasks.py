import logging
import os
import shutil
import traceback

import httpx
import pendulum
import polars as pl
import sentry_sdk
from discord.ext import tasks
from discord.ext.commands import Bot, Cog
from dotenv import load_dotenv
from polars import DataFrame

from constants import (
    BLUESKY_CHANNEL,
    BLUESKY_ROLE,
    BOT_ADMIN_CHANNEL,
    SHOUTOUTS_CHANNEL,
)
from init import at_client
from services import get_next_leap, send_message, update_birthday, upsert_row_to_parquet

load_dotenv()

APP_URL = os.getenv("APP_URL")

logger = logging.getLogger(__name__)


class Tasks(Cog):
    def __init__(self, bot: Bot) -> None:
        self.bot = bot

    @Cog.listener()
    async def on_ready(self) -> None:
        if not self.check_posts.is_running():
            self.check_posts.start()
        if not self.check_birthdays.is_running():
            self.check_birthdays.start()
        if not self.renew_youtube_webhook_subscription.is_running():
            self.renew_youtube_webhook_subscription.start()
        if not self.backup_data.is_running():
            self.backup_data.start()

    _quarter_hours = [
        pendulum.Time(hour, minute) for hour in range(24) for minute in (0, 15, 30, 45)
    ]

    @tasks.loop(hours=24)
    @sentry_sdk.trace()
    async def renew_youtube_webhook_subscription(self) -> None:
        logger.info("Renewing YouTube webhook subscription")
        try:
            YOUTUBE_CHANNEL_ID = "UC7BVlWSXIU4hKtWkBqEgZMA"
            CALLBACK_URL = f"{APP_URL}/youtube/webhook"

            response = httpx.post(
                "https://pubsubhubbub.appspot.com/subscribe",
                data={
                    "hub.mode": "subscribe",
                    "hub.callback": CALLBACK_URL,
                    "hub.topic": f"https://www.youtube.com/xml/feeds/videos.xml?channel_id={YOUTUBE_CHANNEL_ID}",
                    "hub.verify": "async",
                    "hub.lease_seconds": str(864000),  # 10 days
                },
            )

            if response.status_code == 202:
                logger.info("YouTube webhook subscription renewed successfully")
            else:
                logger.error(
                    f"Failed to renew YouTube webhook subscription: {response.status_code} - {response.text}"
                )
                await send_message(
                    f"Failed to renew YouTube webhook subscription: {response.status_code} - {response.text}",
                    BOT_ADMIN_CHANNEL,
                )
        except Exception as e:
            logger.error(f"Exception during YouTube webhook renewal: {e}")
            sentry_sdk.capture_exception(e)
            await send_message(
                f"Exception during YouTube webhook renewal: {e}",
                BOT_ADMIN_CHANNEL,
            )

    @tasks.loop(minutes=1)
    @sentry_sdk.trace()
    async def check_posts(self) -> None:
        try:
            df = pl.read_parquet("data/bluesky.parquet")
            last_sync_date_time = (
                "1970-01-01T00:00:00.000Z" if df.height == 0 else str(df["date"].max())
            )

            try:
                author_feed = at_client.get_author_feed(actor="valinmalach.bsky.social")
            except Exception as e:
                error_details = {
                    "type": type(e).__name__,
                    "message": str(e),
                    "args": e.args,
                    "traceback": traceback.format_exc(),
                }
                error_msg = f"Error fetching Bluesky author feed - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
                logger.warning(f"{error_msg}\nTraceback: {error_details['traceback']}")
                await send_message(
                    error_msg,
                    BOT_ADMIN_CHANNEL,
                )
                return

            posts = sorted(
                [
                    feed.post
                    for feed in author_feed.feed
                    if feed.post.author.handle == "valinmalach.bsky.social"
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
                try:
                    success, error = upsert_row_to_parquet(post, "data/bluesky.parquet")
                    if success:
                        await send_message(
                            f"<@&{BLUESKY_ROLE}>\n\n{post['url']}",
                            BLUESKY_CHANNEL,
                        )
                    else:
                        logger.warning(
                            f"Failed to insert post {post['id']} into parquet: {error}",
                        )
                        await send_message(
                            f"Failed to insert post {post['id']} into parquet: {error}",
                            BOT_ADMIN_CHANNEL,
                        )
                except Exception as e:
                    logger.error(f"Exception upserting Bluesky post {post['id']}: {e}")
                    sentry_sdk.capture_exception(e)
                    await send_message(
                        f"Failed to insert post {post['id']} into parquet: {e}",
                        BOT_ADMIN_CHANNEL,
                    )
        except Exception as e:
            logger.error(f"Fatal error during Bluesky posts sync: {e}")
            sentry_sdk.capture_exception(e)
            await send_message(
                f"Fatal error with Bluesky posts check: {e}",
                BOT_ADMIN_CHANNEL,
            )

    @tasks.loop(time=pendulum.Time(0, 0, 0, 0))
    @sentry_sdk.trace()
    async def backup_data(self) -> None:
        try:
            date_string = pendulum.now().format("YYYY-MM-DD")
            backup_path = f"C:/backups/data_{date_string}/"

            if not os.path.exists(backup_path):
                os.makedirs(backup_path)

            for item in os.listdir("data/"):
                source_path = os.path.join("data/", item)
                dest_path = os.path.join(backup_path, item)
                try:
                    if os.path.isdir(source_path):
                        shutil.copytree(source_path, dest_path, dirs_exist_ok=True)
                    else:
                        shutil.copy2(source_path, dest_path)
                except Exception as e:
                    is_dir = os.path.isdir(source_path)
                    logger.error(
                        f"Error backing up {'directory' if is_dir else 'file'} {item}: {e}"
                    )
                    sentry_sdk.capture_exception(e)
                    await send_message(
                        f"Error backing up {'directory' if is_dir else 'file'} {item}: {e}",
                        BOT_ADMIN_CHANNEL,
                    )
        except Exception as e:
            logger.error(f"Fatal error during backup data task: {e}")
            sentry_sdk.capture_exception(e)
            await send_message(
                f"Fatal error with backup data task: {e}",
                BOT_ADMIN_CHANNEL,
            )

    @tasks.loop(time=_quarter_hours)
    @sentry_sdk.trace()
    async def check_birthdays(self) -> None:
        try:
            now = (
                pendulum.now("UTC")
                .replace(second=0, microsecond=0)
                .strftime("%Y-%m-%dT%H:%M:%S.000Z")
            )

            df = pl.read_parquet("data/users.parquet")
            birthday_users = df.filter(pl.col("birthday") == now)
            await self._process_birthday_records(birthday_users)
        except Exception as e:
            logger.error(f"Fatal error during birthday check task: {e}")
            sentry_sdk.capture_exception(e)
            await send_message(
                f"Fatal error with birthday check: {e}",
                BOT_ADMIN_CHANNEL,
            )

    @sentry_sdk.trace()
    async def _process_birthday_records(self, birthdays_now: DataFrame) -> None:
        now = pendulum.now()
        for record in birthdays_now.iter_rows(named=True):
            user_id = record["id"]
            user = self.bot.get_user(int(user_id))
            if user is None:
                logger.warning(f"Discord user ID {user_id} not found in guild cache")
                sentry_sdk.capture_message(f"User with ID {user_id} not found.")
                await send_message(
                    f"_process_birthday_records: User with ID {user_id} not found.",
                    BOT_ADMIN_CHANNEL,
                )
                continue
            await send_message(
                f"Happy Birthday {user.mention}!",
                SHOUTOUTS_CHANNEL,
            )
            if record["isBirthdayLeap"]:
                leap = True
                next_birthday = f"{get_next_leap(now.year)}{record['birthday'][4:]}"
            else:
                leap = False
                next_birthday = f"{now.year + 1}{record['birthday'][4:]}"
            updated_record = {
                "id": user_id,
                "username": record["username"],
                "birthday": next_birthday,
                "isBirthdayLeap": leap,
            }
            success, error = update_birthday(updated_record)
            if not success:
                logger.error(f"Failed to update birthday for user: {error}")
                await send_message(
                    f"Failed to update birthday for {updated_record['username']}: {error}",
                    BOT_ADMIN_CHANNEL,
                )


async def setup(bot: Bot) -> None:
    await bot.add_cog(Tasks(bot))
