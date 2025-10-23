import io
import logging
import os
import shutil
import traceback

import discord
import pendulum
import polars as pl
from atproto import exceptions
from discord.ext import tasks
from discord.ext.commands import Bot, Cog
from dotenv import load_dotenv
from polars import DataFrame

from constants import (
    BLUESKY_CHANNEL,
    BLUESKY_ROLE,
    BOT_ADMIN_CHANNEL,
    SHOUTOUTS_CHANNEL,
    YOUTUBE_CHANNEL_IDS,
    ErrorDetails,
    UserRecord,
)
from init import at_client
from services import (
    get_next_leap,
    read_parquet_cached,
    send_message,
    update_birthday,
    upsert_row_to_parquet,
)
from services.helper.http_client import http_client_manager

load_dotenv()

logger = logging.getLogger(__name__)

APP_URL = os.getenv("APP_URL")


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

    async def log_error(self, message: str, traceback_str: str) -> None:
        traceback_buffer = io.BytesIO(traceback_str.encode("utf-8"))
        traceback_file = discord.File(traceback_buffer, filename="traceback.txt")
        await send_message(message, BOT_ADMIN_CHANNEL, file=traceback_file)

    @tasks.loop(hours=24)
    async def renew_youtube_webhook_subscription(self) -> None:
        logger.info("Renewing YouTube webhook subscriptions")
        try:
            CALLBACK_URL = f"{APP_URL}/youtube/webhook"

            for channel_id in YOUTUBE_CHANNEL_IDS:
                response = await http_client_manager.request(
                    "POST",
                    "https://pubsubhubbub.appspot.com/subscribe",
                    data={
                        "hub.mode": "subscribe",
                        "hub.callback": CALLBACK_URL,
                        "hub.topic": f"https://www.youtube.com/xml/feeds/videos.xml?channel_id={channel_id}",
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
            error_details: ErrorDetails = {
                "type": type(e).__name__,
                "message": str(e),
                "args": e.args,
                "traceback": traceback.format_exc(),
            }
            error_msg = f"Error renewing YouTube webhook subscription - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
            logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
            await self.log_error(error_msg, error_details["traceback"])

    @tasks.loop(minutes=1)
    async def check_posts(self) -> None:
        try:
            df = await read_parquet_cached("data/bluesky.parquet")
            last_sync_date_time = (
                "1970-01-01T00:00:00.000Z" if df.height == 0 else str(df["date"].max())
            )

            author_feed = await self._fetch_author_feed()
            if author_feed is None:
                return

            posts = self._extract_new_posts(author_feed, last_sync_date_time)
            await self._process_posts(posts)

        except Exception as e:
            await self._handle_fatal_error(e, "Fatal error during Bluesky posts sync")

    async def _fetch_author_feed(self):
        """Fetch author feed from Bluesky API with error handling."""
        try:
            return at_client.get_author_feed(actor="valinmalach.bsky.social")
        except exceptions.InvokeTimeoutError:
            return None
        except Exception as e:
            if self._is_temporary_api_error(e):
                return None

            await self._handle_fatal_error(e, "Error fetching Bluesky author feed")
            return None

    def _is_temporary_api_error(self, e: Exception) -> bool:
        """Check if the exception is a temporary API error (502/503)."""
        if not (hasattr(e, "args") and len(e.args) > 0):
            return False

        response = e.args[0]
        if not hasattr(response, "status_code"):
            return False

        if response.status_code in {502, 503}:
            error_msg = (
                response.content.error
                if hasattr(response, "content") and hasattr(response.content, "error")
                else "ErrorNotProvided"
            )
            logger.info(
                f"Bluesky API temporarily unavailable ({response.status_code} {error_msg})"
            )
            return True

        return False

    def _extract_new_posts(self, author_feed, last_sync_date_time: str) -> list[dict]:
        """Extract and format new posts from the author feed."""
        posts = sorted(
            [
                feed.post
                for feed in author_feed.feed
                if feed.post.author.handle == "valinmalach.bsky.social"
                and feed.post.indexed_at > last_sync_date_time
            ],
            key=lambda post: post.indexed_at,
        )

        return [
            {
                "id": post.uri.split("/")[-1],
                "date": post.indexed_at,
                "url": f"https://bsky.app/profile/valinmalach.bsky.social/post/{post.uri.split('/')[-1]}",
            }
            for post in posts
        ]

    async def _process_posts(self, posts: list[dict]) -> None:
        """Process and send notifications for new posts."""
        for post in posts:
            try:
                upsert_row_to_parquet(post, "data/bluesky.parquet")
                await send_message(
                    f"<@&{BLUESKY_ROLE}>\n\n{post['url']}",
                    BLUESKY_CHANNEL,
                )
            except Exception as e:
                error_msg = f"Exception upserting Bluesky post {post['id']}"
                await self._handle_fatal_error(e, error_msg)

    async def _handle_fatal_error(self, e: Exception, context: str) -> None:
        """Handle fatal errors with consistent logging and notification."""
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"{context} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await self.log_error(error_msg, error_details["traceback"])

    @tasks.loop(time=pendulum.Time(0, 0, 0, 0))
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
                    error_details: ErrorDetails = {
                        "type": type(e).__name__,
                        "message": str(e),
                        "args": e.args,
                        "traceback": traceback.format_exc(),
                    }
                    error_msg = f"Error backing up {'directory' if is_dir else 'file'} {item} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
                    logger.error(
                        f"{error_msg}\nTraceback:\n{error_details['traceback']}"
                    )
                    await self.log_error(error_msg, error_details["traceback"])
        except Exception as e:
            error_details: ErrorDetails = {
                "type": type(e).__name__,
                "message": str(e),
                "args": e.args,
                "traceback": traceback.format_exc(),
            }
            error_msg = f"Fatal error during backup data task - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
            logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
            await self.log_error(error_msg, error_details["traceback"])

    @tasks.loop(time=_quarter_hours)
    async def check_birthdays(self) -> None:
        try:
            now = (
                pendulum.now("UTC")
                .replace(second=0, microsecond=0)
                .strftime("%Y-%m-%dT%H:%M:%S.000Z")
            )

            df = await read_parquet_cached("data/users.parquet")
            birthday_users = df.filter(pl.col("birthday") == now)
            await self._process_birthday_records(birthday_users)
        except Exception as e:
            error_details: ErrorDetails = {
                "type": type(e).__name__,
                "message": str(e),
                "args": e.args,
                "traceback": traceback.format_exc(),
            }
            error_msg = f"Fatal error during birthday check task - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
            logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
            await self.log_error(error_msg, error_details["traceback"])

    async def _process_birthday_records(self, birthdays_now: DataFrame) -> None:
        now = pendulum.now()
        for record in birthdays_now.iter_rows(named=True):
            user_id = record["id"]
            user = self.bot.get_user(int(user_id))
            if user is None:
                logger.warning(f"Discord user ID {user_id} not found in guild cache")
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
            updated_record: UserRecord = {
                "id": user_id,
                "username": record["username"],
                "birthday": next_birthday,
                "isBirthdayLeap": leap,
            }
            try:
                update_birthday(updated_record)
            except Exception as e:
                error_details: ErrorDetails = {
                    "type": type(e).__name__,
                    "message": str(e),
                    "args": e.args,
                    "traceback": traceback.format_exc(),
                }
                error_msg = f"Failed to update birthday for user {record['username']} (ID: {user_id}) - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
                logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
                await self.log_error(error_msg, error_details["traceback"])


async def setup(bot: Bot) -> None:
    await bot.add_cog(Tasks(bot))
