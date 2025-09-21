import logging
import os

import httpx
import pendulum
import polars as pl
import sentry_sdk
from discord.ext import tasks
from discord.ext.commands import Bot, Cog
from polars import DataFrame

from constants import (
    BLUESKY_CHANNEL,
    BLUESKY_ROLE,
    BOT_ADMIN_CHANNEL,
    SHOUTOUTS_CHANNEL,
)
from init import at_client
from services import get_next_leap, send_message, update_birthday, upsert_row_to_parquet

logger = logging.getLogger(__name__)


class Tasks(Cog):
    def __init__(self, bot: Bot) -> None:
        self.bot = bot

    @Cog.listener()
    async def on_ready(self) -> None:
        logger.info("Bot is ready. Initializing scheduled tasks.")
        if not self.check_posts.is_running():
            self.check_posts.start()
        if not self.check_birthdays.is_running():
            self.check_birthdays.start()

    _quarter_hours = [
        pendulum.Time(hour, minute) for hour in range(24) for minute in (0, 15, 30, 45)
    ]

    # Temporarily disabled to see if webhook endpoint is fully functional first
    # @tasks.loop(hours=24)
    # @sentry_sdk.trace()
    # async def renew_youtube_webhook_subscription(self) -> None:
    #     logger.info("Renewing YouTube webhook subscription")
    #     try:
    #         YOUTUBE_CHANNEL_ID = "UC7BVlWSXIU4hKtWkBqEgZMA"
    #         CALLBACK_URL = "https://valin.loclx.io/youtube/webhook"

    #         response = httpx.post(
    #             "https://pubsubhubbub.appspot.com/subscribe",
    #             data={
    #                 "hub.mode": "subscribe",
    #                 "hub.callback": CALLBACK_URL,
    #                 "hub.topic": f"https://www.youtube.com/xml/feeds/videos.xml?channel_id={YOUTUBE_CHANNEL_ID}",
    #                 "hub.verify": "async",
    #                 "hub.lease_seconds": str(864000),  # 10 days
    #             },
    #         )

    #         if response.status_code == 202:
    #             logger.info("YouTube webhook subscription renewed successfully")
    #         else:
    #             logger.error(
    #                 f"Failed to renew YouTube webhook subscription: {response.status_code} - {response.text}"
    #             )
    #             await send_message(
    #                 f"Failed to renew YouTube webhook subscription: {response.status_code} - {response.text}",
    #                 BOT_ADMIN_CHANNEL,
    #             )
    #     except Exception as e:
    #         logger.error(f"Exception during YouTube webhook renewal: {e}")
    #         sentry_sdk.capture_exception(e)
    #         await send_message(
    #             f"Exception during YouTube webhook renewal: {e}",
    #             BOT_ADMIN_CHANNEL,
    #         )

    @tasks.loop(minutes=1)
    @sentry_sdk.trace()
    async def check_posts(self) -> None:
        logger.info("Executing Bluesky posts synchronization task")
        try:
            logger.info("Retrieving last synced Bluesky post date from database")
            df = pl.read_parquet("data/bluesky.parquet")
            last_sync_date_time = (
                "1970-01-01T00:00:00.000Z" if df.height == 0 else str(df["date"].max())
            )

            logger.info(
                "Fetching Bluesky author feed for user 'valinmalach.bsky.social'"
            )
            try:
                author_feed = at_client.get_author_feed(actor="valinmalach.bsky.social")
            except Exception as e:
                logger.error(f"Error fetching Bluesky author feed: {e}")
                sentry_sdk.capture_exception(e)
                return

            logger.info(
                f"Sorting and filtering new posts since last sync: date > {last_sync_date_time}"
            )
            posts = sorted(
                [
                    feed.post
                    for feed in author_feed.feed
                    if feed.post.author.handle == "valinmalach.bsky.social"
                    and feed.post.indexed_at > last_sync_date_time
                ],
                key=lambda post: post.indexed_at,
            )

            logger.info(f"Transforming posts to payloads: count={len(posts)}")
            posts = [
                {
                    "id": post.uri.split("/")[-1],
                    "date": post.indexed_at,
                    "url": f"https://bsky.app/profile/valinmalach.bsky.social/post/{post.uri.split('/')[-1]}",
                }
                for post in posts
            ]

            logger.info(f"Processing {len(posts)} new posts for insertion")
            for post in posts:
                logger.info(f"Upserting post with id {post['id']}")
                try:
                    success, error = upsert_row_to_parquet(post, "data/bluesky.parquet")
                    if success:
                        logger.info(f"Inserted Bluesky post {post['id']} into parquet")
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
                        f"Failed to insert post {post['id']} into database: {e}",
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
        logger.info("Executing backup data task")
        try:
            # Placeholder for backup logic
            logger.info("Backup data task is not yet implemented")

            # Copy all files from data/ to C:/backups/data/[date]/
            date_string = pendulum.now().format("YYYY-MM-DD")
            backup_path = f"C:/backups/data/{date_string}/"

            if not os.path.exists(backup_path):
                os.makedirs(backup_path)

            for filename in os.listdir("data/"):
                source_file = os.path.join("data/", filename)
                dest_file = os.path.join(backup_path, filename)
                logger.info(f"Backing up {source_file} to {dest_file}")
                try:
                    with open(source_file, "rb") as src, open(dest_file, "wb") as dst:
                        dst.write(src.read())
                    logger.info(f"Successfully backed up {filename}")
                except Exception as e:
                    logger.error(f"Error backing up file {filename}: {e}")
                    sentry_sdk.capture_exception(e)
                    await send_message(
                        f"Error backing up file {filename}: {e}",
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
        logger.info("Executing birthday check task")
        try:
            logger.info("Calculating current UTC time for birthday matching")
            now = (
                pendulum.now("UTC")
                .replace(second=0, microsecond=0)
                .strftime("%Y-%m-%dT%H:%M:%S.000Z")
            )

            logger.info(f"Querying users with birthday equal to now={now}")
            df = pl.read_parquet("data/users.parquet")
            birthday_users = df.filter(pl.col("birthday") == now)
            logger.info(
                f"Processing batch of birthday records: count={birthday_users.height}",
            )
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
        logger.info(
            f"Handling birthday records, total to process: {birthdays_now.height}"
        )
        now = pendulum.now()
        logger.info(f"Processing {birthdays_now.height} birthday records.")
        for record in birthdays_now.iter_rows(named=True):
            user_id = record["id"]
            logger.info(f"Processing birthday for user ID {user_id}")
            user = self.bot.get_user(int(user_id))
            logger.info(f"Looking up Discord user object for ID {user_id}")
            if user is None:
                logger.warning(f"Discord user ID {user_id} not found in guild cache")
                sentry_sdk.capture_message(f"User with ID {user_id} not found.")
                await send_message(
                    f"_process_birthday_records: User with ID {user_id} not found.",
                    BOT_ADMIN_CHANNEL,
                )
                continue
            logger.info(
                f"Sending birthday greeting message to channel {SHOUTOUTS_CHANNEL} for user ID {user_id}"
            )
            await send_message(
                f"Happy Birthday {user.mention}!",
                SHOUTOUTS_CHANNEL,
            )
            logger.info(
                f"Determining next birthday occurrence for user ID {user_id} (leap={record['isBirthdayLeap']})"
            )
            if record["isBirthdayLeap"]:
                logger.info(
                    f"User ID {user_id} has leap-year birthday, calculating next leap year"
                )
                leap = True
                next_birthday = f"{get_next_leap(now.year)}{record['birthday'][4:]}"
            else:
                leap = False
                next_birthday = f"{now.year + 1}{record['birthday'][4:]}"
            logger.info(
                f"Building updated birthday record for user ID {user_id}: next_birthday={next_birthday}"
            )
            updated_record = {
                "id": user_id,
                "username": record["username"],
                "birthday": next_birthday,
                "isBirthdayLeap": leap,
            }
            logger.info(f"Updating birthday record in database for user ID {user_id}")
            success, error = update_birthday(updated_record)
            if success:
                logger.info(
                    f"Updated next birthday for user ID {user_id} to {next_birthday}"
                )
            else:
                logger.error(
                    f"Failed to update birthday for user ID {user_id}: {error}"
                )
                await send_message(
                    f"Failed to update birthday for {updated_record['username']}: {error}",
                    BOT_ADMIN_CHANNEL,
                )


async def setup(bot: Bot) -> None:
    await bot.add_cog(Tasks(bot))
