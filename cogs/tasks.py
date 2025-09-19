import datetime
import logging

import pandas as pd
import sentry_sdk
from discord.ext import tasks
from discord.ext.commands import Bot, Cog
from pandas import DataFrame

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
        datetime.time(hour, minute) for hour in range(24) for minute in (0, 15, 30, 45)
    ]

    @tasks.loop(minutes=1)
    @sentry_sdk.trace()
    async def check_posts(self) -> None:
        logger.info("Executing Bluesky posts synchronization task")
        try:
            logger.info("Retrieving last synced Bluesky post date from database")
            df = pd.read_parquet("data/bluesky.parquet")
            last_sync_date_time = (
                "1970-01-01T00:00:00.000Z" if df.empty else df["date"].max()
            )

            logger.info(
                "Fetching Bluesky author feed for user 'valinmalach.bsky.social'"
            )
            try:
                author_feed = at_client.get_author_feed(actor="valinmalach.bsky.social")
            except Exception as e:
                logger.error("Error fetching Bluesky author feed: %s", e)
                sentry_sdk.capture_exception(e)
                return

            logger.info(
                "Sorting and filtering new posts since last sync: date > %s",
                last_sync_date_time,
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

            logger.info("Transforming posts to payloads: count=%d", len(posts))
            posts = [
                {
                    "id": post.uri.split("/")[-1],
                    "date": post.indexed_at,
                    "url": f"https://bsky.app/profile/valinmalach.bsky.social/post/{post.uri.split('/')[-1]}",
                }
                for post in posts
            ]

            logger.info("Processing %d new posts for insertion", len(posts))
            for post in posts:
                logger.info("Upserting post with id %s", post["id"])
                try:
                    success, error = upsert_row_to_parquet(post, "data/bluesky.parquet")
                    if success:
                        logger.info(
                            "Inserted Bluesky post %s into database", post["id"]
                        )
                        await send_message(
                            f"<@&{BLUESKY_ROLE}>\n\n{post['url']}",
                            BLUESKY_CHANNEL,
                        )
                    else:
                        logger.warning(
                            "Failed to insert post %s into database: %s",
                            post["id"],
                            error,
                        )
                        await send_message(
                            f"Failed to insert post {post['id']} into database: {error}",
                            BOT_ADMIN_CHANNEL,
                        )
                except Exception as e:
                    logger.error(
                        "Exception upserting Bluesky post %s: %s", post["id"], e
                    )
                    sentry_sdk.capture_exception(e)
                    await send_message(
                        f"Failed to insert post {post['id']} into database: {e}",
                        BOT_ADMIN_CHANNEL,
                    )
        except Exception as e:
            logger.error("Fatal error during Bluesky posts sync: %s", e)
            sentry_sdk.capture_exception(e)
            await send_message(
                f"Fatal error with Bluesky posts check: {e}",
                BOT_ADMIN_CHANNEL,
            )

    @tasks.loop(time=_quarter_hours)
    @sentry_sdk.trace()
    async def check_birthdays(self) -> None:
        logger.info("Executing birthday check task")
        try:
            logger.info("Calculating current UTC time for birthday matching")
            now = (
                datetime.datetime.now(datetime.timezone.utc)
                .replace(second=0, microsecond=0)
                .strftime("%Y-%m-%dT%H:%M:%S.000Z")
            )

            logger.info("Querying users with birthday equal to now=%s", now)
            df = pd.read_parquet("data/users.parquet")
            birthday_users = df[df["birthday"] == now]
            logger.info(
                "Processing batch of birthday records: count=%d",
                len(birthday_users),
            )
            await self._process_birthday_records(birthday_users)
        except Exception as e:
            logger.error("Fatal error during birthday check task: %s", e)
            sentry_sdk.capture_exception(e)
            await send_message(
                f"Fatal error with birthday check: {e}",
                BOT_ADMIN_CHANNEL,
            )

    @sentry_sdk.trace()
    async def _process_birthday_records(self, birthdays_now: DataFrame) -> None:
        logger.info(
            "Handling birthday records, total to process: %d", len(birthdays_now)
        )
        now = datetime.datetime.now()
        logger.info(f"Processing {len(birthdays_now)} birthday records.")
        for _, record in birthdays_now.iterrows():
            user_id = record["id"]
            logger.info("Processing birthday for user ID %s", user_id)
            user = self.bot.get_user(int(user_id))
            logger.info("Looking up Discord user object for ID %s", user_id)
            if user is None:
                logger.warning("Discord user ID %s not found in guild cache", user_id)
                sentry_sdk.capture_message(f"User with ID {user_id} not found.")
                await send_message(
                    f"_process_birthday_records: User with ID {user_id} not found.",
                    BOT_ADMIN_CHANNEL,
                )
                continue
            logger.info(
                "Sending birthday greeting message to channel %s for user ID %s",
                SHOUTOUTS_CHANNEL,
                user_id,
            )
            await send_message(
                f"Happy Birthday {user.mention}!",
                SHOUTOUTS_CHANNEL,
            )
            logger.info(
                "Determining next birthday occurrence for user ID %s (leap=%s)",
                user_id,
                record["isBirthdayLeap"],
            )
            if record["isBirthdayLeap"]:
                logger.info(
                    "User ID %s has leap-year birthday, calculating next leap year",
                    user_id,
                )
                leap = True
                next_birthday = f"{get_next_leap(now.year)}{record['birthday'][4:]}"
            else:
                leap = False
                next_birthday = f"{now.year + 1}{record['birthday'][4:]}"
            logger.info(
                "Building updated birthday record for user ID %s: next_birthday=%s",
                user_id,
                next_birthday,
            )
            updated_record = {
                "id": user_id,
                "username": record["username"],
                "birthday": next_birthday,
                "isBirthdayLeap": leap,
            }
            logger.info("Updating birthday record in database for user ID %s", user_id)
            success, error = update_birthday(updated_record)
            if success:
                logger.info(
                    "Updated next birthday for user ID %s to %s", user_id, next_birthday
                )
            else:
                logger.error(
                    "Failed to update birthday for user ID %s: %s", user_id, error
                )
                await send_message(
                    f"Failed to update birthday for {updated_record['username']}: {error}",
                    BOT_ADMIN_CHANNEL,
                )


async def setup(bot: Bot) -> None:
    await bot.add_cog(Tasks(bot))
