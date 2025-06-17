import asyncio
import logging

import discord
import sentry_sdk
from discord import CategoryChannel, ForumChannel
from discord.abc import PrivateChannel
from discord.ext.commands import Bot
from requests.exceptions import ConnectionError

from constants import BOT_ADMIN_CHANNEL, GUILD_ID
from init import xata_client
from services import update_alert

MY_GUILD = discord.Object(id=GUILD_ID)
logger = logging.getLogger(__name__)


async def restart_live_alert_tasks() -> None:
    logger.info("Attempting to restart live alert tasks from Xata.")

    page_num = 1
    while True:
        try:
            logger.info(f"Fetching page {page_num} of live alert records from Xata.")
            records = xata_client.data().query("live_alerts", {"page": {"size": 200}})
            logger.info(
                f"Successfully fetched page {page_num} of live alert records. Found {len(records['records'])} records on this page."
            )
            break
        except ConnectionError as e:
            logger.error(
                f"ConnectionError while fetching page {page_num} of live alert records: {e}. Retrying in 1 second."
            )
            sentry_sdk.capture_exception(e)
            await asyncio.sleep(1)

    logger.info(f"Processing {len(records['records'])} records from page {page_num}.")
    for alert in records["records"]:
        broadcaster_id = str(alert["id"])
        channel_id = int(alert["channel_id"])
        message_id = int(alert["message_id"])
        stream_id = str(alert["stream_id"])
        stream_started_at = str(alert["stream_started_at"])
        logger.info(
            f"Processing alert: broadcaster_id={broadcaster_id}, channel_id={channel_id}, message_id={message_id}, stream_id={stream_id}, stream_started_at={stream_started_at}"
        )
        asyncio.create_task(
            update_alert(
                broadcaster_id=broadcaster_id,
                channel_id=channel_id,
                message_id=message_id,
                stream_id=stream_id,
                stream_started_at=stream_started_at,
            )
        )
        logger.info(
            f"Created task to update alert for broadcaster_id={broadcaster_id}."
        )

    while records.has_more_results():
        page_num += 1
        while True:
            try:
                logger.info(
                    f"Fetching next page ({page_num}) of live alert records from Xata using cursor: {records.get_cursor()}."
                )
                records = xata_client.data().query(
                    "live_alerts",
                    {
                        "page": {"page": {"size": 200}, "after": records.get_cursor()},
                    },
                )
                logger.info(
                    f"Successfully fetched page {page_num} of live alert records. Found {len(records['records'])} records on this page."
                )
                break
            except ConnectionError as e:
                logger.error(
                    f"ConnectionError while fetching page {page_num} of live alert records: {e}. Retrying in 1 second."
                )
                sentry_sdk.capture_exception(e)
                await asyncio.sleep(1)

        logger.info(
            f"Processing {len(records['records'])} records from page {page_num}."
        )
        for alert in records["records"]:
            broadcaster_id = str(alert["id"])
            channel_id = int(alert["channel_id"])
            message_id = int(alert["message_id"])
            stream_id = str(alert["stream_id"])
            stream_started_at = str(alert["stream_started_at"])
            logger.info(
                f"Processing alert: broadcaster_id={broadcaster_id}, channel_id={channel_id}, message_id={message_id}, stream_id={stream_id}, stream_started_at={stream_started_at}"
            )
            asyncio.create_task(
                update_alert(
                    broadcaster_id=broadcaster_id,
                    channel_id=channel_id,
                    message_id=message_id,
                    stream_id=stream_id,
                    stream_started_at=stream_started_at,
                )
            )
            logger.info(
                f"Created task to update alert for broadcaster_id={broadcaster_id}."
            )

    logger.info("Finished restarting live alert tasks.")


class MyBot(Bot):
    def __init__(self, *, command_prefix: str, intents: discord.Intents) -> None:
        super().__init__(command_prefix=command_prefix, intents=intents)
        self.case_insensitive = True

    async def setup_hook(self) -> None:
        self.tree.copy_global_to(guild=MY_GUILD)
        await self.tree.sync(guild=MY_GUILD)

        from views import (
            DMsOpenView,
            NSFWAccessView,
            OtherRolesView,
            PingRolesView,
            PronounRolesView,
            RulesView,
        )

        # register all persistent Views so buttons still work after a restart
        self.add_view(RulesView())
        self.add_view(PingRolesView())
        self.add_view(NSFWAccessView())
        self.add_view(PronounRolesView())
        self.add_view(OtherRolesView())
        self.add_view(DMsOpenView())


bot = MyBot(command_prefix="$", intents=discord.Intents.all())


@bot.event
@sentry_sdk.trace()
async def on_ready() -> None:
    logger.info("Creating task to restart live alert tasks from Xata.")
    asyncio.create_task(restart_live_alert_tasks())

    channel = bot.get_channel(BOT_ADMIN_CHANNEL)
    if channel is None or isinstance(
        channel, (ForumChannel, CategoryChannel, PrivateChannel)
    ):
        return
    await channel.send("Started successfully!")
