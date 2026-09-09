import logging
from datetime import datetime
from typing import ClassVar

import pendulum
from discord.ext import tasks
from discord.ext.commands import Bot, Cog
from discord.utils import escape_markdown

from db import repository
from db.models import DiscordUser
from errors import notify, report
from services.birthday import next_birthday
from services.config import config
from services.send import send_message
from services.twitch.api import broken_subscriptions
from services.twitch.helix import HelixError

logger = logging.getLogger(__name__)

# A tick that ran late should still announce. A birthday staler than this is
# only moved on, so a bot that was down for days does not greet the wrong day.
_ANNOUNCE_GRACE_SECONDS = 24 * 60 * 60


def undeliverable_summary(broken: dict[str, str]) -> str:
    detail = "\n".join(f"- {identity}: {reason}" for identity, reason in broken.items())
    return (
        f"{len(broken)} Twitch subscription(s) will not deliver:\n{detail}\n"
        "/subscribe replaces the stream.online and stream.offline pair; the"
        " rest are registered outside the bot."
    )


class Tasks(Cog):
    def __init__(self, bot: Bot) -> None:
        self.bot = bot

    @Cog.listener()
    async def on_ready(self) -> None:
        if not self.check_birthdays.is_running():
            self.check_birthdays.start()
        if not self.recheck_subscriptions.is_running():
            self.recheck_subscriptions.start()

    # What was undeliverable last time this looked. A broken subscription stays
    # broken until somebody fixes it, so a loop that reported every pass would
    # report the same thing forever; this is what makes it report a change.
    _known_broken: ClassVar[dict[str, str]] = {}

    @tasks.loop(hours=1)
    async def recheck_subscriptions(self) -> None:
        """Notice a subscription that will not deliver, and one that starts to.

        Twitch only reports a subscription it disabled by calling the webhook,
        which is the thing that is not working, so nothing else would ever find
        out. An interval loop runs its first pass immediately, so this is the
        startup check as well as the hourly one - having both meant the first
        hourly pass reported everything startup had already reported.
        """
        try:
            broken = await broken_subscriptions()
        except HelixError as e:
            await report(e, "Could not re-check the Twitch subscriptions")
            return

        newly = {k: v for k, v in broken.items() if k not in self._known_broken}
        recovered = [k for k in self._known_broken if k not in broken]
        self._known_broken.clear()
        self._known_broken.update(broken)

        if newly:
            await notify(undeliverable_summary(newly))
        if recovered:
            await notify(
                f"{len(recovered)} Twitch subscription(s) deliver again:\n"
                + "\n".join(f"- {identity}" for identity in recovered)
            )

    _quarter_hours: ClassVar[list[pendulum.Time]] = [
        pendulum.Time(hour, minute) for hour in range(24) for minute in (0, 15, 30, 45)
    ]

    @tasks.loop(time=_quarter_hours)
    async def check_birthdays(self) -> None:
        try:
            now = pendulum.now("UTC").replace(second=0, microsecond=0)
            due = await repository.users_due_birthday(now)
            await self._process_birthday_records(due)
        except Exception as e:  # noqa: BLE001
            await report(e, "Fatal error during birthday check task")

    async def _process_birthday_records(self, due: list[DiscordUser]) -> None:
        now = pendulum.now("UTC")
        for record in due:
            try:
                await self._process_birthday(record, now)
            except Exception as e:  # noqa: BLE001
                await report(
                    e,
                    f"Failed to process birthday for user {escape_markdown(record.username)}"
                    f" (ID: {record.id})",
                )

    async def _process_birthday(
        self, record: DiscordUser, now: pendulum.DateTime
    ) -> None:
        if record.birthday is None:
            return

        stale = not self._within_announce_grace(record.birthday, now)

        # Rescheduled before it is announced, and either way. A record left in
        # the past comes due again on every run: if the announcement failed that
        # is a greeting nobody sent, but if the write failed after a greeting
        # went out it is the same greeting again, every quarter of an hour.
        leap = bool(record.is_birthday_leap)
        zone = record.birthday_timezone
        next_at = next_birthday(record.birthday, leap, now, zone)
        await repository.upsert_user(record.id, record.username, next_at, leap, zone)

        if stale:
            await notify(
                f"Birthday for {escape_markdown(record.username)} (ID: {record.id}) was due at"
                f" {record.birthday} and is too stale to announce, so nobody"
                f" greeted them; rescheduled to {next_at}.",
                key=f"birthday-stale:{record.id}",
            )
            return

        await self._announce_birthday(record)

    @staticmethod
    def _within_announce_grace(birthday: datetime, now: pendulum.DateTime) -> bool:
        late = (now - pendulum.instance(birthday)).total_seconds()
        return late <= _ANNOUNCE_GRACE_SECONDS

    async def _announce_birthday(self, record: DiscordUser) -> None:
        user = self.bot.get_user(record.id)
        if user is None:
            logger.warning(f"Discord user ID {record.id} not found in guild cache")
            await notify(f"_announce_birthday: User with ID {record.id} not found.")
            return
        await send_message(
            config.template("discord_birthday", mention=user.mention),
            config.channel("shoutouts"),
        )


async def setup(bot: Bot) -> None:
    await bot.add_cog(Tasks(bot))
