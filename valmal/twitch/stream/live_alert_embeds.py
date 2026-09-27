"""What a live alert looks like: the embeds, and the button under them.

Pure rendering, lifted out of the module that runs the alert so that one is
about the lifecycle and this one is about how it reads. Everything here takes
what Helix returned and gives back something Discord can post.
"""

import re

import discord
import pendulum
from discord.ui import View
from discord.utils import escape_markdown

from valmal.bot.present import quoted
from valmal.core.config import config
from valmal.core.errors import notify_soon
from valmal.twitch.eventsub.commands import is_twitch_login
from valmal.twitch.models.api.channel import Channel
from valmal.twitch.models.api.stream import Stream
from valmal.twitch.models.api.user import User
from valmal.twitch.models.api.video import Video
from valmal.twitch.timestamps import parse_rfc3339


def twitch_url(user_login: str) -> str:
    """A link to the channel, refusing a login that isn't one.

    Helix is trusted, but the result lands in a markdown link's URL slot
    (announcement_embed/live_embed), where an unescaped ")" would close the
    link early. Twitch's own login grammar cannot produce one; this is what
    makes that a guarantee rather than an assumption.

    Empty is let through rather than refused: live_alert_cycle._close passes
    "" when both Helix lookups it prefers have failed, as a deliberate
    "no login to link" fallback rather than a malformed one -- refusing it
    would turn a stream this bot cannot fully describe into a live alert
    that can never close.
    """
    if user_login and not is_twitch_login(user_login):
        raise ValueError(f"Not a Twitch login: {user_login!r}")
    return f"https://www.twitch.tv/{user_login}"


# Twitch's own video ids, observed and documented as purely numeric.
_VOD_ID = re.compile(r"\A[0-9]{1,20}\Z")


def vod_url(video_id: str) -> str:
    """A link to a VOD, built from its id rather than trusting Video.url.

    Same reasoning as twitch_url: the result lands in a markdown link's URL
    slot (offline_embed's VOD field), so it is constructed from a validated
    identifier instead of trusting whatever string Helix's ``url`` field
    happens to hold.
    """
    if not _VOD_ID.fullmatch(video_id):
        raise ValueError(f"Not a Twitch video id: {video_id!r}")
    return f"https://www.twitch.tv/videos/{video_id}"


def mention(channel_id: int) -> str | None:
    """The live-alerts role ping, which only the stream alerts channel gets."""
    return (
        f"<@&{config.role('live_alerts')}>"
        if channel_id == config.channel("stream_alerts")
        else None
    )


def watch_button(url: str) -> View:
    view = View(timeout=None)
    view.add_item(
        discord.ui.Button(
            label=config.template("stream_watch_button"),
            style=discord.ButtonStyle.link,
            url=url,
        )
    )
    return view


# Inside a markdown link label, one pass, so nothing is escaped twice.
# escape_markdown neutralises a whole [label](url) but leaves a bare bracket
# alone, which is all this position needs: the brackets around the title are the
# bot's own, so "](" in a title ends that link and starts one going anywhere.
_IN_LINK = str.maketrans({c: "\\" + c for c in "[]()*_~`|\\"})


def _linkable(text: str) -> str:
    """Text safe to sit inside a markdown link label."""
    return text.translate(_IN_LINK)


# What Twitch hands back is whatever the broadcaster typed, and these alerts are
# posted for every broadcaster the bot announces, not only the owner. A title or
# a game name goes into a description or a field value, both of which render
# markdown: an unescaped "](" ends the link it sits inside and starts one
# pointing anywhere. An author name is plain text to Discord and is left alone,
# the same rule valmal/bot/audit.py records.
def announcement_embed(
    stream: Stream, user_info: User | None, url: str
) -> discord.Embed:
    """The alert as first posted, timestamped at the stream's start."""
    raw_thumb_url = stream.thumbnail_url.replace("{width}x{height}", "400x225")

    return (
        discord.Embed(
            description=f"[**{_linkable(stream.title)}**]({url})",
            color=config.color("embed_color_stream"),
            timestamp=parse_rfc3339(stream.started_at),
        )
        .set_author(
            name=config.template("stream_live_title", name=stream.user_name),
            icon_url=user_info.profile_image_url if user_info else None,
            url=url,
        )
        .add_field(
            name=config.template("stream_field_game"),
            value=escape_markdown(stream.game_name),
            inline=True,
        )
        .add_field(
            name=config.template("stream_field_viewers"),
            value=f"{stream.viewer_count}",
            inline=True,
        )
        .set_image(url=f"{raw_thumb_url}?cb={int(pendulum.now().timestamp())}")
    )


def live_embed(
    stream: Stream,
    user_info: User | None,
    url: str,
    age: str,
    started_at_timestamp: str,
    now: pendulum.DateTime,
) -> discord.Embed:
    """The alert as refreshed, carrying how long the stream has been up."""
    raw_thumb_url = stream.thumbnail_url.replace("{width}x{height}", "400x225")

    return (
        discord.Embed(
            description=f"[**{_linkable(stream.title)}**]({url})",
            color=config.color("embed_color_stream"),
            timestamp=now,
        )
        .set_author(
            name=config.template("stream_live_title", name=stream.user_name),
            icon_url=user_info.profile_image_url if user_info else None,
            url=url,
        )
        .add_field(
            name=config.template("stream_field_game"),
            value=escape_markdown(stream.game_name),
            inline=True,
        )
        .add_field(
            name=config.template("stream_field_viewers"),
            value=f"{stream.viewer_count}",
            inline=True,
        )
        .add_field(
            name=config.template("stream_field_started_at"),
            value=started_at_timestamp,
            inline=True,
        )
        # now, not an independent pendulum.now(): the caller already took "now" to stamp
        # the embed with, and a second wall-clock read would make the cache-buster
        # nondeterministic against the rest of the embed, and untestable.
        .set_image(url=f"{raw_thumb_url}?cb={int(now.timestamp())}")
        .set_footer(text=config.template("stream_footer_online", age=age))
    )


def _title(stream: Stream | None, vod: Video | None, channel: Channel | None) -> str:
    if stream:
        return stream.title
    if vod:
        return vod.title
    return channel.title if channel else "Unknown"


def _display_name(stream: Stream | None, user_info: User | None) -> str:
    if stream:
        return stream.user_name
    return user_info.display_name if user_info else "Unknown"


def _game(stream: Stream | None, channel: Channel | None) -> str:
    if stream:
        return stream.game_name
    return channel.game_name if channel else "Unknown"


def offline_embed(
    stream: Stream | None,
    vod: Video | None,
    channel: Channel | None,
    user_info: User | None,
    url: str,
    age: str,
    now: pendulum.DateTime,
) -> discord.Embed:
    """The alert as left behind once the stream is over."""
    embed = (
        discord.Embed(
            description=f"**{escape_markdown(_title(stream, vod, channel))}**",
            color=config.color("embed_color_stream"),
            timestamp=now,
        )
        .set_author(
            name=config.template(
                "stream_offline_title", name=_display_name(stream, user_info)
            ),
            icon_url=user_info.profile_image_url if user_info else None,
            url=url,
        )
        .add_field(
            name=config.template("stream_field_game"),
            value=escape_markdown(_game(stream, channel)),
            inline=True,
        )
        .set_footer(text=config.template("stream_footer_offline", age=age))
    )

    if vod:
        try:
            link = vod_url(vod.id)
        except ValueError:
            # Degrade rather than lose the embed, as _title, _display_name and _game do: an
            # id Twitch did not send as numeric means no VOD field. Said aloud, though: the
            # field is the alert's way back to the recording, so leaving it out should be seen.
            link = None
            notify_soon(
                f"A live alert closed without its VOD link: the id Twitch sent,"
                f" {quoted(vod.id)}, is not numeric.",
                key="live-alert-bad-vod-id",
            )
        if link:
            embed = embed.add_field(
                name=config.template("stream_field_vod"),
                value=config.template("stream_field_vod_value", url=link),
                inline=True,
            )

    return embed
