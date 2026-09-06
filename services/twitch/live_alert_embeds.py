"""What a live alert looks like: the embeds, and the button under them.

Pure rendering, lifted out of the module that runs the alert so that one is
about the lifecycle and this one is about how it reads. Everything here takes
what Helix returned and gives back something Discord can post.
"""

import discord
import pendulum
from discord.ui import View
from discord.utils import escape_markdown

from models import Channel, Stream, User, Video
from services.config import config
from services.twitch.signature import parse_rfc3339


def twitch_url(user_login: str) -> str:
    return f"https://www.twitch.tv/{user_login}"


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
# the same rule services/audit.py records.
def announcement_embed(stream: Stream, user_info: User | None) -> discord.Embed:
    """The alert as first posted, timestamped at the stream's start."""
    url = twitch_url(stream.user_login)
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
        .set_image(url=f"{raw_thumb_url}?cb={int(pendulum.now().timestamp())}")
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
        embed = embed.add_field(
            name=config.template("stream_field_vod"),
            value=config.template("stream_field_vod_value", url=vod.url),
            inline=True,
        )

    return embed
