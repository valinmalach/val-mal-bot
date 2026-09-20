"""What closing an alert needs to know about a stream that has ended: its VOD, and the
channel details and link the offline embed falls back on."""

from valmal.core.errors import notify, report
from valmal.twitch.client.api import get_channel, get_stream_vod
from valmal.twitch.client.helix import HelixError
from valmal.twitch.models.api.channel import Channel
from valmal.twitch.models.api.stream import Stream
from valmal.twitch.models.api.user import User
from valmal.twitch.models.api.video import Video
from valmal.twitch.stream.live_alert_embeds import twitch_url


async def fetch_vod(broadcaster_id: int, stream_id: int) -> Video | None:
    if not stream_id:
        return None
    try:
        return await get_stream_vod(broadcaster_id, stream_id)
    except Exception as e:  # noqa: BLE001
        await report(e, f"Failed to fetch VOD info for broadcaster_id={broadcaster_id}")
        return None


async def gather_close_info(
    broadcaster_id: int,
    stream_id: int,
    stream: Stream | None,
    user_info: User | None,
) -> tuple[Stream | None, Channel | None, str]:
    """What ``_close`` needs to build the offline embed: channel info (with
    its own Helix fallback), which stream still counts as this alert's own,
    and the login to link to.
    """
    try:
        channel_info = await get_channel(broadcaster_id)
    except HelixError as e:
        # Only a fallback for the title and game, which the VOD usually supplies
        # anyway. Not worth abandoning the close over, but worth saying.
        await notify(
            f"Closing the live alert for broadcaster {broadcaster_id} without channel"
            f" details: {e}",
            key=f"live-alert-close-channel:{broadcaster_id}",
        )
        channel_info = None

    # A live stream that is not the one this alert announced describes a
    # different broadcast; borrowing its title would retitle this message.
    own_stream = stream if stream is not None and stream.id == str(stream_id) else None

    if user_info:
        login = user_info.login
    elif channel_info:
        login = channel_info.broadcaster_login
    else:
        login = ""

    try:
        url = twitch_url(login)
    except ValueError as e:
        # Retrying would not help: a malformed login is a permanent property
        # of this cycle's Helix data, not a transient failure, and _close's
        # whole job is to guarantee the alert actually closes. Degrades to
        # the same generic link an unresolved login already produces.
        await notify(
            f"Closing the live alert for broadcaster {broadcaster_id} with an"
            f" unusable login ({e}); using a generic link instead.",
            key=f"live-alert-close-login:{broadcaster_id}",
        )
        url = twitch_url("")

    return own_stream, channel_info, url
