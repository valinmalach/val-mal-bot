from models import ChannelChatMessageEventSub
from services.helper.twitch import check_mod, twitch_send_message
from services.twitch.api import get_channel, get_user_by_username

from .shoutout_queue import shoutout_queue


async def lurk(event_sub: ChannelChatMessageEventSub, _: str) -> None:
    broadcaster_id = event_sub.event.broadcaster_user_id
    chatter_name = event_sub.event.chatter_user_name
    message = f"{chatter_name} has gone to lurk. Eat, drink, sleep, water your pets, feed your plants. Make sure to take care of yourself and stay safe while you're away!"
    await twitch_send_message(broadcaster_id, message)


async def discord_command(event_sub: ChannelChatMessageEventSub, _: str) -> None:
    broadcaster_id = event_sub.event.broadcaster_user_id
    message = "https://discord.gg/tkJyNJH2k7 Come join us and hang out! This is also where all my updates on streams and whatnot go"
    await twitch_send_message(broadcaster_id, message)


async def kofi(event_sub: ChannelChatMessageEventSub, _: str) -> None:
    broadcaster_id = event_sub.event.broadcaster_user_id
    message = "Idk why you would want to donate, but here: https://ko-fi.com/valinmalach But always remember to take care of yourselves first!"
    await twitch_send_message(broadcaster_id, message)


# async def megathon(event_sub: ChannelChatMessageEventSub, _: str) -> None:
#     broadcaster_id = event_sub.event.broadcaster_user_id
#     message = "I'm holding a megathon until 31st October! Click here to see the goals: https://x.com/ValinMalach/status/1949087837296726406"
#     await twitch_send_message(broadcaster_id, message)
#     message = "Subs, bits, donos to my kofi and throne all contribute to the goals! https://ko-fi.com/valinmalach https://throne.com/valinmalach"
#     await twitch_send_message(broadcaster_id, message)


async def raid(event_sub: ChannelChatMessageEventSub, _: str) -> None:
    broadcaster_id = event_sub.event.broadcaster_user_id
    message = "valinmArrive valinmRaid Valin Raid valinmArrive valinmRaid Valin Raid valinmArrive valinmRaid Your Fallen Angel is here valinmHeart valinmHeart"
    await twitch_send_message(broadcaster_id, message)
    message = "DinoDance DinoDance Valin Raid DinoDance DinoDance Valin Raid DinoDance DinoDance Your Fallen Angel is here <3 <3"
    await twitch_send_message(broadcaster_id, message)


async def socials(event_sub: ChannelChatMessageEventSub, _: str) -> None:
    broadcaster_id = event_sub.event.broadcaster_user_id
    message = "Twitter: https://twitter.com/ValinMalach Bluesky: https://bsky.app/profile/valinmalach.bsky.social"
    await twitch_send_message(broadcaster_id, message)


async def throne(event_sub: ChannelChatMessageEventSub, _: str) -> None:
    broadcaster_id = event_sub.event.broadcaster_user_id
    message = "There's really only one thing on it for now lol... https://throne.com/valinmalach If I do add more, they will all be for stream!"
    await twitch_send_message(broadcaster_id, message)


async def unlurk(event_sub: ChannelChatMessageEventSub, _: str) -> None:
    broadcaster_id = event_sub.event.broadcaster_user_id
    chatter_name = event_sub.event.chatter_user_name
    message = f"{chatter_name} has returned from their lurk. Welcome back! Hope you had a good break and are ready to hang out again!"
    await twitch_send_message(broadcaster_id, message)


async def hug(event_sub: ChannelChatMessageEventSub, args: str) -> None:
    target = args.split(" ", 1)[0] if args else ""
    broadcaster_id = event_sub.event.broadcaster_user_id
    chatter_name = event_sub.event.chatter_user_name
    if not target:
        message = f"{chatter_name} gives everyone a big warm hug. How sweet! <3"
        await twitch_send_message(broadcaster_id, message)
        return None
    message = f"{chatter_name} gives {target} a big warm hug. How sweet! <3"
    await twitch_send_message(broadcaster_id, message)


async def shoutout(event_sub: ChannelChatMessageEventSub, args: str) -> None:
    if not await check_mod(event_sub):
        return None

    broadcaster_id = event_sub.event.broadcaster_user_id
    target = (
        args.split(" ", 1)[0] if args else ""
    ) or event_sub.event.broadcaster_user_login
    if target.startswith("@"):
        target = target[1:]

    user = await get_user_by_username(target)
    target_channel = await get_channel(int(user.id)) if user else None
    if not target_channel:
        message = "User not found."
        await twitch_send_message(broadcaster_id, message)
        return None

    if shoutout_queue.activated:
        shoutout_queue.add_to_queue(target)

    message = f"Go follow {target_channel.broadcaster_name} at https://www.twitch.tv/{target_channel.broadcaster_login}. They were last seen playing {target_channel.game_name}."
    await twitch_send_message(broadcaster_id, message)


async def everything(event_sub: ChannelChatMessageEventSub, args: str) -> None:
    if not await check_mod(event_sub):
        return None

    await discord_command(event_sub, args)
    await socials(event_sub, args)
    await kofi(event_sub, args)
    await throne(event_sub, args)
    # await megathon(event_sub, args)
    await raid(event_sub, args)
