"""Snapshot of the values the bot hardcoded before the database existed.

Literals on purpose: this stays valid once constants.py and the view modules
are gone, and remains a working bootstrap for an empty database.
"""

CHANNELS = {
    "audit_logs": 1291775826655707166,
    "bot_admin": 1346408909442781237,
    "dm_requests": 1292413187270115328,
    "food": 1291026325045248101,
    "pets": 1291027524947546164,
    "promo": 1378917167336001606,
    "ranting": 1291026750947590266,
    "roles": 1285277373167570946,
    "rules": 1285275553611517963,
    "shoutouts": 1291026077287710751,
    "stream_alerts": 1285276760044474461,
    "welcome": 1285276874645438544,
}

ROLES = [
    {
        "key": "follower",
        "role_id": 1291769015190032435,
        "name": "🙇Followers",
        "emoji": "✅",
        "custom_id": "accept_rules",
        "embed_key": "rules",
        "position": 0,
    },
    {
        "key": "announcements",
        "role_id": 1292347932904915007,
        "name": "📢Announcements",
        "emoji": "📢",
        "custom_id": "announcements_role",
        "embed_key": "ping_roles",
        "position": 0,
    },
    {
        "key": "live_alerts",
        "role_id": 1292348044888768605,
        "name": "🔴Live Alerts",
        "emoji": "🔴",
        "custom_id": "live_alert_role",
        "embed_key": "ping_roles",
        "position": 1,
    },
    {
        "key": "ping",
        "role_id": 1292348084998897737,
        "name": "❗Ping Role",
        "emoji": "❗",
        "custom_id": "ping_role",
        "embed_key": "ping_roles",
        "position": 2,
    },
    {
        "key": "free_stuff",
        "role_id": 1359500454941298709,
        "name": "🎁Free Stuff",
        "emoji": "🎁",
        "custom_id": "free_stuff_role",
        "embed_key": "ping_roles",
        "position": 3,
    },
    {
        "key": "nsfw_access",
        "role_id": 1292348175553794050,
        "name": "🔞NSFW Access",
        "emoji": "🔞",
        "custom_id": "nsfw_access",
        "embed_key": "nsfw_access",
        "position": 0,
    },
    {
        "key": "he_him",
        "role_id": 1292386380038672404,
        "name": "🙋\u200d♂️He/Him",
        "emoji": "🙋\u200d♂️",
        "custom_id": "he_him_role",
        "embed_key": "pronoun_roles",
        "position": 0,
    },
    {
        "key": "she_her",
        "role_id": 1292386514726289542,
        "name": "🙋\u200d♀️She/Her",
        "emoji": "🙋\u200d♀️",
        "custom_id": "she_her_role",
        "embed_key": "pronoun_roles",
        "position": 1,
    },
    {
        "key": "they_them",
        "role_id": 1292386617348194346,
        "name": "🙋They/Them",
        "emoji": "🙋",
        "custom_id": "they_them_role",
        "embed_key": "pronoun_roles",
        "position": 2,
    },
    {
        "key": "other_ask",
        "role_id": 1292386717449453599,
        "name": "❓Other/Ask",
        "emoji": "❓",
        "custom_id": "other_ask_role",
        "embed_key": "pronoun_roles",
        "position": 3,
    },
    {
        "key": "streamer",
        "role_id": 1292386827008999486,
        "name": "📽️Streamer",
        "emoji": "📽️",
        "custom_id": "streamer_role",
        "embed_key": "other_roles",
        "position": 0,
    },
    {
        "key": "gamer",
        "role_id": 1292386929299689472,
        "name": "🎮Gamer",
        "emoji": "🎮",
        "custom_id": "gamer_role",
        "embed_key": "other_roles",
        "position": 1,
    },
    {
        "key": "artist",
        "role_id": 1292386998438596710,
        "name": "🎨Artist",
        "emoji": "🎨",
        "custom_id": "artist_role",
        "embed_key": "other_roles",
        "position": 2,
    },
    {
        "key": "dms_open",
        "role_id": 1292387067568853012,
        "name": "🟩DMs Open",
        "emoji": "🟩",
        "custom_id": "dms_open",
        "embed_key": "dms_open",
        "position": 0,
    },
    {
        "key": "ask_to_dm",
        "role_id": 1292387187576274995,
        "name": "🟨Ask to DM",
        "emoji": "🟨",
        "custom_id": "ask_to_dm",
        "embed_key": "dms_open",
        "position": 1,
    },
    {
        "key": "dms_closed",
        "role_id": 1292387243964764193,
        "name": "🟥DMs Closed",
        "emoji": "🟥",
        "custom_id": "dms_closed",
        "embed_key": "dms_open",
        "position": 2,
    },
]

EMBEDS = [
    {
        "key": "rules",
        "title": "Server Rules",
        "description": "Please read and follow the server rules:",
        "color": 2123412,
        "channel_key": "rules",
        "position": 0,
    },
    {
        "key": "ping_roles",
        "title": "Ping Roles",
        "description": None,
        "color": 2123412,
        "channel_key": "roles",
        "position": 1,
    },
    {
        "key": "nsfw_access",
        "title": "NSFW Access",
        "description": None,
        "color": 2123412,
        "channel_key": "roles",
        "position": 2,
    },
    {
        "key": "pronoun_roles",
        "title": "Pronouns",
        "description": None,
        "color": 2123412,
        "channel_key": "roles",
        "position": 3,
    },
    {
        "key": "other_roles",
        "title": "Other Roles",
        "description": None,
        "color": 2123412,
        "channel_key": "roles",
        "position": 4,
    },
    {
        "key": "dms_open",
        "title": "DMs Open?",
        "description": None,
        "color": 2123412,
        "channel_key": "roles",
        "position": 5,
    },
]

EMBED_FIELDS = [
    {
        "embed_key": "rules",
        "position": 0,
        "name": "",
        "value": "* This is an 18+ only server. Anyone discovered to be under 18 will be "
        "banned PERMANENTLY. No appeals will be entertained. I don't care if your "
        "18th birthday is tomorrow. You can wait a day before you join my "
        "community. If you are banned pre-18, your ban WILL NOT be lifted once you "
        "hit 18. Permanent means permanent.",
        "inline": False,
    },
    {
        "embed_key": "rules",
        "position": 1,
        "name": "",
        "value": "* Although this is an 18+ server, this does not give anybody free rein to "
        "just say anything anywhere. Keep the appropriate topics in their "
        "appropriate channels. I.e. Please keep NSFW stuff to NSFW channels. "
        "They're there for a reason. I may be horny af, but other people might not "
        "be. Let's keep things safe and comfy for everybody here, ok? 🙂",
        "inline": False,
    },
    {
        "embed_key": "rules",
        "position": 2,
        "name": "",
        "value": "* No doxxing or sharing of personal information (birthdays and other "
        "significant dates are exceptions). Barring {channel:food} and "
        "{channel:pets} and possibly scenery, no sharing of irl pictures, videos or "
        "anything. This might be a bit overkill, but I have known people to have "
        "shared an innocent irl picture without realising there's doxxable info in "
        "the background. I just want to ensure a completely dox-free environment.",
        "inline": False,
    },
    {
        "embed_key": "rules",
        "position": 3,
        "name": "",
        "value": "* No inappropriate names/profile pics.",
        "inline": False,
    },
    {
        "embed_key": "rules",
        "position": 4,
        "name": "",
        "value": "* No spamming.",
        "inline": False,
    },
    {
        "embed_key": "rules",
        "position": 5,
        "name": "",
        "value": "* Try to keep things in their relevant channels.",
        "inline": False,
    },
    {
        "embed_key": "rules",
        "position": 6,
        "name": "",
        "value": "* Please restrict messages to English as much as possible! I would like to "
        "keep all messages in a language that I understand.",
        "inline": False,
    },
    {
        "embed_key": "rules",
        "position": 7,
        "name": "",
        "value": "* Please do not discuss sensitive topics including, but not limited to, "
        "politics and religion on this server.",
        "inline": False,
    },
    {
        "embed_key": "rules",
        "position": 8,
        "name": "",
        "value": "* Avoid discussions about severe mental health issues as it may be "
        "triggering for others. If you are in need of help, please seek "
        "professional help. I am not a therapist, and this isn't the place for "
        "that. Exceptions apply to {channel:ranting} **to a certain extent**. "
        "Everything has its limits. The channel can and will be removed if it's "
        "abused.",
        "inline": False,
    },
    {
        "embed_key": "rules",
        "position": 9,
        "name": "",
        "value": "* Speaking of {channel:ranting}, spoiler when necessary. Especially any "
        "topics that might even be remotely triggering. If you are unsure, just "
        "spoiler it. There's no harm in spoilering.",
        "inline": False,
    },
    {
        "embed_key": "rules",
        "position": 10,
        "name": "",
        "value": "* Don't be a dick, a bigot, a Karen, etc. Use common sense and show common "
        "decency. Be respectful to one another, and above all: CONSENT, CONSENT, "
        "CONSENT. For anything and everything.",
        "inline": False,
    },
    {
        "embed_key": "rules",
        "position": 11,
        "name": "",
        "value": "* No discrimination or bigotry whatsoever, be it racism, homophobia, "
        "transphobia, etc. Depending on circumstances, it MAY be acceptable if it's "
        "painfully obvious to be a joke. But if other people (or I) start to feel "
        "offended/uncomfortable, you will get a strike.",
        "inline": False,
    },
    {
        "embed_key": "rules",
        "position": 12,
        "name": "",
        "value": "* Speaking of strikes, barring insta-ban offences, you are allowed 3 "
        "strikes. 1 strike and you will be given a warning. 2 strikes and you will "
        "be timed out for a day. 3 strikes and you will be banned. I may or may not "
        "allow more strikes to be given to you depending on the level of offence.",
        "inline": False,
    },
    {
        "embed_key": "rules",
        "position": 13,
        "name": "",
        "value": "* I'm generally a very lenient and non-confrontational person, so I will "
        "allow loads of shit, but the moment someone else feels uncomfortable, or "
        "if someone/mods/me tells you to stop. You stop. No questions or arguments.",
        "inline": False,
    },
    {
        "embed_key": "rules",
        "position": 14,
        "name": "",
        "value": "* My Twitch and Discord will have shared bans/strikes. Any offences in one "
        "or the other will reflect in both.",
        "inline": False,
    },
    {
        "embed_key": "rules",
        "position": 15,
        "name": "",
        "value": "* No self-promo outside of the {channel:promo} channel.",
        "inline": False,
    },
    {
        "embed_key": "rules",
        "position": 16,
        "name": "",
        "value": "* This is a weird rule, but no calling cats cars. It pisses me off to no "
        "end. I've been told it's 'cute' and \"it's like saying 'forgor' "
        "instead of 'forgot'\". No it's fucking not. There's a difference. "
        "Because 'forgor' isn't a real word, but 'car' is a real word. It's "
        "confusing, it makes my brain hurt. So, please, just don't. If it's a "
        "GENUINE typo, of course I won't get angry. But if you INTENTIONALLY typo "
        "it, you're just purposefully making things confusing.",
        "inline": False,
    },
    {
        "embed_key": "rules",
        "position": 17,
        "name": "",
        "value": "**Click the button below to accept the rules to get the {role:follower} "
        "role and gain access to the server. By clicking the button, I expect you "
        "to have read and understood the rules.**",
        "inline": False,
    },
    {
        "embed_key": "ping_roles",
        "position": 0,
        "name": "",
        "value": "{role:announcements}",
        "inline": False,
    },
    {
        "embed_key": "ping_roles",
        "position": 1,
        "name": "",
        "value": "{role:live_alerts}",
        "inline": False,
    },
    {
        "embed_key": "ping_roles",
        "position": 2,
        "name": "",
        "value": "{role:ping}",
        "inline": False,
    },
    {
        "embed_key": "ping_roles",
        "position": 3,
        "name": "",
        "value": "{role:free_stuff}",
        "inline": False,
    },
    {
        "embed_key": "nsfw_access",
        "position": 0,
        "name": "",
        "value": "{role:nsfw_access}",
        "inline": False,
    },
    {
        "embed_key": "pronoun_roles",
        "position": 0,
        "name": "",
        "value": "{role:he_him}",
        "inline": False,
    },
    {
        "embed_key": "pronoun_roles",
        "position": 1,
        "name": "",
        "value": "{role:she_her}",
        "inline": False,
    },
    {
        "embed_key": "pronoun_roles",
        "position": 2,
        "name": "",
        "value": "{role:they_them}",
        "inline": False,
    },
    {
        "embed_key": "pronoun_roles",
        "position": 3,
        "name": "",
        "value": "{role:other_ask}",
        "inline": False,
    },
    {
        "embed_key": "other_roles",
        "position": 0,
        "name": "",
        "value": "{role:streamer}",
        "inline": False,
    },
    {
        "embed_key": "other_roles",
        "position": 1,
        "name": "",
        "value": "{role:gamer}",
        "inline": False,
    },
    {
        "embed_key": "other_roles",
        "position": 2,
        "name": "",
        "value": "{role:artist}",
        "inline": False,
    },
    {
        "embed_key": "dms_open",
        "position": 0,
        "name": "",
        "value": "{role:dms_open}",
        "inline": False,
    },
    {
        "embed_key": "dms_open",
        "position": 1,
        "name": "",
        "value": "{role:ask_to_dm} (Ask in {channel:dm_requests})",
        "inline": False,
    },
    {
        "embed_key": "dms_open",
        "position": 2,
        "name": "",
        "value": "{role:dms_closed}",
        "inline": False,
    },
]

COMMANDS = [
    {"name": "lurk", "handler": "static", "position": 0},
    {"name": "unlurk", "handler": "static", "position": 1},
    {"name": "discord", "handler": "static", "position": 2},
    {"name": "kofi", "handler": "static", "position": 3},
    {"name": "throne", "handler": "static", "position": 4},
    {"name": "socials", "handler": "static", "position": 5},
    {"name": "raid", "handler": "static", "position": 6},
    {"name": "hug", "handler": "hug", "position": 7},
    {"name": "so", "handler": "shoutout", "mod_only": True, "position": 8},
    {"name": "everything", "handler": "composite", "mod_only": True, "position": 9},
]

COMMAND_RESPONSES = [
    {
        "command_name": "lurk",
        "position": 0,
        "message": (
            "{chatter} has gone to lurk. Eat, drink, sleep, water your pets, feed "
            "your plants. Make sure to take care of yourself and stay safe while "
            "you're away!"
        ),
    },
    {
        "command_name": "unlurk",
        "position": 0,
        "message": (
            "{chatter} has returned from their lurk. Welcome back! Hope you had a "
            "good break and are ready to hang out again!"
        ),
    },
    {
        "command_name": "discord",
        "position": 0,
        "message": (
            "https://discord.gg/tkJyNJH2k7 Come join us and hang out! This is also "
            "where all my updates on streams and whatnot go"
        ),
    },
    {
        "command_name": "kofi",
        "position": 0,
        "message": (
            "Idk why you would want to donate, but here: https://ko-fi.com/valinmalach "
            "But always remember to take care of yourselves first!"
        ),
    },
    {
        "command_name": "throne",
        "position": 0,
        "message": (
            "There's really only one thing on it for now lol... "
            "https://throne.com/valinmalach If I do add more, they will all be for "
            "stream!"
        ),
    },
    {
        "command_name": "socials",
        "position": 0,
        "message": "Twitter: https://twitter.com/ValinMalach",
    },
    {
        "command_name": "raid",
        "position": 0,
        "message": (
            "valinmArrive valinmRaid Valin Raid valinmArrive valinmRaid Valin Raid "
            "valinmArrive valinmRaid Your Fallen Angel is here valinmCake valinmCake"
        ),
    },
    {
        "command_name": "raid",
        "position": 1,
        "message": (
            "DinoDance DinoDance Valin Raid DinoDance DinoDance Valin Raid DinoDance "
            "DinoDance Your Fallen Angel is here <3 <3"
        ),
    },
]

COMMAND_COMPONENTS = [
    {"parent_name": "everything", "child_name": "discord", "position": 0},
    {"parent_name": "everything", "child_name": "socials", "position": 1},
    {"parent_name": "everything", "child_name": "kofi", "position": 2},
    {"parent_name": "everything", "child_name": "throne", "position": 3},
    {"parent_name": "everything", "child_name": "raid", "position": 4},
]

AUTO_RESPONSES = [
    {"trigger": "ping", "response": "pong"},
    {"trigger": "plap", "response": "clank"},
]

TWITCH_APP_SCOPES = [
    "channel:bot",
    "channel:read:ads",
    "channel:read:redemptions",
    "moderator:manage:announcements",
    "moderator:manage:banned_users",
    "moderator:manage:blocked_terms",
    "moderator:manage:chat_messages",
    "moderator:manage:chat_settings",
    "moderator:manage:shoutouts",
    "moderator:manage:unban_requests",
    "moderator:manage:warnings",
    "moderator:read:chatters",
    "moderator:read:followers",
    "moderator:read:moderators",
    "moderator:read:vips",
    "user:bot",
    "user:read:chat",
    "user:write:chat",
]

SETTINGS = [
    {
        "key": "guild_id",
        "value": "813237030385090580",
        "value_type": "integer",
        "description": "The Discord guild the bot serves",
    },
    {
        "key": "owner_id",
        "value": "389318636201967628",
        "value_type": "integer",
        "description": "Fallback mention when the guild owner cannot be resolved",
    },
    {
        "key": "broadcaster_username",
        "value": "valinmalach",
        "value_type": "string",
        "description": "Twitch login of the main broadcaster",
    },
    {
        "key": "command_prefix",
        "value": "$",
        "value_type": "string",
        "description": "Discord text command prefix",
    },
    {
        "key": "backup_directory",
        "value": "C:/backups",
        "value_type": "string",
        "description": "Destination for the nightly data backup",
    },
    {
        "key": "embed_color_welcome",
        "value": "10181046",
        "value_type": "integer",
        "description": "Welcome embed (0x9B59B6)",
    },
    {
        "key": "embed_color_join",
        "value": "4437378",
        "value_type": "integer",
        "description": "Member joined audit embed (0x43B582)",
    },
    {
        "key": "embed_color_goodbye",
        "value": "10038562",
        "value_type": "integer",
        "description": "Goodbye embed (0x992D22)",
    },
    {
        "key": "embed_color_danger",
        "value": "16729871",
        "value_type": "integer",
        "description": "Leaves, deletions, bans (0xFF470F)",
    },
    {
        "key": "embed_color_info",
        "value": "3375061",
        "value_type": "integer",
        "description": "Edits, updates, other neutral events (0x337FD5)",
    },
    {
        "key": "embed_color_stream",
        "value": "9455359",
        "value_type": "integer",
        "description": "Live alert embeds (0x9046FF)",
    },
]

MESSAGE_TEMPLATES = [
    {
        "key": "twitch_hug_everyone",
        "content": "{chatter} gives everyone a big warm hug. How sweet! <3",
        "description": "!hug with no target",
    },
    {
        "key": "twitch_hug_target",
        "content": "{chatter} gives {target} a big warm hug. How sweet! <3",
        "description": "!hug aimed at someone",
    },
    {
        "key": "twitch_shoutout",
        "content": (
            "Go follow {name} at https://www.twitch.tv/{login}. They were last "
            "seen playing {game}."
        ),
        "description": "!so success",
    },
    {
        "key": "twitch_shoutout_not_found",
        "content": "User not found.",
        "description": "!so when the Twitch user does not exist",
    },
    {
        "key": "twitch_mod_only",
        "content": "Only moderators can use this command.",
        "description": "Refusal for a mod-only command",
    },
    {
        "key": "twitch_follow_thanks",
        "content": (
            "Thank you for following, {user}! valinmKiss Your support means a "
            "lot to me! I hope you enjoy your stay! valinmKiss"
        ),
        "description": "channel.follow event",
    },
    {
        "key": "twitch_ad_break_warning",
        "content": (
            "The next ad break will start in 5 minutes! Feel free to take a "
            "quick break while the ads run! valinmHydrate"
        ),
        "description": "Sent five minutes before a scheduled ad break",
    },
    {
        "key": "twitch_raid_out",
        "content": (
            "We just raided {name}. In case you got left behind, you can find "
            "them here: {url} valinmRaid"
        ),
        "description": "channel.raid event, outgoing",
    },
    {
        "key": "discord_birthday",
        "content": "Happy Birthday {mention}!",
        "description": "Posted to the shoutouts channel at midnight",
    },
    {
        "key": "discord_welcome",
        "content": "**Welcome to Malachar, {mention}**",
        "description": "Welcome embed description",
    },
    {
        "key": "discord_startup",
        "content": "Started successfully!",
        "description": "Posted to the bot admin channel on ready",
    },
    {
        "key": "birthday_set",
        "content": "I've remembered your birthday! I'll wish you at midnight of your selected timezone!",
        "description": "/birthday set succeeded",
    },
    {
        "key": "birthday_set_leap",
        "content": "That's an unfortunate birthday 😦\n\nAh well, looks like I'll only wish you every 4 years!",
        "description": "/birthday set on 29 February",
    },
    {
        "key": "birthday_removed",
        "content": "I've removed your birthday! I won't wish you anymore!",
        "description": "/birthday remove succeeded",
    },
    {
        "key": "birthday_none_to_remove",
        "content": "You had no birthday to remove. Maybe try setting one first before asking me to remove it?",
        "description": "/birthday remove with nothing stored",
    },
    {
        "key": "birthday_bad_timezone",
        "content": "Sorry. I've never heard of the timezone {timezone}. Have you tried using the autocomplete options provided? Because those are the only timezones I know of.",
        "description": "/birthday set with an unknown timezone",
    },
    {
        "key": "birthday_bad_day",
        "content": "{month} doesn't have that many days...",
        "description": "/birthday set with a day the month lacks",
    },
    {
        "key": "birthday_remove_failed",
        "content": "An error occurred while trying to remove your birthday.",
        "description": "/birthday remove with no user record",
    },
    {
        "key": "birthday_operation_failed",
        "content": "Oops, it seems like I couldn't {action} your birthday...\n\n# {mention} FIX MEEEE!!!",
        "description": "Birthday command raised; action is set or forget",
    },
    {
        "key": "admin_nuking",
        "content": "Nuking channel...",
        "description": "/nuke",
    },
    {
        "key": "admin_rules_sent",
        "content": "Rules embed send to rules channel!",
        "description": "/rules",
    },
    {
        "key": "admin_roles_sent",
        "content": "Roles embeds send to roles channel!",
        "description": "/roles",
    },
    {
        "key": "admin_purge_done",
        "content": "Deleted {count} message(s).",
        "description": "/purge succeeded",
    },
    {
        "key": "admin_purge_forbidden",
        "content": "Missing permissions to delete messages here.",
        "description": "/purge without permission",
    },
    {
        "key": "admin_purge_failed",
        "content": "Failed to delete messages: {error}",
        "description": "/purge raised an HTTP error",
    },
    {
        "key": "admin_wrong_channel",
        "content": "This command can only be used in a server text channel or thread.",
        "description": "/purge in an unsupported channel type",
    },
    {
        "key": "admin_no_bulk_delete",
        "content": "This channel does not support bulk message deletion.",
        "description": "/purge where purge is unavailable",
    },
    {
        "key": "discord_goodbye",
        "content": "**{mention} has left. Goodbye!**",
        "description": "Goodbye embed in the welcome channel",
    },
    {
        "key": "audit_member_joined",
        "content": "Member Joined",
        "description": "Audit embed author",
    },
    {
        "key": "audit_member_left",
        "content": "Member Left",
        "description": "Audit embed author",
    },
    {
        "key": "audit_nickname_changed",
        "content": "**{mention} changed their nickname**",
        "description": "Audit log entry",
    },
    {
        "key": "audit_pfp_changed",
        "content": "**{mention} changed their profile picture**",
        "description": "Audit log entry",
    },
    {
        "key": "audit_timed_out",
        "content": "**{mention} has been timed out**\nExpires {expiry}",
        "description": "Audit log entry",
    },
    {
        "key": "audit_timeout_removed",
        "content": "**{mention}'s timeout has been removed**",
        "description": "Audit log entry",
    },
    {
        "key": "audit_attachment_deleted",
        "content": "**Attachment sent by {mention} deleted in {channel}**",
        "description": "Audit log entry",
    },
    {
        "key": "stream_live_title",
        "content": "{name} is now live!",
        "description": "Live alert embed author",
    },
    {
        "key": "stream_offline_title",
        "content": "{name} was live",
        "description": "Live alert embed author once offline",
    },
    {
        "key": "stream_footer_online",
        "content": "Online for {age} | Last updated",
        "description": "Live alert footer while live",
    },
    {
        "key": "stream_footer_offline",
        "content": "Online for {age} | Offline at",
        "description": "Live alert footer once offline",
    },
    {
        "key": "stream_watch_button",
        "content": "Watch Stream",
        "description": "Live alert link button label",
    },
    {
        "key": "stream_field_game",
        "content": "**Game**",
        "description": "Live alert field label",
    },
    {
        "key": "stream_field_viewers",
        "content": "**Viewers**",
        "description": "Live alert field label",
    },
    {
        "key": "stream_field_vod",
        "content": "**VOD**",
        "description": "Live alert field label",
    },
    {
        "key": "stream_field_started_at",
        "content": "**Started At**",
        "description": "Live alert field label",
    },
]
