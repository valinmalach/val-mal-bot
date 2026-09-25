"""The Valin Malach bot: a Discord bot and a Twitch webhook receiver in one process.

``core`` is what both faces lean on, ``db`` is Postgres, ``bot`` is everything reached
through Discord and ``twitch`` everything to do with Twitch. Nothing is re-exported
here: import from the module that owns the name.
"""
