from railway_sdk import (
    Project,
    RailwayContext,
    define_railway,
    github,
    preserve,
    project,
    service,
)

# Owns only this service, so a plan never proposes removing anything else in the
# project. See https://docs.railway.com/infrastructure-as-code#multi-repo-projects
PARTIAL = "val-mal-bot"


@define_railway
def main(ctx: RailwayContext | None = None) -> Project:
    val_mal_bot = service(
        "val-mal-bot",
        source=github("valinmalach/val-mal-bot", branch="master"),
        start="python -m alembic upgrade head && exec python main.py",
        # A variable missing here is deleted by the next apply; preserve() keeps
        # the value Railway holds without writing it into the repo.
        env={
            "APP_URL": preserve(),
            "DATABASE_URL": preserve(),
            "DB_ECHO": preserve(),
            "DISCORD_TOKEN": preserve(),
            "TWITCH_CLIENT_ID": preserve(),
            "TWITCH_CLIENT_SECRET": preserve(),
            "TWITCH_WEBHOOK_SECRET": preserve(),
            # glibc gives each thread (DNS lookups, discord.py's heartbeat) an arena
            # of its own, which fragments and is never handed back.
            "MALLOC_ARENA_MAX": "2",
        },
    )
    return project("val-mal-bot", resources=[val_mal_bot])
