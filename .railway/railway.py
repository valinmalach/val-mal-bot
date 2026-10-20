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

PYCACHE = "/app/.pycache"


@define_railway
def main(ctx: RailwayContext | None = None) -> Project:
    val_mal_bot = service(
        "val-mal-bot",
        source=github("valinmalach/val-mal-bot", branch="master"),
        # Bytecode for the stdlib, the venv and our code, at the level Alembic runs (0)
        # and the bot's (-OO), so neither compiles from source at startup (#128). Into
        # PYTHONPYCACHEPREFIX, under /app: the deploy image takes /mise, where the
        # stdlib lives, from an earlier Railpack step than this one. No quotes: Railpack
        # strips them before sh -c, so the stdlib is found through the venv's symlink.
        build=(
            "uv sync --locked --no-dev --no-editable && "
            f"PYTHONPYCACHEPREFIX={PYCACHE} .venv/bin/python -m compileall -q -j0 "
            "--invalidation-mode unchecked-hash -o 0 -o 2 "
            "$(dirname $(dirname $(readlink -f .venv/bin/python)))/lib/python3.15 "
            ".venv valmal main.py"
        ),
        # Lazy imports for the bot alone: a module nothing touches is never loaded.
        # Alembic stays eager, since importing the models is what registers them.
        # -OO drops docstrings; every slash command sets its description explicitly.
        start="python -m alembic upgrade head && exec python -OO -X lazy_imports=all main.py",
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
            # glibc's starting values, fixed: left alone it raises both after a
            # large free, and the heap then keeps what it frees (#104).
            "MALLOC_MMAP_THRESHOLD_": "131072",
            "MALLOC_TRIM_THRESHOLD_": "262144",
            "PYTHONPYCACHEPREFIX": PYCACHE,
        },
    )
    return project("val-mal-bot", resources=[val_mal_bot])
