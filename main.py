import asyncio
import logging
import signal
import sys
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response
from starlette.routing import Route

from valmal.bot import gateway_log
from valmal.bot.client import bot
from valmal.bot.cogs import COGS
from valmal.core import http_client
from valmal.core.background import fire_and_forget
from valmal.core.errors import report
from valmal.core.logging_json import JsonFormatter
from valmal.core.settings import settings
from valmal.twitch.eventsub.router import twitch_router
from valmal.twitch.oauth.router import twitch_oauth_router

# Railway colors a line by which stream it landed on, not by what Python
# attached to it -- unless the line is JSON with a "level" key, which the
# viewer decodes into its own level and searchable fields instead.
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[_handler])
gateway_log.install()
logger = logging.getLogger(__name__)


async def _report_failed_extensions(failures: list[tuple[str, Exception]]) -> None:
    """Report a cog that would not load, once the admin channel can be reached.

    Cogs load before bot.start, and config.load runs in setup_hook, so nothing
    said here during loading could resolve a channel to say it in.
    """
    await bot.wait_until_ready()
    for ext, exc in failures:
        await report(exc, f"Failed to load extension {ext}")


async def main() -> None:
    try:
        bot.remove_command("help")
        results = await asyncio.gather(
            *(bot.load_extension(ext) for ext in COGS), return_exceptions=True
        )
        failures = [
            (ext, res)
            for ext, res in zip(COGS, results, strict=True)
            if isinstance(res, Exception)
        ]
        for ext, exc in failures:
            logger.error("Failed to load extension %s", ext, exc_info=exc)
        if failures:
            fire_and_forget(_report_failed_extensions(failures), name="cog-failures")
        await bot.start(settings.active_discord_token)
    except Exception as e:  # noqa: BLE001
        await report(e, "Unhandled exception in main")
    # A shutdown cancels this task, so here the bot has stopped by itself. The
    # server stops too, and uvicorn re-raises the signal so the process exits as
    # failed, which Railway restarts, rather than serving on with no bot.
    logger.error("The Discord bot stopped, so the process is stopping")
    signal.raise_signal(signal.SIGTERM)


@asynccontextmanager
async def lifespan(app: Starlette) -> AsyncGenerator[None]:
    fire_and_forget(main(), name="bot")
    yield
    await http_client.aclose()


async def root(request: Request) -> Response:
    return PlainTextResponse("Valin Malach Bot")


async def health(request: Request) -> Response:
    return PlainTextResponse("Healthy")


app = Starlette(
    routes=[
        Route("/", root),
        Route("/health", health),
        *twitch_router.routes,
        *twitch_oauth_router.routes,
    ],
    lifespan=lifespan,
)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        # Railway reaches the container on its external interface, so binding all of
        # them is the point.
        host="0.0.0.0",  # noqa: S104  # nosec B104
        port=settings.port,
        log_level="info",
        access_log=True,
        log_config=None,
        # Nothing here serves a websocket; "auto" imports the whole stack anyway.
        ws="none",
    )
