import asyncio
import logging
import sys
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Response
from fastapi.responses import PlainTextResponse

from constants import COGS
from valmal.bot.client import bot
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
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)


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


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    fire_and_forget(main(), name="bot")
    yield
    await http_client.aclose()


app = FastAPI(lifespan=lifespan)
app.include_router(twitch_router)
app.include_router(twitch_oauth_router)


@app.get("/")
async def root() -> Response:
    return PlainTextResponse("Valin Malach Bot")


@app.get("/health")
async def health() -> Response:
    return PlainTextResponse("Healthy")


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
    )
