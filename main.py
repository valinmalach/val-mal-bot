import truststore

truststore.inject_into_ssl()

import asyncio
import logging
import os
from contextlib import asynccontextmanager

import pendulum
import psutil
import sentry_sdk
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import FileResponse
from rich.logging import RichHandler
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.logging import LoggingIntegration

from constants import COGS
from controller import twitch_router, youtube_router
from init import bot

_last_heartbeat = pendulum.now().int_timestamp

load_dotenv()

DISCORD_TOKEN = os.getenv("TEST_DISCORD_TOKEN")

sentry_sdk.init(
    dsn="https://8a7232f8683fae9b47c91b194053ed11@o4508900413865984.ingest.us.sentry.io/4508900418584576",
    integrations=[FastApiIntegration(), LoggingIntegration()],
    enable_logs=True,
)

logging.basicConfig(
    level=logging.INFO, format="%(message)s", datefmt="[%X]", handlers=[RichHandler()]
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)


async def main() -> None:
    try:
        if not DISCORD_TOKEN:
            logger.error("DISCORD_TOKEN is not set, aborting startup")
            raise ValueError("DISCORD_TOKEN is not set in the environment variables.")
        bot.remove_command("help")
        results = await asyncio.gather(
            *(bot.load_extension(ext) for ext in COGS), return_exceptions=True
        )
        for ext, res in zip(COGS, results):
            if isinstance(res, Exception):
                logger.error(f"Failed to load extension {ext}: {res}")
                sentry_sdk.capture_exception(res)

        await bot.start(DISCORD_TOKEN)
    except Exception as e:
        logger.error(f"Error in main: {e}")
        sentry_sdk.capture_exception(e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(main())
    yield


app = FastAPI(lifespan=lifespan)
app.include_router(twitch_router)
app.include_router(youtube_router)


@app.get("/health")
async def health() -> Response:
    return Response("Health check OK", status_code=204)


@app.get("/health/detailed")
@sentry_sdk.trace()
async def detailed_health() -> dict:
    process = psutil.Process()
    cpu_percent = process.cpu_percent()
    memory_info = process.memory_info()

    return {
        "status": "OK",
        "cpu_percent": cpu_percent,
        "memory_mb": memory_info.rss / 1024 / 1024,
        "bot_latency": bot.latency if bot.is_ready() else None,
        "last_heartbeat_age": pendulum.now().int_timestamp - _last_heartbeat,
    }


@app.get("/robots.txt")
async def robots_txt() -> Response:
    if not os.path.exists("robots.txt"):
        logger.warning("robots.txt file not found, returning empty response")
        raise HTTPException(status_code=404)
    return FileResponse("robots.txt")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
        access_log=True,
        log_config=None,
    )
