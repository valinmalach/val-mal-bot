import truststore

truststore.inject_into_ssl()

import asyncio
import logging
import os
from contextlib import asynccontextmanager

import sentry_sdk
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import FileResponse
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.logging import LoggingIntegration

from constants import COGS
from controller import twitch_router, youtube_router
from init import bot

load_dotenv()

sentry_sdk.init(
    dsn="https://8a7232f8683fae9b47c91b194053ed11@o4508900413865984.ingest.us.sentry.io/4508900418584576",
    integrations=[FastApiIntegration(), LoggingIntegration()],
    # Add data like request headers and IP for users,
    # see https://docs.sentry.io/platforms/python/data-management/data-collected/ for more info
    send_default_pii=True,
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for tracing.
    traces_sample_rate=1.0,
    # Set profile_session_sample_rate to 1.0 to profile 100%
    # of profile sessions.
    profile_session_sample_rate=1.0,
    # Set profile_lifecycle to "trace" to automatically
    # run the profiler on when there is an active transaction
    profile_lifecycle="trace",
    enable_logs=True,
    _experiments={
        "continuous_profiling_auto_start": True,  # Automatically start the profiler
        "enable_metrics": True,  # Enable metrics collection
    },
)

sentry_sdk.profiler.start_profiler()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")


@sentry_sdk.trace()
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
@sentry_sdk.trace()
async def lifespan(app: FastAPI):
    asyncio.create_task(main())
    yield


app = FastAPI(lifespan=lifespan)
app.include_router(twitch_router)
app.include_router(youtube_router)


@app.get("/health")
@sentry_sdk.trace()
async def health() -> Response:
    return Response("Health check OK", status_code=204)


@app.get("/robots.txt")
@sentry_sdk.trace()
async def robots_txt() -> Response:
    if not os.path.exists("robots.txt"):
        logger.warning("robots.txt file not found, returning empty response")
        raise HTTPException(status_code=404)
    return FileResponse("robots.txt")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
