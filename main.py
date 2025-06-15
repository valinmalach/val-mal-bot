import truststore

truststore.inject_into_ssl()

import asyncio
import logging
import os

import sentry_sdk
from dotenv import load_dotenv
from quart import Quart, Response, ResponseReturnValue, send_from_directory
from sentry_sdk.integrations.quart import QuartIntegration

from constants import COGS
from controller import twitch_bp
from init import bot

load_dotenv()

sentry_sdk.init(
    dsn="https://8a7232f8683fae9b47c91b194053ed11@o4508900413865984.ingest.us.sentry.io/4508900418584576",
    integrations=[QuartIntegration()],
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
    _experiments={
        "enable_logs": True,  # Enable logging to Sentry
    },
)

sentry_sdk.profiler.start_profiler()  # type: ignore

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")


@sentry_sdk.trace()
async def main() -> None:
    logger.info("Starting main function")
    try:
        logger.info("Checking DISCORD_TOKEN presence")
        if not DISCORD_TOKEN:
            logger.error("DISCORD_TOKEN is not set, aborting startup")
            raise ValueError("DISCORD_TOKEN is not set in the environment variables.")
        logger.info("DISCORD_TOKEN found, loading extensions: %s", COGS)
        bot.remove_command("help")
        results = await asyncio.gather(
            *(bot.load_extension(ext) for ext in COGS), return_exceptions=True
        )
        for ext, res in zip(COGS, results):
            if isinstance(res, Exception):
                logger.error("Failed to load extension %s: %s", ext, res)
                sentry_sdk.capture_exception(res)
            else:
                logger.info("Successfully loaded extension %s", ext)

        logger.info("Starting bot with DISCORD_TOKEN")
        await bot.start(DISCORD_TOKEN)
    except Exception as e:
        logger.error("Error in main: %s", e)
        sentry_sdk.capture_exception(e)


app = Quart(__name__)
app.register_blueprint(twitch_bp)


@app.before_serving
@sentry_sdk.trace()
async def before_serving():
    logger.info("Quart app is starting, initializing bot")
    asyncio.create_task(main())


@app.route("/health", methods=["GET"])
@sentry_sdk.trace()
async def health() -> ResponseReturnValue:
    logger.info("Health check endpoint called")
    return Response("Health check OK", status=200)


@app.route("/robots.txt")
@sentry_sdk.trace()
async def robots_txt() -> ResponseReturnValue:
    logger.info("Serving robots.txt")
    if not os.path.exists("robots.txt"):
        logger.warning("robots.txt file not found, returning empty response")
        return Response("", status=404)
    return await send_from_directory(".", "robots.txt")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, use_reloader=False)
