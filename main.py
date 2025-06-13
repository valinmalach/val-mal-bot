import truststore

truststore.inject_into_ssl()

import asyncio
import logging
import os

import sentry_sdk
from dotenv import load_dotenv
from quart import Quart
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
    try:
        if not DISCORD_TOKEN:
            raise ValueError("DISCORD_TOKEN is not set in the environment variables.")
        bot.remove_command("help")
        results = await asyncio.gather(
            *(bot.load_extension(ext) for ext in COGS), return_exceptions=True
        )
        for ext, res in zip(COGS, results):
            if isinstance(res, Exception):
                sentry_sdk.capture_exception(res)
                print(f"Failed to load extension {ext}: {res}")

        await bot.start(DISCORD_TOKEN)
    except Exception as e:
        sentry_sdk.capture_exception(e)
        print(f"Error connecting the bot: {e}")


app = Quart(__name__)
app.register_blueprint(twitch_bp)


@app.before_serving
async def before_serving():
    asyncio.create_task(main())


@app.route("/health", methods=["GET"])
async def health() -> str:
    return "Healthy"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, use_reloader=False)
