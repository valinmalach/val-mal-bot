import truststore

truststore.inject_into_ssl()

import asyncio
import logging
import os
import signal
import sys
from contextlib import asynccontextmanager, suppress

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
from services.helper.http_client import http_client_manager

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")

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

# Global shutdown event for signal coordination
shutdown_event = asyncio.Event()
restart_requested = False


class GracefulShutdown:
    def __init__(self):
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, self._handle_signal)
        if hasattr(signal, "SIGINT"):
            signal.signal(signal.SIGINT, self._handle_signal)
        logger.info("Signal handlers registered for graceful shutdown")

    def _handle_signal(self, signum, frame):
        signal_name = (
            signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
        )
        logger.info(
            f"Received signal {signal_name} ({signum}), initiating graceful shutdown..."
        )

        try:
            loop = asyncio.get_running_loop()
            loop.call_soon_threadsafe(shutdown_event.set)
        except RuntimeError:
            asyncio.run(self._set_shutdown_event())

    async def _set_shutdown_event(self):
        shutdown_event.set()


shutdown_handler = GracefulShutdown()


async def request_restart():
    """Request a restart by setting the restart flag and shutting down gracefully."""
    global restart_requested
    logger.info("Restart requested - initiating graceful shutdown...")
    restart_requested = True
    shutdown_event.set()


async def start_bot() -> None:
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

        logger.info("Starting Discord bot...")

        # Create a task for bot.start that can be cancelled
        bot_start_task = asyncio.create_task(bot.start(DISCORD_TOKEN))

        # Wait for either the bot to finish or shutdown signal
        done, pending = await asyncio.wait(
            [bot_start_task, asyncio.create_task(shutdown_event.wait())],
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Cancel any pending tasks
        for task in pending:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

        # If shutdown was requested, close the bot
        if shutdown_event.is_set():
            logger.info("Shutdown signal received, closing bot...")
    except Exception as e:
        if not shutdown_event.is_set():
            logger.error(f"Error in bot startup: {e}")
            sentry_sdk.capture_exception(e)
    finally:
        if not bot.is_closed():
            logger.info("Closing Discord bot connection...")
            await bot.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting FastAPI application...")
    bot_task = asyncio.create_task(start_bot())

    yield

    logger.info("Shutting down FastAPI application...")

    if not shutdown_event.is_set():
        logger.info("Setting shutdown event from lifespan")
        shutdown_event.set()

    if bot_task and not bot_task.done():
        logger.info("Stopping Discord bot...")
        try:
            await asyncio.wait_for(bot_task, timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("Bot shutdown timeout, cancelling task...")
            bot_task.cancel()
            with suppress(asyncio.CancelledError):
                await bot_task

    await http_client_manager.close()
    logger.info("Application shutdown complete")


app = FastAPI(lifespan=lifespan)
app.include_router(twitch_router)
app.include_router(youtube_router)

app.state.request_restart = request_restart


@app.get("/")
async def root() -> Response:
    return Response(status_code=204)


@app.get("/health")
async def health() -> Response:
    return Response("Health check OK", status_code=200)


@app.get("/robots.txt")
async def robots_txt() -> Response:
    if not os.path.exists("robots.txt"):
        logger.warning("robots.txt file not found, returning empty response")
        raise HTTPException(status_code=404)
    return FileResponse("robots.txt")


@app.get("/favicon.ico")
async def favicon() -> Response:
    if not os.path.exists("favicon.ico"):
        logger.warning("favicon.ico file not found, returning empty response")
        raise HTTPException(status_code=404)
    return FileResponse("favicon.ico")


async def main():
    """Main function that handles restart loop."""
    global restart_requested, shutdown_event

    restart_requested = False
    while True:
        shutdown_event = asyncio.Event()  # Reset the event for each iteration

        try:
            import uvicorn

            config = uvicorn.Config(
                app,
                host="0.0.0.0",
                port=8000,
                log_level="info",
                access_log=True,
                log_config=None,
            )
            server = uvicorn.Server(config)

            logger.info("Starting FastAPI server...")
            await server.serve()

        except Exception as e:
            logger.error(f"Server error: {e}")
            sentry_sdk.capture_exception(e)

        if restart_requested:
            logger.info("Restart was requested - calling restart script...")

            # Call the restart script (which now only handles git pull and restart)
            try:
                process = await asyncio.create_subprocess_exec(
                    "powershell.exe",
                    "-File",
                    "d:\\Projects\\val-mal-bot\\restart_only.ps1",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await process.communicate()

                if stdout:
                    logger.info(f"Restart script output: {stdout.decode()}")
                if stderr:
                    logger.error(f"Restart script errors: {stderr.decode()}")

            except Exception as e:
                logger.error(f"Failed to execute restart script: {e}")

            # Exit the current process - the script will start a new one
            logger.info("Exiting current process for restart...")
            sys.exit(0)
        else:
            # Normal shutdown, exit the loop
            logger.info("Normal shutdown - exiting...")
            break


if __name__ == "__main__":
    asyncio.run(main())
