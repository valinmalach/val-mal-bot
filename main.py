import traceback

import truststore

truststore.inject_into_ssl()

import asyncio
import logging
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import FileResponse
from rich.logging import RichHandler

from constants import COGS, ErrorDetails
from controller import twitch_router, youtube_router
from init import bot
from services.helper.http_client import http_client_manager

load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(message)s", datefmt="[%X]", handlers=[RichHandler()]
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")


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
                error_details: ErrorDetails = {
                    "type": type(res).__name__,
                    "message": str(res),
                    "args": res.args,
                    "traceback": traceback.format_exc(),
                }
                error_msg = f"Failed to load extension {ext} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
                logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")

        await bot.start(DISCORD_TOKEN)
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Unhandled exception in main - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(main())
    yield
    await http_client_manager.close()


app = FastAPI(lifespan=lifespan)
app.include_router(twitch_router)
app.include_router(youtube_router)


@app.get("/")
async def root() -> Response:
    return Response(status_code=204)


@app.get("/health")
async def health() -> Response:
    return Response("Health check OK", status_code=204)


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
