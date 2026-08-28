import truststore

truststore.inject_into_ssl()


import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import FileResponse, PlainTextResponse
from rich.logging import RichHandler

from background import fire_and_forget
from config import settings
from constants import COGS
from controller import twitch_router
from errors import report
from init import bot
from services.helper.http_client import http_client_manager

logging.basicConfig(
    level=logging.INFO, format="%(message)s", datefmt="[%X]", handlers=[RichHandler()]
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)


async def main() -> None:
    try:
        bot.remove_command("help")
        results = await asyncio.gather(
            *(bot.load_extension(ext) for ext in COGS), return_exceptions=True
        )
        for ext, res in zip(COGS, results):
            if isinstance(res, Exception):
                await report(res, f"Failed to load extension {ext}")
        await bot.start(settings.active_discord_token)
    except Exception as e:  # noqa: BLE001
        await report(e, "Unhandled exception in main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    fire_and_forget(main(), name="bot")
    yield
    await http_client_manager.close()


app = FastAPI(lifespan=lifespan)
app.include_router(twitch_router)


def static_file_response(filename: str) -> Response:
    if not os.path.exists(filename):
        logger.warning(f"{filename} file not found, returning empty response")
        raise HTTPException(status_code=404)
    return FileResponse(filename)


@app.get("/")
async def root() -> Response:
    return PlainTextResponse("Valin Malach Bot")


@app.get("/health")
async def health() -> Response:
    return PlainTextResponse("Healthy")


@app.get("/robots.txt")
async def robots_txt() -> Response:
    return static_file_response("robots.txt")


@app.get("/favicon.ico")
async def favicon() -> Response:
    return static_file_response("favicon.ico")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=settings.port,
        log_level="info",
        access_log=True,
        log_config=None,
    )
