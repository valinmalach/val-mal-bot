import truststore

truststore.inject_into_ssl()


import asyncio
import logging
import os
import traceback
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import FileResponse
from rich.logging import RichHandler

from config import settings
from constants import COGS, ErrorDetails
from controller import twitch_router
from init import bot
from services.helper.http_client import http_client_manager

logging.basicConfig(
    level=logging.INFO, format="%(message)s", datefmt="[%X]", handlers=[RichHandler()]
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)


def get_error_details(e: Exception) -> ErrorDetails:
    return {
        "type": type(e).__name__,
        "message": str(e),
        "args": e.args,
        "traceback": traceback.format_exc(),
    }


def log_error(message: str, error_details: ErrorDetails):
    logger.error(f"{message}\nTraceback:\n{error_details['traceback']}")


async def main() -> None:
    try:
        bot.remove_command("help")
        results = await asyncio.gather(
            *(bot.load_extension(ext) for ext in COGS), return_exceptions=True
        )
        for ext, res in zip(COGS, results):
            if isinstance(res, Exception):
                error_details = get_error_details(res)
                log_error(
                    f"Failed to load extension {ext} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}",
                    error_details,
                )
        await bot.start(settings.active_discord_token)
    except Exception as e:  # noqa: BLE001
        error_details = get_error_details(e)
        log_error(
            f"Unhandled exception in main - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}",
            error_details,
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    _ = asyncio.create_task(main())
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
@app.get("/health")
async def root_or_health() -> Response:
    # Both endpoints return 204, health returns a message
    if "health" in str(root_or_health.__name__):
        return Response("Health check OK", status_code=204)
    return Response(status_code=204)


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
        port=8000,
        log_level="info",
        access_log=True,
        log_config=None,
    )
