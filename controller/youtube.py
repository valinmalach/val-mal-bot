import io
import logging
import traceback

import discord
import polars as pl
import xmltodict
from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import PlainTextResponse

from constants import BOT_ADMIN_CHANNEL, PROMO_CHANNEL, VIDEOS, ErrorDetails
from services import send_message
from services.helper.helper import read_parquet_cached, upsert_row_to_parquet

logger = logging.getLogger(__name__)

youtube_router = APIRouter()


async def is_new_video(channel_id: str, video_id: str) -> bool:
    """Check if the video is new by looking it up in the parquet file."""
    try:
        df = await read_parquet_cached(VIDEOS)

        # Check if this video already exists for this channel
        existing = df.filter(
            (pl.col("channel_id") == channel_id) & (pl.col("video_id") == video_id)
        )

        return existing.height == 0

    except Exception as e:
        logger.error(f"Error checking if video is new: {e}")
        # If we can't check, assume it's new to avoid missing videos
        return True


async def log_error(message: str, traceback_str: str) -> None:
    traceback_buffer = io.BytesIO(traceback_str.encode("utf-8"))
    traceback_file = discord.File(traceback_buffer, filename="traceback.txt")
    await send_message(message, BOT_ADMIN_CHANNEL, file=traceback_file)


async def add_video_to_parquet(channel_id: str, video_id: str):
    """Add a new video to the parquet file."""
    try:
        upsert_row_to_parquet(
            {"channel_id": channel_id, "video_id": video_id},
            VIDEOS,
            id_column="video_id",
        )
        logger.info(f"Added video {video_id} from channel {channel_id} to parquet file")
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Error adding video {video_id} from channel {channel_id} to parquet file - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])


@youtube_router.get("/youtube/webhook")
async def youtube_webhook(request: Request):
    query_params = request.query_params
    challenge = query_params.get("hub.challenge")
    mode = query_params.get("hub.mode")
    topic = query_params.get("hub.topic")

    logger.info(f"YouTube webhook verification - Mode: {mode}, Topic: {topic}")

    if challenge and mode == "subscribe":
        logger.info("YouTube webhook verification successful")
        return PlainTextResponse(content=challenge)

    logger.warning("YouTube webhook verification failed")
    raise HTTPException(status_code=400, detail="Invalid verification request")


@youtube_router.post("/youtube/webhook")
async def youtube_webhook_notification(request: Request):
    try:
        xml_data = await request.body()
        xml_string = xml_data.decode("utf-8")

        # Basic validation to prevent XXE
        if "<!ENTITY" in xml_string or "<!DOCTYPE" in xml_string:
            raise HTTPException(status_code=400, detail="Invalid XML content")

        logger.info(f"Received YouTube webhook notification: {xml_string[:200]}...")

        data = xmltodict.parse(xml_string)

        if entry := data.get("feed", {}).get("entry"):
            video_id = entry.get("yt:videoId")
            channel_id = entry.get("yt:channelId")
            author = entry.get("author", {}).get("name", "Unknown")

            if video_id:
                await handle_new_video(
                    video_id=video_id,
                    channel_id=channel_id,
                    author=author,
                )

        return Response(content="OK", status_code=200)
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Error processing YouTube webhook - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])
        raise HTTPException(status_code=500, detail="Internal server error") from e


async def handle_new_video(
    video_id: str,
    channel_id: str,
    author: str,
):
    try:
        if not is_new_video(channel_id, video_id):
            logger.info(
                f"Ignoring existing video for: {video_id} from channel {channel_id}"
            )
            return

        await add_video_to_parquet(channel_id, video_id)

        url = f"https://www.youtube.com/watch?v={video_id}"
        message = f"New video uploaded by {author}!\n{url}"

        await send_message(message, PROMO_CHANNEL)

        logger.info(f"Processed new video: {video_id} from {author}")

    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Error handling new video notification for video {video_id} from channel {channel_id} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])
