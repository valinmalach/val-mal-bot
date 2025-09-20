import logging
from typing import Optional
from xml.etree.ElementTree import ParseError

import xmltodict
from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import PlainTextResponse

from constants import PROMO_CHANNEL
from services import send_message

youtube_router = APIRouter()

logger = logging.getLogger(__name__)


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
            author = entry.get("author", {}).get("name", "Unknown")
            published = entry.get("published")
            updated = entry.get("updated")

            if video_id:
                await handle_new_video(
                    video_id=video_id,
                    author=author,
                    published=published,
                    updated=updated,
                )

        return Response(content="OK", status_code=200)

    except ParseError as e:
        logger.error(f"Failed to parse YouTube webhook XML: {e}")
        raise HTTPException(status_code=400, detail="Invalid XML format") from e
    except Exception as e:
        logger.error(f"Error processing YouTube webhook: {e}")
        raise HTTPException(status_code=500, detail="Internal server error") from e


async def handle_new_video(
    video_id: str,
    author: str,
    published: Optional[str] = None,
    updated: Optional[str] = None,
):
    try:
        is_new_video = published == updated if published and updated else True

        if not is_new_video:
            logger.info(f"Ignoring video update for: {video_id}")
            return

        url = f"https://www.youtube.com/watch?v={video_id}"
        message = f"New video uploaded by {author}!\n{url}"

        await send_message(message, PROMO_CHANNEL)

    except Exception as e:
        logger.error(f"Error handling new video notification: {e}")
