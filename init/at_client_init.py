import logging
import os
from typing import Optional

import sentry_sdk
from atproto_client import Client, Session, SessionEvent
from dotenv import load_dotenv

load_dotenv()

BLUESKY_LOGIN = os.getenv("BLUESKY_LOGIN")
BLUESKY_APP_PASSWORD = os.getenv("BLUESKY_APP_PASSWORD")

logger = logging.getLogger(__name__)


@sentry_sdk.trace()
def get_session() -> Optional[str]:
    logger.info("Attempting to load session from file")
    try:
        with open("at_client_session.txt", encoding="UTF-8") as f:
            session = f.read()
            logger.info("Session loaded from file")
            return session
    except FileNotFoundError:
        logger.info("Session file not found")
        return None


@sentry_sdk.trace()
def save_session(session_string: str) -> None:
    logger.info("Saving session to file")
    with open("at_client_session.txt", "w", encoding="UTF-8") as f:
        f.write(session_string)
    logger.info("Session saved to file")


@sentry_sdk.trace()
def on_session_change(event: SessionEvent, session: Session) -> None:
    logger.info(
        f"Session changed: {event} {repr(session)}",
    )
    if event in (SessionEvent.CREATE, SessionEvent.REFRESH):
        logger.info("Saving changed session")
        save_session(session.export())


@sentry_sdk.trace()
def init_client() -> Client:
    logger.info("Initialising AT Proto Client")
    client = Client()
    client.on_session_change(on_session_change)

    if session_string := get_session():
        logger.info("Reusing existing session")
        client.login(session_string=session_string)
    else:
        logger.info("Creating new session with credentials")
        client.login(BLUESKY_LOGIN, BLUESKY_APP_PASSWORD)

    logger.info("AT Proto Client initialized")
    return client


at_client = init_client()
