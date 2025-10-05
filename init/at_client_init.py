import logging
import os
from typing import Optional

from atproto_client import Client, Session, SessionEvent
from dotenv import load_dotenv

load_dotenv()

BLUESKY_LOGIN = os.getenv("BLUESKY_LOGIN")
BLUESKY_APP_PASSWORD = os.getenv("BLUESKY_APP_PASSWORD")

logger = logging.getLogger(__name__)


def get_session() -> Optional[str]:
    try:
        with open("at_client_session.txt", encoding="UTF-8") as f:
            return f.read()
    except FileNotFoundError:
        return None


def save_session(session_string: str) -> None:
    with open("at_client_session.txt", "w", encoding="UTF-8") as f:
        f.write(session_string)


def on_session_change(event: SessionEvent, session: Session) -> None:
    if event in (SessionEvent.CREATE, SessionEvent.REFRESH):
        save_session(session.export())


def init_client() -> Client:
    client = Client()
    client.on_session_change(on_session_change)

    if session_string := get_session():
        client.login(session_string=session_string)
    else:
        client.login(BLUESKY_LOGIN, BLUESKY_APP_PASSWORD)

    return client


at_client = init_client()
