# import logging
# import os
# from typing import Optional

# import sentry_sdk
# from atproto_client import Client, Session, SessionEvent
# from dotenv import load_dotenv

# load_dotenv()

# BLUESKY_LOGIN = os.getenv("BLUESKY_LOGIN")
# BLUESKY_APP_PASSWORD = os.getenv("BLUESKY_APP_PASSWORD")

# logger = logging.getLogger(__name__)


# @sentry_sdk.trace()
# def get_session() -> Optional[str]:
#     try:
#         with open("at_client_session.txt", encoding="UTF-8") as f:
#             return f.read()
#     except FileNotFoundError:
#         return None


# @sentry_sdk.trace()
# def save_session(session_string: str) -> None:
#     with open("at_client_session.txt", "w", encoding="UTF-8") as f:
#         f.write(session_string)


# @sentry_sdk.trace()
# def on_session_change(event: SessionEvent, session: Session) -> None:
#     logger.info(
#         f"Session changed: {event} {repr(session)}",
#     )
#     if event in (SessionEvent.CREATE, SessionEvent.REFRESH):
#         logger.info("Saving changed session")
#         save_session(session.export())


# @sentry_sdk.trace()
# def init_client() -> Client:
#     client = Client()
#     client.on_session_change(on_session_change)

#     if session_string := get_session():
#         logger.info("Reusing session")
#         client.login(session_string=session_string)
#     else:
#         logger.info("Creating new session")
#         client.login(BLUESKY_LOGIN, BLUESKY_APP_PASSWORD)

#     return client


# at_client = init_client()
