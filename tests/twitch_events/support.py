"""What the Twitch event handlers reach out to, recorded."""

from typing import Any

from services.twitch.helix import HelixError
from tests.twitch.support import stream_json, user_json
from valmal.twitch.models.api.stream import Stream
from valmal.twitch.models.api.user import User


class EventWorld:
    """Every collaborator of services.twitch.events, recorded and scriptable."""

    def __init__(self) -> None:
        self.notified: list[tuple[str, str | None]] = []
        self.reported: list[str] = []
        self.asked: list[int] = []
        self.stream_answers: list[Stream | HelixError | None] = [None]
        self.user_answer: User | HelixError | None = None
        self.said: list[tuple[str | int, str, str]] = []
        self.say_result = True
        self.templates: list[tuple[str | int, str, dict[str, Any]]] = []
        self.dispatched: list[tuple[str, str]] = []
        self.dispatch_error: Exception | None = None
        self.slept: list[float] = []
        self.clock = 1000.0
        self.calls: list[tuple[str, Any]] = []
        self.announce_error: Exception | None = None
        self.wake_error: Exception | None = None
        self.redeem_error: Exception | None = None
        self.template_error: Exception | None = None

    async def notify(self, text: str, *, key: str | None = None) -> bool:
        self.notified.append((text, key))
        return True

    async def report(self, exc: Exception, context: str, **_: object) -> None:
        self.reported.append(context)

    async def get_stream(self, broadcaster_id: int) -> Stream | None:
        self.asked.append(broadcaster_id)
        last = len(self.stream_answers) - 1
        answer = self.stream_answers[min(len(self.asked) - 1, last)]
        if isinstance(answer, Exception):
            raise answer
        return answer

    async def get_user(self, broadcaster_id: int) -> User | None:
        if isinstance(self.user_answer, Exception):
            raise self.user_answer
        return self.user_answer

    async def say(self, broadcaster_id: str | int, text: str, what: str) -> bool:
        self.said.append((broadcaster_id, text, what))
        return self.say_result

    async def say_template(
        self, broadcaster_id: str | int, key: str, **values: Any
    ) -> bool:
        if self.template_error is not None:
            raise self.template_error
        self.templates.append((broadcaster_id, key, values))
        return True

    async def dispatch(self, event_sub: Any, name: str, args: str) -> None:
        self.dispatched.append((name, args))
        if self.dispatch_error is not None:
            raise self.dispatch_error

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.clock += seconds

    async def began(self, broadcaster_id: int, stream: Stream) -> None:
        self.calls.append(("began", broadcaster_id))

    async def announce(self, *args: Any) -> None:
        self.calls.append(("announce", args))
        if self.announce_error is not None:
            raise self.announce_error

    async def wake_alert(self, broadcaster_id: int) -> None:
        self.calls.append(("alert.wake", broadcaster_id))
        if self.wake_error is not None:
            raise self.wake_error

    async def wake_session(self, broadcaster_id: int) -> None:
        self.calls.append(("session.wake", broadcaster_id))

    async def chatted(self, event_sub: Any) -> None:
        self.calls.append(("chatted", event_sub.event.chatter_user_login))

    async def redeemed(self, event_sub: Any) -> None:
        self.calls.append(("redeemed", event_sub.event.user_login))
        if self.redeem_error is not None:
            raise self.redeem_error

    def schedule_warning(self, broadcaster_id: str) -> None:
        self.calls.append(("schedule_warning", broadcaster_id))

    def raided(self, user_id: str) -> None:
        self.calls.append(("raided", user_id))


def live(login: str = "valinmalach", stream_id: str = "10") -> Stream:
    return Stream.model_validate(stream_json(stream_id, user_login=login))


def profile(id: str = "111", login: str = "valinmalach") -> User:
    return User.model_validate(user_json(id, login))
