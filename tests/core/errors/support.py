"""Fakes shared by the error-reporting tests."""

from typing import Any


class Clock:
    """errors reads time.monotonic; patching the module's own `time` name leaves
    the event loop's clock alone, which patching time.monotonic itself would not."""

    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def monotonic(self) -> float:
        return self.now


class Channel:
    """What valmal.bot.send.send_message was asked to do."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.result: int | None = 1
        self.error: Exception | None = None

    async def send_message(self, content: str, channel_id: int, **kwargs: Any) -> Any:
        self.calls.append({"content": content, "channel_id": channel_id, **kwargs})
        if self.error is not None:
            raise self.error
        return self.result
