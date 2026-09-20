from types import SimpleNamespace
from typing import Any


class Errors:
    def __init__(self) -> None:
        self.reported: list[tuple[BaseException, str]] = []
        self.notified: list[str] = []
        self.notify_result = True
        self.fired: list[str | None] = []
        self.templates: dict[str, str] = {
            "discord_startup": "started",
            "command_failed": "it failed",
            "command_no_permission": "not allowed",
        }
        self.calls: list[str] = []


def interaction(command: str | None = "birthday set", done: bool = False) -> Any:
    sent: list[tuple[str, str, bool]] = []

    async def send_message(text: str, *, ephemeral: bool) -> None:
        sent.append(("response", text, ephemeral))

    async def followup(text: str, *, ephemeral: bool) -> None:
        sent.append(("followup", text, ephemeral))

    return SimpleNamespace(
        command=SimpleNamespace(qualified_name=command) if command else None,
        response=SimpleNamespace(is_done=lambda: done, send_message=send_message),
        followup=SimpleNamespace(send=followup),
        sent=sent,
    )
