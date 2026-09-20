import asyncio
import gc
import logging

import pytest

import background
from background import fire_and_forget

pytestmark = pytest.mark.anyio


@pytest.fixture
def reports(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Exception, str]]:
    """errors.report is imported inside _finished, so it is patched at its home."""
    import errors

    seen: list[tuple[Exception, str]] = []

    async def report(exc: Exception, context: str, **_: object) -> None:
        seen.append((exc, context))

    monkeypatch.setattr(errors, "report", report)
    return seen


async def _drain() -> None:
    """Let every task started here, and the report tasks they start, finish.

    Bounded, and it says what is stuck: an unbounded wait turns a leaked task
    into a hung test run instead of a failure.
    """
    for _ in range(50):
        await asyncio.sleep(0)
        if not background._tasks:
            return
    stuck = [(t.get_name(), t.done()) for t in background._tasks]
    raise AssertionError(f"tasks never left the set: {stuck}")


async def test_the_task_is_referenced_until_it_finishes() -> None:
    release = asyncio.Event()

    async def wait() -> str:
        await release.wait()
        return "done"

    task = fire_and_forget(wait(), name="wait")

    assert task in background._tasks
    release.set()
    assert await task == "done"
    await asyncio.sleep(0)
    assert task not in background._tasks


async def test_a_task_nothing_else_holds_survives_a_collection() -> None:
    """The reason this module exists: asyncio keeps only a weak reference."""
    done = asyncio.Event()

    async def finish() -> None:
        await asyncio.sleep(0)
        done.set()

    fire_and_forget(finish(), name="finish")
    gc.collect()

    await asyncio.wait_for(done.wait(), timeout=1)


async def test_the_task_carries_its_name() -> None:
    async def noop() -> None:
        return None

    task = fire_and_forget(noop(), name="my-name")

    assert task.get_name() == "my-name"
    await task


async def test_a_failure_is_logged_and_reported_under_the_tasks_name(
    reports: list[tuple[Exception, str]], caplog: pytest.LogCaptureFixture
) -> None:
    boom = RuntimeError("boom")

    async def fail() -> None:
        raise boom

    with caplog.at_level(logging.ERROR, logger="background"):
        fire_and_forget(fail(), name="doomed")
        await _drain()

    assert reports == [(boom, "Background task doomed failed")]
    assert any("Background task %s failed" in r.msg for r in caplog.records)


@pytest.mark.usefixtures("reports")
async def test_the_report_task_is_itself_named_after_the_failure() -> None:
    async def fail() -> None:
        raise RuntimeError("x")

    fire_and_forget(fail(), name="doomed")
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    names = [t.get_name() for t in background._tasks]
    await _drain()

    assert "report-doomed" in names


async def test_success_reports_nothing(reports: list[tuple[Exception, str]]) -> None:
    async def fine() -> None:
        return None

    fire_and_forget(fine(), name="fine")
    await _drain()

    assert not reports


async def test_a_cancelled_task_reports_nothing(
    reports: list[tuple[Exception, str]],
) -> None:
    async def forever() -> None:
        await asyncio.Event().wait()

    task = fire_and_forget(forever(), name="forever")
    await asyncio.sleep(0)
    task.cancel()
    await _drain()

    assert task.cancelled()
    assert not reports
    assert task not in background._tasks


async def test_a_cancelled_tasks_own_done_callback_never_raises(
    reports: list[tuple[Exception, str]],
) -> None:
    """task.exception() raises CancelledError on a cancelled task; that would
    escape the done-callback itself, unnoticed by the two asserts above."""
    loop = asyncio.get_running_loop()
    caught: list[BaseException] = []
    original = loop.get_exception_handler()

    def catch(_loop: asyncio.AbstractEventLoop, context: dict[str, object]) -> None:
        exc = context.get("exception")
        if isinstance(exc, BaseException):
            caught.append(exc)

    loop.set_exception_handler(catch)
    try:

        async def forever() -> None:
            await asyncio.Event().wait()

        task = fire_and_forget(forever(), name="forever")
        await asyncio.sleep(0)
        task.cancel()
        await _drain()
    finally:
        loop.set_exception_handler(original)

    assert not caught


async def test_a_base_exception_is_logged_but_not_reported(
    reports: list[tuple[Exception, str]], caplog: pytest.LogCaptureFixture
) -> None:
    """The process being asked to stop is not a fault worth waking anyone for."""

    class Stop(BaseException):
        pass

    async def stop() -> None:
        raise Stop

    with caplog.at_level(logging.ERROR, logger="background"):
        task = fire_and_forget(stop(), name="stopper")
        await _drain()

    assert isinstance(task.exception(), Stop)
    assert not reports
    assert any("stopper" in r.getMessage() for r in caplog.records)


async def test_the_task_set_does_not_grow_across_many_tasks() -> None:
    async def noop() -> None:
        return None

    for _ in range(50):
        fire_and_forget(noop(), name="noop")
    await _drain()

    assert not background._tasks


def test_finishing_with_no_running_loop_only_logs(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A task finishing as the loop goes down has nowhere to schedule a report."""
    import errors

    called: list[object] = []
    monkeypatch.setattr(errors, "report", lambda *args: called.append(args))

    class Failed:
        def cancelled(self) -> bool:
            return False

        def exception(self) -> Exception:
            return RuntimeError("late")

        def get_name(self) -> str:
            return "late-task"

    with caplog.at_level(logging.ERROR, logger="background"):
        background._finished(Failed())  # pyright: ignore[reportArgumentType]

    assert not called
    assert any("late-task" in r.getMessage() for r in caplog.records)
