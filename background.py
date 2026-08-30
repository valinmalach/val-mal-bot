"""Strong references for tasks nothing awaits.

asyncio holds only a weak reference to a running task, so one that nothing else
keeps can be collected before it finishes. A task started here is referenced
until it ends, and a failure is logged when it happens rather than whenever the
task is eventually collected.
"""

import asyncio
import logging
from collections.abc import Coroutine
from typing import Any

logger = logging.getLogger(__name__)

_tasks: set[asyncio.Task] = set()


def fire_and_forget(
    coro: Coroutine[Any, Any, Any], name: str | None = None
) -> asyncio.Task:
    """Start a coroutine nobody awaits, keeping it alive until it completes."""
    task = asyncio.create_task(coro, name=name)
    _tasks.add(task)
    task.add_done_callback(_finished)
    return task


def _finished(task: asyncio.Task) -> None:
    _tasks.discard(task)
    if task.cancelled():
        return
    error = task.exception()
    if error is None:
        return

    logger.error("Background task %s failed", task.get_name(), exc_info=error)

    if not isinstance(error, Exception):
        # A BaseException that is not an Exception is the process being asked to
        # stop, not a fault worth waking anyone for.
        return

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # Called as the loop goes down; the log line is all there can be.
        return

    # The last catch for a task whose own handler did not run, or did not
    # survive. report never raises, so the task started here cannot come back
    # through this branch.
    from errors import report

    fire_and_forget(
        report(error, f"Background task {task.get_name()} failed"),
        name=f"report-{task.get_name()}",
    )
