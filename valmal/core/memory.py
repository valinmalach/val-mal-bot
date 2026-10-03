"""Where this process's memory is, as one flat mapping for a log line.

Railway bills memory and graphs only the total. This splits it into what the
kernel holds (``/proc/self/status``), what glibc's heap holds and how much of
that is freed but kept (``mallinfo2``), and how many blocks Python has
allocated, which is enough to tell growing data from an allocator holding on.
A source this platform lacks is left out rather than reported as zero.
"""

import ctypes
import functools
import sys
from collections.abc import Callable
from pathlib import Path

_STATUS = Path("/proc/self/status")

_STATUS_KEYS = {
    "VmRSS": "rss_kb",
    "VmHWM": "rss_peak_kb",
    "RssAnon": "rss_anon_kb",
    "RssFile": "rss_file_kb",
    "Threads": "threads",
}


class _MallInfo2(ctypes.Structure):
    _fields_ = [
        (name, ctypes.c_size_t)
        for name in (
            "arena",
            "ordblks",
            "smblks",
            "hblks",
            "hblkhd",
            "usmblks",
            "fsmblks",
            "uordblks",
            "fordblks",
            "keepcost",
        )
    ]


_HEAP_KEYS = {
    "arena": "heap_bytes",
    "uordblks": "heap_in_use_bytes",
    "fordblks": "heap_free_bytes",
    "keepcost": "heap_top_free_bytes",
    "hblkhd": "mmapped_bytes",
}


@functools.cache
def _mallinfo2() -> Callable[[], _MallInfo2] | None:
    # glibc 2.33+ only. pythonapi searches the whole process on Linux and only
    # Python's own DLL on Windows, so musl and Windows both come back None.
    fn = getattr(ctypes.pythonapi, "mallinfo2", None)
    if fn is None:
        return None
    fn.restype = _MallInfo2
    return fn


def _status() -> dict[str, int]:
    try:
        text = _STATUS.read_text()
    except OSError:
        return {}
    found: dict[str, int] = {}
    for line in text.splitlines():
        name, _, value = line.partition(":")
        if key := _STATUS_KEYS.get(name):
            found[key] = int(value.split()[0])
    return found


def _heap() -> dict[str, int]:
    fn = _mallinfo2()
    if fn is None:
        return {}
    info = fn()
    return {key: getattr(info, field) for field, key in _HEAP_KEYS.items()}


def snapshot() -> dict[str, int]:
    """This process's memory now, keyed by plain names with their units."""
    return _status() | _heap() | {"python_blocks": sys.getallocatedblocks()}
