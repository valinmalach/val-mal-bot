import sys
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace

import pytest

from valmal.core import memory

STATUS = "\n".join(
    [
        "Name:\tpython",
        "VmHWM:\t  110144 kB",
        "VmRSS:\t  105840 kB",
        "RssAnon:\t   83748 kB",
        "RssFile:\t   23124 kB",
        "Threads:\t6",
        "voluntary_ctxt_switches:\t12",
    ]
)

HEAP_KEYS = {
    "heap_bytes",
    "heap_in_use_bytes",
    "heap_free_bytes",
    "heap_top_free_bytes",
    "mmapped_bytes",
}
STATUS_KEYS = {"rss_kb", "rss_peak_kb", "rss_anon_kb", "rss_file_kb", "threads"}


@pytest.fixture(autouse=True)
def _fresh_lookup() -> Iterator[None]:
    memory._mallinfo2.cache_clear()  # pyright: ignore[reportPrivateUsage]
    yield
    memory._mallinfo2.cache_clear()  # pyright: ignore[reportPrivateUsage]


@pytest.fixture
def status(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "status"
    path.write_text(STATUS)
    monkeypatch.setattr(memory, "_STATUS", path)
    return path


def no_heap() -> None:
    return None


class TestStatus:
    def test_reads_the_kernels_figures_under_plain_names(
        self, status: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(memory, "_mallinfo2", no_heap)

        found = memory.snapshot()

        assert found["rss_kb"] == 105840
        assert found["rss_peak_kb"] == 110144
        assert found["rss_anon_kb"] == 83748
        assert found["rss_file_kb"] == 23124
        assert found["threads"] == 6
        assert "voluntary_ctxt_switches" not in found

    def test_no_proc_leaves_its_keys_out_instead_of_failing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(memory, "_STATUS", tmp_path / "absent")
        monkeypatch.setattr(memory, "_mallinfo2", no_heap)

        found = memory.snapshot()

        assert STATUS_KEYS.isdisjoint(found)
        assert found["python_blocks"] > 0


class TestHeap:
    def test_names_glibcs_counters_for_what_they_hold(
        self, status: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def mallinfo2() -> memory._MallInfo2:  # pyright: ignore[reportPrivateUsage]
            return memory._MallInfo2(  # pyright: ignore[reportPrivateUsage]
                arena=30, hblkhd=2, uordblks=23, fordblks=7, keepcost=1, ordblks=99
            )

        def lookup() -> object:
            return mallinfo2

        monkeypatch.setattr(memory, "_mallinfo2", lookup)

        found = memory.snapshot()

        assert {k: found[k] for k in HEAP_KEYS} == {
            "heap_bytes": 30,
            "heap_in_use_bytes": 23,
            "heap_free_bytes": 7,
            "heap_top_free_bytes": 1,
            "mmapped_bytes": 2,
        }
        assert "ordblks" not in found

    def test_no_mallinfo2_leaves_the_heap_keys_out(
        self, status: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(memory, "_mallinfo2", no_heap)

        assert HEAP_KEYS.isdisjoint(memory.snapshot())


class TestLookup:
    def test_a_process_without_it_has_no_heap(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(memory, "ctypes", SimpleNamespace(pythonapi=object()))

        assert memory._mallinfo2() is None  # pyright: ignore[reportPrivateUsage]

    def test_glibc_returns_the_function_told_what_it_returns(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fn = SimpleNamespace(restype=None)
        monkeypatch.setattr(
            memory, "ctypes", SimpleNamespace(pythonapi=SimpleNamespace(mallinfo2=fn))
        )

        assert memory._mallinfo2() is fn  # pyright: ignore[reportPrivateUsage]
        assert fn.restype is memory._MallInfo2  # pyright: ignore[reportPrivateUsage]


@pytest.mark.skipif(sys.platform != "linux", reason="needs /proc and glibc")
def test_the_real_process_reports_every_key_with_a_consistent_heap() -> None:
    found = memory.snapshot()

    assert STATUS_KEYS | HEAP_KEYS | {"python_blocks"} <= found.keys()
    assert found["heap_in_use_bytes"] + found["heap_free_bytes"] == found["heap_bytes"]
