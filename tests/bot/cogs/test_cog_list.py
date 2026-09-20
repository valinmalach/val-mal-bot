import importlib.util
from pathlib import Path

from valmal.bot.cogs import COGS

ROOT = Path(__file__).resolve().parents[3]


def test_every_cog_module_is_registered() -> None:
    """Nothing auto-discovers a cog: one missing from COGS is never loaded."""
    modules = {
        f"valmal.bot.cogs.{path.stem}"
        for path in (ROOT / "valmal" / "bot" / "cogs").glob("*.py")
        if path.stem != "__init__"
    }

    assert modules == set(COGS)


def test_every_registered_cog_names_a_module_that_exists() -> None:
    for name in COGS:
        assert importlib.util.find_spec(name) is not None, name


def test_no_cog_is_registered_twice() -> None:
    assert len(COGS) == len(set(COGS))
