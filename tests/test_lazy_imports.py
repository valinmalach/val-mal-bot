"""The bot runs with -OO -X lazy_imports=all (.railway/railway.py), which nothing else
here does: an import that only fails lazily, or code that needs a docstring or an
assert, would otherwise first fail in production.

In a process of its own, because the mode is global and pytest itself does not
survive it (an ImportCycleError inside _pytest.config).
"""

from tests.support import run_python

# Every module, then every name on it: a lazy import resolves on first use, and an
# ImportCycleError is raised by the use, not by the import statement. Not the
# schema side: SQLAlchemy's ORM fails lazily, and only Alembic, which runs eager,
# imports it (the bot never does, ADR 0005).
_RESOLVE_EVERYTHING = r"""
import importlib, pathlib, re, sys
# Not assert: -OO strips it.
if (sys.get_lazy_imports(), sys.flags.optimize) != ("all", 2):
    raise SystemExit("not the production flags")
# From the files: pkgutil.walk_packages imports each package to look inside it.
schema = re.compile(r"^(import|from) (sqlalchemy|sqlmodel)\b", re.MULTILINE)
names = ["main"] + [
    ".".join(f.with_suffix("").parts).removesuffix(".__init__")
    for f in sorted(pathlib.Path("valmal").rglob("*.py"))
    if not schema.search(f.read_text(encoding="utf-8"))
]
for name in names:
    module = importlib.import_module(name)
    for attribute in dir(module):
        getattr(module, attribute)
# Resolving a name is not using it: pendulum broke on its second parse, not its
# import, so the time handling the bot leans on is run twice too.
from datetime import UTC, datetime
from valmal.bot.birthday import Months, next_birthday_on
from valmal.bot.duration import get_age
from valmal.twitch.timestamps import parse_rfc3339
for _ in range(2):
    parse_rfc3339("2026-10-12T13:01:52+00:00")
    next_birthday_on(Months.March, 8, "America/Havana", datetime(2026, 1, 1, tzinfo=UTC))
    get_age(datetime(2025, 1, 1, tzinfo=UTC))
print(len(names))
"""


def test_every_module_imports_and_resolves_under_lazy_imports() -> None:
    done = run_python("-OO", "-X", "lazy_imports=all", "-c", _RESOLVE_EVERYTHING)

    # The last line, as main's logging writes to stdout too; a sweep that found no
    # modules would pass without checking anything.
    assert int(done.stdout.splitlines()[-1]) > 50
