from datetime import UTC

from valmal.db.base import utc_now


def test_the_models_python_side_timestamp_is_aware_utc() -> None:
    """A naive one would be read back as local time by a timestamptz column."""
    assert utc_now().tzinfo is UTC
