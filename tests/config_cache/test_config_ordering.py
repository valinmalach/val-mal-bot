"""Where the cache promises an order, it must not depend on the order the database
happened to return rows in.

Postgres guarantees none without an ORDER BY, and an UPDATE can move a row, so an
admin editing one auto-response could change which of two overlapping replies
fires.
"""

from typing import Any

import pytest

from tests.config_cache.support import role
from valmal.db.models import AutoResponseMatch, DiscordAutoResponse, DiscordEmbed

pytestmark = pytest.mark.anyio


async def test_overlapping_auto_responses_resolve_the_same_way_whatever_order_rows_arrive(
    load: Any,
) -> None:
    prefix = DiscordAutoResponse(
        id=1, trigger="hel", response="prefix", match_type=AutoResponseMatch.PREFIX
    )
    exact = DiscordAutoResponse(id=2, trigger="hello", response="exact")

    in_id_order = await load(prefix, exact)
    reversed_order = await load(exact, prefix)

    assert in_id_order.auto_response("hello") == "prefix"
    assert reversed_order.auto_response("hello") == "prefix"


async def test_embeds_at_the_same_position_are_listed_by_key(load: Any) -> None:
    for rows in ([("b", 1), ("a", 1), ("c", 0)], [("c", 0), ("a", 1), ("b", 1)]):
        cache = await load(*(DiscordEmbed(key=k, position=p) for k, p in rows))

        assert cache.embed_keys() == ["c", "a", "b"]


async def test_roles_at_the_same_position_are_listed_by_key(load: Any) -> None:
    for keys in (["b", "a"], ["a", "b"]):
        cache = await load(
            *(role(k, i, embed_key="panel", position=1) for i, k in enumerate(keys))
        )

        assert [r.key for r in cache.roles_for_embed("panel")] == ["a", "b"]
