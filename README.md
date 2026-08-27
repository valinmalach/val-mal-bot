# Valin Malach Bot

Discord and Twitch bot. Configuration and runtime records live in Postgres; see
[db/README.md](db/README.md) for the schema and the migration status.

## Running locally

Everything below stays on your machine: a Postgres container, the test Discord
bot, and no public tunnel.

**1. Start the database.**

```sh
docker compose up -d
```

Postgres listens on `localhost:5432`. If that port is taken, change the host
side of the mapping in `compose.yaml` and match it in `DATABASE_URL`.

**2. Point `.env` at it and at the test bot.**

```sh
DATABASE_URL=postgresql://valmal:valmal@localhost:5432/valmal
USE_TEST_BOT=1
APP_URL=http://localhost:8000
```

`USE_TEST_BOT=1` makes the bot log in with `TEST_DISCORD_TOKEN`. Without it the
real bot logs in, which is rarely what you want against a local database.

**3. Create the schema and fill it.**

```sh
uv run alembic upgrade head   # 14 tables and the configuration in them
```

Configuration arrives with the schema, in a migration, and skips what already
exists, so this is safe to repeat.

**4. Run the bot.**

```sh
uv run main.py
```

### What local mode cannot test

Twitch EventSub delivers over the public internet, so nothing reaches
`localhost`. Stream alerts, follows, raids, ad breaks and chat commands all
arrive by webhook and will not fire. Outbound Twitch API calls still work, and
the bot will mint an app token on demand.

To exercise webhooks you need the tunnel, which is what `start.sh` sets up in
the container.

### Pointing the test bot at a test guild

`guild_id` is an `app_setting` row, not a constant. To keep local runs out of
the real server, change it once the migrations have run:

```sql
UPDATE app_setting SET value = '<test guild id>' WHERE key = 'guild_id';
```

The channel and role IDs in `discord_channel` and `discord_role` need the same
treatment for a different guild.

## Checks

```sh
uvx ruff check . --exclude .venv
uvx ruff format --check . --exclude .venv
uvx pyright
```
