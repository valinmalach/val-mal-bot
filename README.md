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

## Reauthorizing Twitch accounts

The `oauth_token` table has three identities. The `app` row is minted through
Twitch's client-credentials flow and correctly has no refresh token. The `user`
and `broadcaster` rows come from Twitch's authorization-code flow and each need
an access token and refresh token.

Before reauthorizing, register both exact production callback URLs in the
[Twitch developer console](https://dev.twitch.tv/console/apps):

```text
<APP_URL>/twitch/oauth/callback
<APP_URL>/twitch/oauth/callback/broadcaster
```

Run `/twitch-auth` in Discord as the configured `owner_id`. It returns two
ephemeral, ten-minute links:

1. **User:** authorize the bot/moderator Twitch account whose ID is stored in
   `twitch_bot_user_id`. The bot sends that ID as `moderator_id`, so Twitch
   requires it to match this access token.
2. **Broadcaster:** authorize the channel-owner Twitch account whose ID is
   stored in `twitch_broadcaster_id`. Twitch requires the ad-schedule token's
   user ID to match this broadcaster ID.

Use a private browser window or sign out between grants if the accounts differ.
Both links request the scopes in the `twitch_app_scopes` app setting and force
Twitch to show the authorization step. Before storing anything, the callback
uses Twitch's validation endpoint to reject the wrong account, client ID, or an
incomplete scope grant. A successful callback upserts only its corresponding
`user` or `broadcaster` row; it does not change the working `app` row.

The implementation follows Twitch's current documentation for the
[authorization-code flow](https://dev.twitch.tv/docs/authentication/getting-tokens-oauth/#authorization-code-grant-flow),
[token validation](https://dev.twitch.tv/docs/authentication/validate-tokens/),
[refresh-token rotation](https://dev.twitch.tv/docs/authentication/refresh-tokens/),
[shoutout authorization](https://dev.twitch.tv/docs/api/reference/#send-a-shoutout),
and [ad-schedule authorization](https://dev.twitch.tv/docs/api/reference/#get-ad-schedule).

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
