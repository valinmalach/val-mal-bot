"""The environment Settings is tested against."""

# Names of every variable Settings reads, so a test can start from a blank slate
# rather than from whatever the developer's shell happens to export.
REQUIRED = {
    "DISCORD_TOKEN": "discord-secret-value",
    "TWITCH_CLIENT_ID": "client-id-value",
    "TWITCH_CLIENT_SECRET": "client-secret-value",
    "TWITCH_WEBHOOK_SECRET": "webhook-secret-value",
    "DATABASE_URL": "postgresql://user:hunter2@host:5432/db",
    "APP_URL": "https://bot.example",
}
OPTIONAL = ["TEST_DISCORD_TOKEN", "PORT", "DB_ECHO", "USE_TEST_BOT"]
