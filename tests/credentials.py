"""The four credentials, each with a value of its own.

They were all "test", so a mutation that signed with the client secret instead of the
webhook secret, or sent the client id where the secret goes, still passed. Every
test that checks which credential went where reads it from here.
"""

DISCORD_TOKEN = "discord-token-value"
CLIENT_ID = "twitch-client-id-value"
CLIENT_SECRET = "twitch-client-secret-value"
WEBHOOK_SECRET = "twitch-webhook-secret-value"
