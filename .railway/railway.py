from railway_sdk import (
    Project,
    RailwayContext,
    define_railway,
    github,
    preserve,
    project,
    service,
)

# Owns only this service, so a plan never proposes removing anything else in the
# project. See https://docs.railway.com/infrastructure-as-code#multi-repo-projects
PARTIAL = "val-mal-bot"


@define_railway
def main(ctx: RailwayContext | None = None) -> Project:
    val_mal_bot = service(
        "val-mal-bot",
        source=github("valinmalach/val-mal-bot", branch="master"),
        start="python -m alembic upgrade head && exec opentelemetry-instrument python main.py",
        # A variable missing here is deleted by the next apply; preserve() keeps
        # the value Railway holds without writing it into the repo.
        env={
            "APP_URL": preserve(),
            "DATABASE_URL": preserve(),
            "DB_ECHO": preserve(),
            "DISCORD_TOKEN": preserve(),
            "TWITCH_CLIENT_ID": preserve(),
            "TWITCH_CLIENT_SECRET": preserve(),
            "TWITCH_WEBHOOK_SECRET": preserve(),
            # Railway's OTLP receiver takes traces only.
            "OTEL_METRICS_EXPORTER": "none",
            "OTEL_LOGS_EXPORTER": "none",
        },
    )
    return project("val-mal-bot", resources=[val_mal_bot])
