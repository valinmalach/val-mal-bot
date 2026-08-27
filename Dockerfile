# The bookworm tags are frozen at an old uv whose newest 3.14 cannot satisfy
# requires-python, so this follows trixie instead.
FROM ghcr.io/astral-sh/uv:0.12.6-python3.14-trixie-slim

WORKDIR /app

# Railway streams stdout; without this the logs arrive in block-sized bursts.
ENV PYTHONUNBUFFERED=1

# Otherwise uv downloads the newest interpreter satisfying requires-python,
# which can be a pre-release with no wheels for the compiled dependencies.
ENV UV_PYTHON_PREFERENCE=only-system

# The official image ships loclx statically linked, so this needs no apt-get.
COPY --from=localxpose/localxpose:latest /ko-app/loclx /usr/local/bin/loclx

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project

COPY . .
RUN chmod +x start.sh

CMD ["/app/start.sh"]
