# The bookworm tags are frozen at an old uv whose newest 3.14 cannot satisfy
# requires-python, so this follows trixie instead.
FROM ghcr.io/astral-sh/uv:0.12.11-python3.14-trixie-slim

WORKDIR /app

# Railway streams stdout; without this the logs arrive in block-sized bursts.
ENV PYTHONUNBUFFERED=1

# Otherwise uv downloads the newest interpreter satisfying requires-python,
# which can be a pre-release with no wheels for the compiled dependencies.
ENV UV_PYTHON_PREFERENCE=only-system

# The official image ships loclx statically linked, so this needs no apt-get.
# Pinned by digest, not tag: this is 24.9.2, which the digest cannot say itself.
COPY --from=localxpose/localxpose@sha256:013ec01336c6444688853c8afeb9fe45dc35199ccb9de9f9c61f266af99d0158 /ko-app/loclx /usr/local/bin/loclx

# Nothing here wants root: uvicorn binds $PORT (8000 unprivileged by default),
# loclx only dials out, and no code path writes outside /app. HOME moves with
# the user so uv's cache and loclx's config have somewhere it owns; switching
# before the copies means the venv is built owned, with no chown -R layer.
RUN useradd --system --no-log-init --home-dir /app appuser && chown appuser /app
ENV HOME=/app
USER appuser

COPY --chown=appuser pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project

COPY --chown=appuser . .
RUN chmod +x start.sh

CMD ["/app/start.sh"]
