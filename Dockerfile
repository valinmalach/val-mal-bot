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

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project

COPY . .
RUN chmod +x start.sh

CMD ["/app/start.sh"]
