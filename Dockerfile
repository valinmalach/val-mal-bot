FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

WORKDIR /app

# Railway streams stdout; without this the logs arrive in block-sized bursts.
ENV PYTHONUNBUFFERED=1

# Install system utilities needed for loclx
RUN apt-get update && apt-get install -y --no-install-recommends curl unzip && \
    curl -s https://loclx.io/dl/loclx-linux-amd64.zip -o loclx.zip && \
    unzip loclx.zip -d /usr/local/bin/ && \
    chmod +x /usr/local/bin/loclx && \
    rm loclx.zip && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies using uv
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project

# Copy project files (including alembic configs, seed.py, and parquet files if committed)
COPY . .

# Grant execute permissions to the startup script
RUN chmod +x start.sh

CMD ["/app/start.sh"]
