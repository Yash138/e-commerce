# syntax=docker/dockerfile:1

FROM python:3.12-slim

# 1. System packages needed for psycopg2 (and other compiled deps)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
         build-essential \
         libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2. Copy and install Python dependencies
COPY requirements.txt .
RUN python -m venv .venv \
    && . .venv/bin/activate \
    && pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# 3. Copy project files
COPY . .

# 4. Copy and point to your Docker-specific env file
COPY .env.docker .env
ENV ENV_FILE=.env

# 5. Ensure we always use the venv when running commands
ENV PATH="/app/.venv/bin:$PATH"

# 6. (Optional) expose if you run Scrapyd or any HTTP service
EXPOSE 6800

# Make /bin/bash the default entrypoint
ENTRYPOINT ["/bin/bash", "-lc"]

# Default command: just keep the container alive
CMD ["tail -f /dev/null"]
