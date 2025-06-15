# File: scrapers/entrypoint.sh

#!/bin/bash
set -e

# Activate the virtual environment
source /app/.venv/bin/activate

# Function to pass SIGINT to the child process (scrapy)
graceful_shutdown() {
  echo "Caught SIGTERM, sending SIGINT to Scrapy..."
  # Use kill to send SIGINT to the process group to ensure child processes get it
  kill -SIGINT 0
}

# Trap the SIGTERM signal and call the graceful_shutdown function
trap graceful_shutdown SIGTERM

# Execute the command passed to the container
exec "$@" &

# Wait for the process to finish
wait $!