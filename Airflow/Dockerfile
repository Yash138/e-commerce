# Start from the official Airflow image.
# IMPORTANT: Use the exact same version you have in your docker-compose.yml file.
# For example, if you use 'apache/airflow:2.9.2', use that here.
ARG AIRFLOW_VERSION=2.10.0
FROM apache/airflow:${AIRFLOW_VERSION}

# Switch to the root user to install packages
USER root

# Example of installing system-level dependencies if needed in the future
# RUN apt-get update && apt-get install -y --no-install-recommends \
#         some-package \
#     && apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user
USER airflow

# Copy the requirements.txt file into the container
COPY requirements.txt /

# Install the Python packages from the requirements file
RUN pip install --no-cache-dir -r /requirements.txt