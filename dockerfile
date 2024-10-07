# Dockerfile for running unit tests and scripts
FROM apache/airflow:2.10.0-python3.8

# Install system dependencies as root
USER root
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Switch to the airflow user to comply with the Airflow Docker image guidelines
USER airflow

# Copy the requirements.txt file to the container
COPY --chown=airflow:airflow requirements.txt /requirements.txt

# Install Python dependencies as the airflow user
RUN pip install --no-cache-dir -r /requirements.txt

# Set the working directory
WORKDIR /opt/airflow

# Copy only necessary files for unit testing and running scripts
COPY --chown=airflow:airflow scripts/ /opt/airflow/scripts/
COPY --chown=airflow:airflow test/ /opt/airflow/test/
COPY --chown=airflow:airflow requirements.txt /opt/airflow/requirements.txt