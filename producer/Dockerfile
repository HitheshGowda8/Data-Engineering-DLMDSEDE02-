FROM python:3.11-slim

WORKDIR /app
COPY processor/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY processor /app/processor
COPY database /app/database

ENV PYTHONPATH=/app

