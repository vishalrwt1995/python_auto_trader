FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src

ENV PYTHONPATH=/app/src

# Run the async WebSocket monitor (not a web server — a long-running async process)
CMD ["python", "-m", "autotrader.services.ws_monitor_service"]
