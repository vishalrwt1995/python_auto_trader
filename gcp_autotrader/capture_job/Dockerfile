FROM python:3.13-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY capture_new_data.py .
# Captures "today" by default; Cloud Scheduler triggers on trading days (Mon-Fri EOD).
ENTRYPOINT ["python", "capture_new_data.py"]
