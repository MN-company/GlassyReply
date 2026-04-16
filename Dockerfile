FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DATA_DIR=/app/data \
    STATE_DB_PATH=/app/data/state.db \
    GMAIL_TOKEN_PATH=/app/data/token.json \
    GMAIL_CREDENTIALS_PATH=/app/data/credentials.json \
    HOST=0.0.0.0 \
    PORT=8080

WORKDIR /app

RUN useradd --create-home --shell /usr/sbin/nologin appuser

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/data && chown -R appuser:appuser /app

USER appuser

EXPOSE 8080
VOLUME ["/app/data"]

CMD ["python", "tg_email.py", "--mode", "polling"]

