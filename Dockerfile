FROM python:3.12-slim

ARG INSTALL_PLAYWRIGHT=0
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
 && if [ "$INSTALL_PLAYWRIGHT" = "1" ]; then \
      pip install --no-cache-dir playwright==1.48.* && playwright install --with-deps chromium; \
    fi

COPY app ./app
COPY scripts ./scripts

CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]
