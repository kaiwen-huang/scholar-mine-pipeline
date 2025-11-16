# app/frontend/Dockerfile
# ---- Base image ----
FROM python:3.11-slim

# ---- Set working directory ----
WORKDIR /app

# ---- Install dependencies ----
# Install system deps for confluent-kafka (librdkafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# ---- Copy project files ----
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# ---- Environment ----
ENV PORT=8080
EXPOSE 8080

# ---- Entrypoint ----
CMD ["python", "main.py"]
