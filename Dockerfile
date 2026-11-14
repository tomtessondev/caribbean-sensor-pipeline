FROM python:3.12-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .

# One image, three roles: `migrate`, `ingest`, `simulate` (see docker-compose.yml).
ENTRYPOINT ["sensor-pipeline"]
CMD ["--help"]
