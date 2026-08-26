FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md alembic.ini ./
COPY app ./app
COPY alembic ./alembic
RUN pip install --no-cache-dir .

EXPOSE 8000

# Migrations are not run here: each FastAPI instance running its own
# `alembic upgrade head` independently on startup races the other on a
# fresh database (both try to checkfirst-then-create the alembic_version
# table, and one loses with a UniqueViolationError). See docker-compose.yml's
# dedicated `migrate` one-off service, which runs `alembic upgrade head`
# exactly once before either fastapi-1 or fastapi-2 starts serving.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
