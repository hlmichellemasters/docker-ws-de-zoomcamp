FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"

COPY pyproject.toml .python-version uv.lock ingest_data.py ./
RUN uv sync --locked

ENTRYPOINT ["python", "ingest_data.py"]