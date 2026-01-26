FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"

COPY pyproject.toml .python-version uv.lock ingest_data.py ingest_zones.py ./
RUN uv sync --locked

# Use python as entrypoint, default to ingest_data.py
ENTRYPOINT ["python"]
CMD ["ingest_data.py"]