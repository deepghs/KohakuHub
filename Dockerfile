FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install uv
RUN pip install --no-cache-dir uv

WORKDIR /app

COPY ./pyproject.toml .
RUN mkdir -p /app/src/kohakuhub
RUN echo "" > /app/src/kohakuhub/__init__.py
RUN uv pip install --system -e .

COPY ./src/kohakuhub ./src/kohakuhub
COPY ./scripts ./scripts
COPY ./docker/startup.py /app/startup.py
COPY ./docker/worker_startup.py /app/docker/worker_startup.py
RUN chmod +x /app/startup.py

EXPOSE 48888
EXPOSE 9108
CMD ["/app/startup.py"]
