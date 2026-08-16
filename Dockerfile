FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY configs ./configs
RUN pip install --no-cache-dir .
USER 65532:65532
ENTRYPOINT ["mlink-demo"]
CMD ["--output", "/tmp/mlink-output", "--records", "25"]

