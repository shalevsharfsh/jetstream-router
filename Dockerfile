# Single image, five roles. See router/main.py for why.
FROM python:3.13-slim AS base

# Dependencies in their own layer so source edits don't invalidate the install.
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY router/ ./router/

# Non-root. Nothing here needs to write to the filesystem, which lets the
# manifests also set readOnlyRootFilesystem and drop all capabilities.
RUN useradd --create-home --uid 10001 app
USER 10001

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

EXPOSE 9090
ENTRYPOINT ["python", "-m", "router.main"]
