# =============================================================================
# DocWeave Graph Updater Service - Production Dockerfile
# =============================================================================
# Multi-stage build for optimized production image

# Stage 1: Build dependencies
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python dependencies
COPY services/graph-updater/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt


# Stage 2: Production image
FROM python:3.11-slim as production

# Security: Run as non-root user
RUN groupadd -r docweave && useradd -r -g docweave docweave

WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy shared modules
COPY shared /app/shared

# Copy service code
COPY services/graph-updater /app/services/graph_updater

# Set ownership
RUN chown -R docweave:docweave /app

# Switch to non-root user
USER docweave

# Environment configuration
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Production settings
ENV ENVIRONMENT=production
ENV LOG_LEVEL=INFO
ENV LOG_FORMAT=json

# Service configuration
ENV SERVICE_NAME=graph-updater
ENV SERVICE_PORT=8004

# Expose service port
EXPOSE 8004

# Health check with proper timeout and retries for production
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8004/health', timeout=5)" || exit 1

# Labels for container metadata
LABEL maintainer="DocWeave Team"
LABEL version="0.4.0"
LABEL description="DocWeave Graph Updater Service - Knowledge Graph and Conflict Resolution"
LABEL org.opencontainers.image.source="https://github.com/docweave/docweave"

# Run the service with proper production settings
# - Workers: 1 per service (scale with replicas, not workers for async)
# - Timeout: 120 seconds for long-running operations
# - Keep-alive: 5 seconds for connection pooling
CMD ["uvicorn", "services.graph_updater.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8004", \
     "--workers", "1", \
     "--timeout-keep-alive", "5", \
     "--limit-max-requests", "10000", \
     "--access-log", \
     "--proxy-headers", \
     "--forwarded-allow-ips", "*"]
