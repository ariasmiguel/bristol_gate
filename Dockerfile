# Bristol Gate - Financial Data Pipeline
# Multi-stage Docker build for optimal image size and security

# Build stage - Install dependencies and package
FROM python:3.11-slim AS builder

# Set working directory
WORKDIR /app

# Install system dependencies required for building Python packages
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy package files for installation
COPY pyproject.toml .
COPY src_pipeline/ src_pipeline/

# Install the Bristol Gate package to system site-packages
RUN pip install --no-cache-dir .

# Production stage - Minimal runtime image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Install runtime dependencies first (as root)
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    chromium \
    chromium-driver \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Set Chrome/Chromium environment variables
ENV CHROME_BIN=/usr/bin/chromium \
    CHROME_DRIVER_PATH=/usr/bin/chromedriver

# Copy Python environment and packages from builder stage
COPY --from=builder /usr/local/lib/python3.11 /usr/local/lib/python3.11
COPY --from=builder /usr/local/bin /usr/local/bin

# Set working directory
WORKDIR /app

# Copy application scripts and configuration
COPY scripts/ scripts/
COPY data/ data/
COPY sql/ sql/
COPY env.example .env

# Make scripts executable
RUN chmod +x scripts/*.sh

# Create necessary directories with proper permissions
RUN mkdir -p data/bronze data/silver data/gold logs downloads

# Create non-root user for security and set ownership
RUN groupadd -r bristol && useradd -r -g bristol bristol && \
    chown -R bristol:bristol /app

# Switch to non-root user
USER bristol

# Expose port for potential web interface (future enhancement)
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import src_pipeline; print('Bristol Gate is healthy')" || exit 1

# Default command - run setup validation
CMD ["python", "-c", "from src_pipeline.pipelines.data_collection import DataCollectionPipeline; from src_pipeline.core.config_manager import ConfigurationManager; print('✅ Bristol Gate Docker container is ready!')"] 