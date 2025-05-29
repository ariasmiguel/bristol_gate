# Bristol Gate - Financial Data Pipeline
# Multi-stage Docker build for optimal image size and security

# Build stage - Install dependencies
FROM python:3.11-slim as builder

# Set working directory
WORKDIR /app

# Install system dependencies required for building Python packages
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --user -r requirements.txt

# Production stage - Minimal runtime image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/root/.local/bin:$PATH"

# Create non-root user for security
RUN groupadd -r bristol && useradd -r -g bristol bristol

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    chromium \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

# Set Chrome/Chromium environment variables
ENV CHROME_BIN=/usr/bin/chromium \
    CHROME_DRIVER_PATH=/usr/bin/chromedriver

# Set working directory
WORKDIR /app

# Copy Python dependencies from builder stage
COPY --from=builder /root/.local /root/.local

# Copy application code
COPY . .

# Create necessary directories with proper permissions
RUN mkdir -p data/bronze data/silver data/gold logs downloads && \
    chown -R bristol:bristol /app

# Copy environment template
RUN cp .env.docker .env.docker

# Switch to non-root user
USER bristol

# Expose port for potential web interface (future enhancement)
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import src_pipeline; print('Bristol Gate is healthy')" || exit 1

# Default command - run a quick validation
CMD ["python", "-c", "from src_pipeline.pipelines.data_collection import DataCollectionPipeline; print('Bristol Gate Docker container is ready!')"] 