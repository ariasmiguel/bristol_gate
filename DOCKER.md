# Bristol Gate - Docker Guide

## 🐳 **Docker Support**

Bristol Gate now includes comprehensive Docker support for easy deployment and consistent environments across different systems.

## 🚀 **Quick Start**

### **Option 1: Docker Compose (Recommended)**
```bash
# Clone the repository
git clone https://github.com/ariasmiguel/bristol_gate.git
cd bristol_gate

# Copy environment template
cp env.example .env
# Edit .env with your API keys

# Start the complete pipeline
docker-compose up
```

### **Option 2: Docker Build**
```bash
# Build the image
docker build -t bristol-gate .

# Run the container
docker run -d \
  -e FRED_API_KEY=your_key \
  -e EIA_TOKEN=your_token \
  -v $(pwd)/data:/app/data \
  bristol-gate
```

## 📋 **What's Included**

### **Docker Services**
- **`bristol-gate`**: Main application container
- **`bristol-scheduler`**: Automated daily updates via cron
- **`setup`**: Database initialization service

### **Features**
- ✅ **Multi-stage build** for optimal image size
- ✅ **Non-root user** for security
- ✅ **Health checks** for container monitoring
- ✅ **Volume persistence** for data and logs
- ✅ **Chrome/Selenium** support for web scraping
- ✅ **Automatic scheduling** for daily updates

## 🔧 **Configuration**

### **Environment Variables**
```bash
# Required API Keys
FRED_API_KEY=your_fred_api_key
EIA_TOKEN=your_eia_api_key

# Optional Configuration
LOG_LEVEL=INFO
DB_PATH=/app/data/bristol_gate.duckdb
CHROME_DRIVER_PATH=/usr/bin/chromedriver
```

### **Volume Mounts**
```yaml
volumes:
  - bristol_data:/app/data        # Database and parquet files
  - bristol_logs:/app/logs        # Application logs
  - ./.env:/app/.env:ro          # Environment configuration
```

## 📊 **Pipeline Execution**

### **Manual Pipeline Run**
```bash
# Run complete pipeline manually
docker-compose run bristol-gate sh -c "
  python setup_duckdb.py --load-symbols &&
  python run_data_collection.py --sources yahoo,fred &&
  python run_aggregate_series.py &&
  python run_features_pipeline.py
"
```

### **Individual Components**
```bash
# Data collection only
docker-compose run bristol-gate python run_data_collection.py --sources yahoo

# Feature generation only
docker-compose run bristol-gate python run_features_pipeline.py

# Database setup
docker-compose run bristol-gate python setup_duckdb.py --load-symbols
```

## 🕐 **Scheduled Updates**

The `bristol-scheduler` service runs daily updates:
- **6:00 AM**: Data collection (`run_data_collection.py`)
- **6:30 AM**: Aggregation (`run_aggregate_series.py`)
- **7:00 AM**: Feature generation (`run_features_pipeline.py`)

### **Custom Schedule**
Modify the cron schedule in `docker-compose.yml`:
```yaml
command: >
  sh -c "
    echo '0 6 * * * cd /app && python run_data_collection.py' | crontab - &&
    # Add your custom schedule here
  "
```

## 🔍 **Monitoring & Logs**

### **View Logs**
```bash
# All services
docker-compose logs

# Specific service
docker-compose logs bristol-gate
docker-compose logs bristol-scheduler

# Follow logs in real-time
docker-compose logs -f bristol-gate
```

### **Container Health**
```bash
# Check service status
docker-compose ps

# Health check status
docker inspect bristol-gate-app | grep -A 5 "Health"
```

## 💾 **Data Management**

### **Accessing Data**
```bash
# List generated files
docker-compose exec bristol-gate ls -la data/silver/

# Copy data out of container
docker cp bristol-gate-app:/app/data/silver/featured_data.parquet ./

# Access database directly
docker-compose exec bristol-gate python -c "
import duckdb
con = duckdb.connect('/app/data/bristol_gate.duckdb')
print(con.execute('SELECT COUNT(*) FROM symbols').fetchone())
"
```

### **Backup Data**
```bash
# Backup the entire data volume
docker run --rm \
  -v bristol_gate_bristol_data:/data \
  -v $(pwd):/backup \
  ubuntu tar czf /backup/bristol_data_backup.tar.gz -C /data .
```

## 🛠️ **Development**

### **Development Mode**
```bash
# Mount source code for development
docker run -it --rm \
  -v $(pwd):/app \
  -v $(pwd)/data:/app/data \
  bristol-gate bash
```

### **Debugging**
```bash
# Interactive shell
docker-compose exec bristol-gate bash

# Python shell with imports
docker-compose exec bristol-gate python -c "
from src_pipeline.pipelines.data_collection import DataCollectionPipeline
pipeline = DataCollectionPipeline()
print('Pipeline ready for debugging')
"
```

## 🚀 **CI/CD Integration**

### **GitHub Actions**
The repository includes GitHub Actions workflow (`.github/workflows/docker-build.yml`) that:
- Builds Docker images on push to `main`/`develop`
- Pushes to Docker Hub automatically
- Tests container imports on PRs
- Supports multi-platform builds (AMD64/ARM64)

### **Required Secrets**
Add these to your GitHub repository secrets:
- `DOCKERHUB_USERNAME`: Your Docker Hub username
- `DOCKERHUB_TOKEN`: Docker Hub access token

## 📈 **Performance**

### **Resource Requirements**
- **CPU**: 2+ cores recommended
- **Memory**: 4GB RAM minimum, 8GB recommended
- **Storage**: 10GB for data and logs
- **Network**: Required for API calls and web scraping

### **Optimization Tips**
```bash
# Limit memory usage
docker-compose run --memory=4g bristol-gate python run_features_pipeline.py

# Parallel processing
docker-compose run bristol-gate python run_features_pipeline.py --workers 4

# Source filtering for testing
docker-compose run bristol-gate python run_data_collection.py --sources yahoo
```

## 🐛 **Troubleshooting**

### **Common Issues**

**Container won't start:**
```bash
# Check logs
docker-compose logs bristol-gate

# Rebuild image
docker-compose build --no-cache bristol-gate
```

**API key errors:**
```bash
# Verify environment variables
docker-compose exec bristol-gate env | grep -E "(FRED|EIA)"

# Test with minimal sources
docker-compose run bristol-gate python run_data_collection.py --sources yahoo
```

**Chrome/Selenium issues:**
```bash
# Check Chrome installation
docker-compose exec bristol-gate chromium --version

# Test Selenium utilities
docker-compose exec bristol-gate python -c "
from src_pipeline.utils.web_scraping_utils import WebScrapingUtils
print('Selenium utilities available')
"
```

**Permission issues:**
```bash
# Fix volume permissions
sudo chown -R 1000:1000 ./data

# Check container user
docker-compose exec bristol-gate whoami
```

## 🌐 **Production Deployment**

### **Docker Swarm**
```bash
# Deploy to swarm
docker stack deploy -c docker-compose.yml bristol-gate
```

### **Kubernetes**
```yaml
# Example Kubernetes deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bristol-gate
spec:
  replicas: 1
  selector:
    matchLabels:
      app: bristol-gate
  template:
    metadata:
      labels:
        app: bristol-gate
    spec:
      containers:
      - name: bristol-gate
        image: bristol-gate:latest
        env:
        - name: FRED_API_KEY
          valueFrom:
            secretKeyRef:
              name: bristol-secrets
              key: fred-api-key
```

---

**🎉 Bristol Gate is now fully containerized and ready for production deployment!**