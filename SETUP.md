# Agricultural Pipeline - Setup & Run Instructions

## 🚀 Quick Start Guide

This guide will help you set up and run the AgR Pipeline system on your local machine or production environment.

## Prerequisites

Before installing the AgR Pipeline, ensure your system meets these requirements:

### System Requirements

- **Node.js 18+** (recommended: use nvm for version management)
- **npm 8+** or **yarn 1.22+**
- **Git** for version control
- **PowerShell** (Windows) or **Bash** (Linux/macOS)

## Installation Methods

### Method 1: Development Setup (Recommended for Development)

#### 1. Clone Repository

```bash
# Clone the repository
git clone https://github.com/your-username/agr-pipeline.git
cd agr-pipeline

# Verify project structure
ls -la
# Should show: backend/ frontend/ package.json README.md
```

#### 2. Install Dependencies

```bash
# Install all dependencies (backend + frontend)
npm run install:all

# OR install individually
cd backend && npm install
cd ../frontend && npm install
cd .. && npm install
```

#### 4. Start Development Environment

```bash
# Start both services concurrently (recommended)
cd agr-pipeline && npm run dev
# This starts both backend (port 1508) and frontend (port 5173)
```

#### 5. Verify Installation

Open your browser and check:

1. **Frontend Dashboard**: http://localhost:5173

   - Should show AgR Pipeline Dashboard with navigation menu

2. **Backend API Health**: http://localhost:1508/health

   - Should return: `{"status":"healthy","timestamp":"...","service":"Agr Pipeline API"}`

3. **API Status**: http://localhost:1508/api/status
   - Should return pipeline status JSON

## Directory Structure Setup

The system automatically creates necessary directories on first run:

```
agr-pipeline/
├── backend/
│   ├── data/                    # Auto-created
│   │   ├── raw/                # Input Parquet files
│   │   ├── processed/          # Partitioned output
│   │   ├── checkpoints/        # Processing state
│   │   ├── logs/              # System logs
│   │   └── temp/              # Temporary files
│   ├── src/                    # Source code
│   └── package.json
├── frontend/
│   ├── src/                    # React source
│   ├── dist/                   # Built files (after build)
│   └── package.json
└── package.json               # Root package
```

## Available Scripts

### Root Level Scripts

```bash
# Installation
npm run install           # Install all dependencies

# Development
npm run dev                    # Start both services [RECOMMENDED]
npm run dev:backend            # Start backend only
npm run dev:frontend           # Start frontend only

# Production
npm run build                  # Build frontend for production
npm run start                  # Start production server (single-port)
npm run start:backend          # Start backend only
npm run start:frontend         # Start frontend only (development server)

# Testing
npm test                       # Run all tests
npm run test:backend          # Run backend tests
npm run lint                  # Lint frontend code

```

## Configuration Options

### Backend Configuration

The backend uses sensible defaults but can be configured via environment variables:

```bash
# Optional environment variables
PORT=1508                     # Server port (default: 1508)
NODE_ENV=development          # Environment mode
CORS_ORIGIN=*                 # CORS origin (default: all)
```

### Data Processing Configuration

Configuration is handled in `backend/src/config/app.js`:

```javascript
// Processing settings
processing: {
    batchSize: 10000,         // Records per batch
    outlierThreshold: 3,      // Standard deviations for outlier detection
    compressionType: 'SNAPPY' // Parquet compression
}

// Data quality thresholds
qualityThresholds: {
    maxMissingPercentage: 10.0,   // Max missing data allowed
    maxAnomalyPercentage: 5.0,    // Max anomalies allowed
    maxTimeGapHours: 24           // Max time gap in data
}
```

## Troubleshooting

### Common Issues

#### Port Already in Use

```bash
# Kill process on port 1508
npx kill-port 1508

# Or use different port
PORT=3001 npm run dev:backend
```

#### Dependencies Issues

```bash
# Clear cache and reinstall
npm run clean
npm run install:all
```

### Debug Mode

Enable debug logging:

```bash
# Backend debug mode
cd backend
DEBUG=agr:* npm run dev

# Frontend verbose mode
cd frontend
npm run dev -- --verbose
```

### Performance Issues

If you experience slow processing:

1. **Increase memory allocation:**

   ```bash
   NODE_OPTIONS="--max-old-space-size=4096" npm run dev:backend
   ```

2. **Reduce batch size** in `backend/src/config/app.js`:

   ```javascript
   processing: {
     batchSize: 5000; // Reduced from 10000
   }
   ```

### Function Not Found Errors

If you encounter errors like:

- `dataIngestion.validateDataWithDuckDB is not a function`
- `dataStorage.writeParquetPartitioned is not a function`

This indicates missing method exports. The following fixes have been implemented:

1. **DataIngestionService**: Added `validateDataWithDuckDB` method for data validation
2. **DataStorageService**: Added `writeParquetPartitioned` method for partitioned storage

If errors persist, restart the backend service:

```bash
# Stop and restart backend
cd backend
npm run dev
```

### Data Processing Issues

If data processing fails:

1. **Check data directory structure:**

   ```bash
   # Ensure raw data directory exists
   mkdir -p backend/data/raw

   # Verify parquet files are present
   ls backend/data/raw/*.parquet
   ```

2. **Test API endpoints:**

   ```bash
   # Health check
   curl http://localhost:1508/health

   # Pipeline status
   curl http://localhost:1508/api/status
   ```

---

**Next**: [Architecture Overview](ARCHITECTURE.md) | [Calibration & Anomaly Detection](CALIBRATION_AND_ANOMALY.md)
