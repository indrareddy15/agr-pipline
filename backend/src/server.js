/**
 * Agr Pipeline RESTful API Server
 * Main application entry point for the sensor data processing pipeline API
 * 
 * This modular Agr pipeline API processes sensor data from Parquet files with the following features:
 * - RESTful API endpoints for file upload and processing
 * - Data ingestion with schema validation
 * - Data transformation including cleaning and enrichment
 * - Data quality validation and reporting
 * - Optimized data storage with partitioning and compression
 * - Incremental loading support with checkpointing
 */

const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const path = require('path');
const { createProxyMiddleware } = require('http-proxy-middleware');

const { logging } = require('./utils');
const apiRoutes = require('./routes');

// Initialize Express app
const app = express();
const PORT = process.env.PORT || 1508;
const isDevelopment = process.env.NODE_ENV !== 'production';

// Middleware
app.use(helmet());
app.use(cors());

// Increase timeout for large file uploads and processing
app.use((req, res, next) => {
    // Set timeout to 15 minutes for upload and process endpoints
    if (req.path.includes('/upload') || req.path.includes('/process')) {
        req.setTimeout(900000); // 15 minutes
        res.setTimeout(900000);
    } else if (req.path.includes('/api/data') || req.path.includes('/api/metadata')) {
        // Set timeout to 5 minutes for data endpoints
        req.setTimeout(300000); // 5 minutes
        res.setTimeout(300000);
    }
    next();
});

app.use(express.json({ limit: '100mb' }));
app.use(express.urlencoded({ extended: true, limit: '100mb' }));

// Serve static files from public directory
app.use(express.static(path.join(__dirname, '..', 'public')));

// In development mode, serve frontend source directly via Vite proxy
// In production mode, serve frontend build files
if (!isDevelopment) {
    const frontendBuildPath = path.join(__dirname, '..', '..', 'frontend', 'dist');
    app.use(express.static(frontendBuildPath));
}

/**
 * Health check endpoint
 */
app.get('/health', (req, res) => {
    res.json({
        status: 'healthy',
        timestamp: new Date().toISOString(),
        service: 'Agr Pipeline API'
    });
});

// API routes
app.use('/api', apiRoutes);

// Development mode: proxy frontend requests to Vite dev server
// Production mode: serve static files
if (isDevelopment) {
    // In development, proxy all non-API requests to Vite dev server running on port 5173
    const viteProxy = createProxyMiddleware({
        target: 'http://localhost:5173',
        changeOrigin: true,
        ws: true, // proxy websockets for HMR
        pathFilter: (pathname) => {
            // Don't proxy API routes, health check, or static assets from backend
            return !pathname.startsWith('/api') &&
                !pathname.startsWith('/health') &&
                !pathname.startsWith('/public');
        }
    });

    app.use(viteProxy);
}

// Error handling middleware
app.use((error, req, res, next) => {
    if (error.code === 'LIMIT_FILE_SIZE') {
        return res.status(400).json({
            status: 'error',
            message: 'File size too large (max 100MB)'
        });
    }

    logging.error(`Unhandled error: ${error.message}`);
    res.status(500).json({
        status: 'error',
        message: 'Internal server error',
        error: error.message
    });
});

// Handle frontend routing - serve index.html for non-API routes
app.use('*', (req, res) => {
    // If request is for API and not found, return 404
    if (req.originalUrl.startsWith('/api/')) {
        return res.status(404).json({
            status: 'error',
            message: 'API endpoint not found'
        });
    }


    if (isDevelopment) {
        // In development mode, redirect to Vite dev server or return development message
        res.json({
            status: 'development',
            message: 'Frontend is served via Vite dev server. Please access the frontend at the appropriate URL.',
            backend_api: `http://localhost:${PORT}/api`,
            note: 'This is the backend API server. The frontend should be accessed through Vite dev server.'
        });
    } else {
        // For production, serve the frontend index.html
        const indexPath = path.join(__dirname, '..', '..', 'frontend', 'dist', 'index.html');
        res.sendFile(indexPath, (err) => {
            if (err) {
                // If frontend build doesn't exist, show build message
                res.status(404).json({
                    status: 'error',
                    message: 'Frontend not built. Run "npm run build:frontend" first.',
                    frontendBuildPath: indexPath
                });
            }
        });
    }
});/**
 * Start the server
 */
async function startServer() {
    try {
        // Initialize Agr Pipeline Service for directory setup
        const ETLPipelineService = require('./services/etlPipeline');
        const etlPipeline = new ETLPipelineService();
        await etlPipeline.initializeDirectories();

        // Start the server
        app.listen(PORT, () => {
            logging.info(`Agr Pipeline API Server running on port ${PORT}`);
            logging.info(`Frontend UI: http://localhost:${PORT}/`);
            logging.info(`Health check: http://localhost:${PORT}/health`);
            logging.info(`API Status: http://localhost:${PORT}/api/status`);
        });
    } catch (error) {
        logging.error(`Failed to start server: ${error.message}`);
        process.exit(1);
    }
}

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
    logging.error(`Uncaught Exception: ${error.message}`);
    process.exit(1);
});

// Handle unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
    logging.error(`Unhandled Rejection at: ${promise}, reason: ${reason}`);
    process.exit(1);
});

// Start the application
if (require.main === module) {
    startServer();
}

module.exports = { app, startServer };
