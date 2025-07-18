/**
 * Test Configuration and Setup
 * Centralized configuration for all test environments
 */

const path = require('path');
const fs = require('fs-extra');

class TestConfig {
    constructor () {
        this.baseDir = path.join(__dirname, '..');
        this.tempDir = path.join(this.baseDir, 'temp');
        this.testDataDir = path.join(this.tempDir, 'data');
        this.testUploadsDir = path.join(this.tempDir, 'uploads');
        this.testProcessedDir = path.join(this.tempDir, 'processed');
        this.testLogsDir = path.join(this.tempDir, 'logs');

        this.testTimeouts = {
            unit: 5000,      // 5 seconds
            integration: 15000, // 15 seconds
            e2e: 60000       // 60 seconds
        };

        this.coverageThresholds = {
            statements: 80,
            branches: 80,
            functions: 80,
            lines: 80
        };

        this.testDataSizes = {
            small: 10,
            medium: 100,
            large: 1000,
            xlarge: 10000
        };

        this.agriculturalDataRanges = {
            temperature: { min: -10, max: 50, unit: '°C' },
            humidity: { min: 0, max: 100, unit: '%' },
            soil_moisture: { min: 0, max: 100, unit: '%' },
            ph_level: { min: 4, max: 9, unit: 'pH' },
            light_intensity: { min: 0, max: 2000, unit: 'lux' },
            battery_level: { min: 0, max: 100, unit: '%' }
        };

        this.sensorIdPatterns = {
            valid: /^[a-zA-Z0-9_-]+$/,
            typical: ['sensor_001', 'field_A_temp_01', 'humidity_sensor_123']
        };

        this.testEnvironments = {
            unit: {
                database: 'memory',
                logging: 'error',
                features: ['basic']
            },
            integration: {
                database: 'test_db',
                logging: 'warn',
                features: ['basic', 'file_ops', 'api']
            },
            e2e: {
                database: 'test_db',
                logging: 'info',
                features: ['basic', 'file_ops', 'api', 'full_pipeline']
            }
        };
    }

    /**
     * Initialize test environment
     * @param {string} environment - Test environment (unit, integration, e2e)
     */
    async initializeTestEnvironment(environment = 'unit') {
        const config = this.testEnvironments[environment];
        if (!config) {
            throw new Error(`Unknown test environment: ${environment}`);
        }

        // Create necessary directories
        await this.createTestDirectories();

        // Set environment variables
        process.env.NODE_ENV = 'test';
        process.env.TEST_ENVIRONMENT = environment;
        process.env.LOG_LEVEL = config.logging;
        process.env.TEST_DATA_DIR = this.testDataDir;
        process.env.TEST_UPLOADS_DIR = this.testUploadsDir;

        console.log(`Test environment initialized: ${environment}`);
    }

    /**
     * Create test directories
     */
    async createTestDirectories() {
        const directories = [
            this.tempDir,
            this.testDataDir,
            this.testUploadsDir,
            this.testProcessedDir,
            this.testLogsDir
        ];

        for (const dir of directories) {
            await fs.ensureDir(dir);
        }
    }

    /**
     * Clean up test environment
     */
    async cleanupTestEnvironment() {
        try {
            await fs.remove(this.tempDir);
            console.log('Test environment cleaned up');
        } catch (error) {
            console.warn('Failed to clean up test environment:', error.message);
        }
    }

    /**
     * Get test data configuration for specific scenario
     * @param {string} scenario - Test scenario name
     * @returns {Object} Test data configuration
     */
    getTestDataConfig(scenario) {
        const configs = {
            'valid_agricultural_data': {
                size: this.testDataSizes.medium,
                sensors: ['field_A_temp_001', 'field_A_humid_001', 'field_B_soil_001'],
                readingTypes: ['temperature', 'humidity', 'soil_moisture'],
                timeRange: { hours: 24 },
                quality: 'high'
            },
            'data_with_quality_issues': {
                size: this.testDataSizes.medium,
                sensors: ['sensor_001', 'sensor_002'],
                readingTypes: ['temperature', 'humidity'],
                missingValues: 0.15, // 15% missing
                outliers: 0.05, // 5% outliers
                quality: 'medium'
            },
            'large_dataset': {
                size: this.testDataSizes.large,
                sensors: Array.from({ length: 20 }, (_, i) => `sensor_${i.toString().padStart(3, '0')}`),
                readingTypes: ['temperature', 'humidity', 'soil_moisture', 'ph_level'],
                timeRange: { hours: 168 }, // 1 week
                quality: 'high'
            },
            'edge_case_data': {
                size: this.testDataSizes.small,
                sensors: ['special_sensor_!@#'],
                readingTypes: ['temperature'],
                extremeValues: true,
                quality: 'low'
            }
        };

        return configs[scenario] || configs['valid_agricultural_data'];
    }

    /**
     * Get performance benchmarks
     * @param {string} operation - Operation name
     * @returns {Object} Performance benchmarks
     */
    getPerformanceBenchmarks(operation) {
        const benchmarks = {
            'data_ingestion': {
                recordsPerSecond: 1000,
                maxMemoryMB: 100,
                maxLatencyMs: 100
            },
            'data_transformation': {
                recordsPerSecond: 500,
                maxMemoryMB: 200,
                maxLatencyMs: 200
            },
            'anomaly_detection': {
                recordsPerSecond: 200,
                maxMemoryMB: 150,
                maxLatencyMs: 500
            },
            'quality_validation': {
                recordsPerSecond: 300,
                maxMemoryMB: 100,
                maxLatencyMs: 300
            },
            'full_pipeline': {
                recordsPerSecond: 100,
                maxMemoryMB: 500,
                maxLatencyMs: 5000
            }
        };

        return benchmarks[operation] || benchmarks['full_pipeline'];
    }

    /**
     * Get API endpoint configurations for testing
     * @returns {Object} API endpoint configurations
     */
    getAPIEndpoints() {
        return {
            data: {
                getData: { method: 'GET', path: '/api/data' },
                getProcessedData: { method: 'GET', path: '/api/data/processed' },
                getQualityReport: { method: 'GET', path: '/api/data/quality-report' },
                getSummary: { method: 'GET', path: '/api/data/summary' }
            },
            upload: {
                uploadFile: { method: 'POST', path: '/api/upload' },
                getUploadHistory: { method: 'GET', path: '/api/upload/history' }
            },
            pipeline: {
                runPipeline: { method: 'POST', path: '/api/pipeline/run' },
                getPipelineStatus: { method: 'GET', path: '/api/pipeline/status' },
                getPipelineMetrics: { method: 'GET', path: '/api/pipeline/metrics' }
            },
            status: {
                getSystemStatus: { method: 'GET', path: '/api/status' },
                getHealthCheck: { method: 'GET', path: '/api/status/health' }
            }
        };
    }

    /**
     * Get expected response schemas for validation
     * @returns {Object} Response schemas
     */
    getResponseSchemas() {
        return {
            dataResponse: {
                success: 'boolean',
                data: 'array',
                pagination: 'object'
            },
            qualityReport: {
                success: 'boolean',
                report: 'object',
                'report.summary': 'object',
                'report.details': 'object'
            },
            pipelineStatus: {
                success: 'boolean',
                status: 'object',
                'status.isRunning': 'boolean',
                'status.currentStage': 'string'
            },
            uploadResponse: {
                success: 'boolean',
                message: 'string',
                fileInfo: 'object'
            },
            errorResponse: {
                success: 'boolean',
                error: 'string'
            }
        };
    }

    /**
     * Generate test report configuration
     * @returns {Object} Test report configuration
     */
    getTestReportConfig() {
        return {
            outputDir: path.join(this.baseDir, 'test-results'),
            formats: ['html', 'json', 'junit'],
            coverage: {
                enabled: true,
                formats: ['text', 'lcov', 'html'],
                directory: path.join(this.baseDir, 'coverage')
            },
            performance: {
                enabled: true,
                outputFile: 'performance-report.json'
            }
        };
    }

    /**
     * Get database test configuration
     * @param {string} environment - Test environment
     * @returns {Object} Database configuration
     */
    getDatabaseConfig(environment = 'unit') {
        const configs = {
            unit: {
                type: 'memory',
                connection: ':memory:',
                pool: false
            },
            integration: {
                type: 'file',
                connection: path.join(this.tempDir, 'test.db'),
                pool: { min: 1, max: 5 }
            },
            e2e: {
                type: 'file',
                connection: path.join(this.tempDir, 'e2e-test.db'),
                pool: { min: 2, max: 10 }
            }
        };

        return configs[environment];
    }

    /**
     * Get mock data generators configuration
     * @returns {Object} Mock data configuration
     */
    getMockDataConfig() {
        return {
            sensors: {
                temperature: {
                    idPrefix: 'temp_',
                    valueRange: this.agriculturalDataRanges.temperature,
                    accuracy: 0.1
                },
                humidity: {
                    idPrefix: 'humid_',
                    valueRange: this.agriculturalDataRanges.humidity,
                    accuracy: 1
                },
                soil_moisture: {
                    idPrefix: 'soil_',
                    valueRange: this.agriculturalDataRanges.soil_moisture,
                    accuracy: 1
                }
            },
            timePatterns: {
                hourly: { interval: 3600000 }, // 1 hour in ms
                daily: { interval: 86400000 }, // 24 hours in ms
                continuous: { interval: 300000 } // 5 minutes in ms
            },
            anomalyPatterns: {
                spike: { factor: 3, duration: 1 },
                drift: { factor: 1.5, duration: 10 },
                missing: { probability: 0.05 }
            }
        };
    }
}

module.exports = new TestConfig();
