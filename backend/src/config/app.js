const path = require('path');

/**
 * Application configuration constants and settings
 */
const config = {
    // Directory paths
    paths: {
        RAW_DATA_DIR: 'data/raw/',
        PROCESSED_DIR: 'data/processed/',
        CHECKPOINT_DIR: 'data/checkpoints/',
        CHECKPOINT_FILE: path.join('data/checkpoints/', 'processed_files.txt'),
        QUALITY_REPORT_FILE: 'data/data_quality_report.csv',
        INGESTION_LOG_FILE: 'data/ingestion_log.csv'
    },

    // Parquet schema definition
    parquetSchema: {
        sensor_id: { type: 'UTF8' },
        timestamp: { type: 'TIMESTAMP_NS' }, // Updated to match actual schema
        reading_type: { type: 'UTF8' },
        value: { type: 'DOUBLE' },
        battery_level: { type: 'DOUBLE' }
    },

    // Calibration parameters for data normalization
    calibrationParams: {
        temperature: { multiplier: 1.0, offset: 0.0 },
        humidity: { multiplier: 1.0, offset: 0.0 },
        soil_moisture: { multiplier: 1.0, offset: 0.0 },
        light_intensity: { multiplier: 1.0, offset: 0.0 }
    },

    // Expected data ranges for anomaly detection
    expectedRanges: {
        temperature: { min: -20.0, max: 60.0 },
        humidity: { min: 0.0, max: 100.0 },
        soil_moisture: { min: 0.0, max: 100.0 },
        light_intensity: { min: 0.0, max: 100000.0 }
    },

    // Processing configuration
    processing: {
        batchSize: 10000,
        outlierThreshold: 3,
        compressionType: 'SNAPPY'
    },

    // Timezone configuration for timestamp processing
    timezone: {
        utcOffset: 330, // UTC+5:30 in minutes (5*60 + 30 = 330)
        format: 'Asia/Kolkata', // Timezone name for moment.js
        iso8601_format: '+05:30', // ISO 8601 timezone offset format
        display_name: 'India Standard Time (IST)',
        description: 'All timestamps will be converted to UTC+5:30 (Asia/Kolkata timezone)'
    },

    // Data quality thresholds
    qualityThresholds: {
        maxMissingPercentage: 10.0,
        maxAnomalyPercentage: 5.0,
        maxTimeGapHours: 24
    }
};

module.exports = config;
