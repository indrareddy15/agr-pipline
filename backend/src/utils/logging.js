const { app: appConfig } = require('../config');
const fileSystemUtils = require('./fileSystem');
const path = require('path');
const fs = require('fs-extra');

/**
 * Logging utilities for application events and statistics
 * Handles two types of logs:
 * 1. Agricultural Sensor Data Logs (data/logs/) - for frontend display
 * 2. System/Application Logs (systemlog.log) - for system monitoring
 */
class LoggingUtils {
    constructor () {
        // Agricultural sensor data logs (for frontend display)
        this.sensorLogFile = path.join('data', 'logs', 'sensor_data.log');

        // System/application logs (for system monitoring)
        this.systemLogFile = path.join('systemlog.log');

        this.maxLogSize = 10 * 1024 * 1024; // 10MB
        this.maxLogFiles = 5;
        this.ensureLogDirectories();
    }

    /**
     * Ensure log directories exist
     */
    async ensureLogDirectories() {
        try {
            // Ensure sensor logs directory exists
            const sensorLogDir = path.dirname(this.sensorLogFile);
            await fs.ensureDir(sensorLogDir);

            // Ensure system log file directory exists (root backend directory)
            const systemLogDir = path.dirname(this.systemLogFile);
            await fs.ensureDir(systemLogDir);
        } catch (error) {
            console.error('Error ensuring log directories:', error);
        }
    }

    /**
     * Rotate log files when they get too large
     */
    async rotateLogFile(logFilePath) {
        try {
            if (await fs.pathExists(logFilePath)) {
                const stats = await fs.stat(logFilePath);
                if (stats.size > this.maxLogSize) {
                    // Rotate existing log files
                    for (let i = this.maxLogFiles - 1; i > 0; i--) {
                        const oldFile = `${logFilePath}.${i}`;
                        const newFile = `${logFilePath}.${i + 1}`;
                        if (await fs.pathExists(oldFile)) {
                            await fs.move(oldFile, newFile, { overwrite: true });
                        }
                    }
                    // Move current log to .1
                    await fs.move(logFilePath, `${logFilePath}.1`);
                }
            }
        } catch (error) {
            console.error('Error rotating log file:', error);
        }
    }

    /**
     * Write log message to file
     */
    async writeToFile(message, logFilePath) {
        try {
            await this.ensureLogDirectories();
            await this.rotateLogFile(logFilePath);
            await fs.appendFile(logFilePath, message + '\n');
        } catch (error) {
            console.error('Error writing to log file:', error);
        }
    }
    /**
     * Log ingestion statistics to CSV file
     * @param {Object} stats - Ingestion statistics
     */
    async logIngestionStats(stats) {
        const logEntry = {
            timestamp: new Date().toISOString(),
            files_processed: stats.filesProcessed,
            files_skipped: stats.filesSkipped,
            records_ingested: stats.recordsIngested,
            records_failed: stats.recordsFailed,
            processing_time_ms: stats.processingTime
        };

        const csvLine = Object.values(logEntry).join(',') + '\n';
        const csvHeader = Object.keys(logEntry).join(',') + '\n';

        // Write header if file doesn't exist
        if (!await fileSystemUtils.pathExists(appConfig.paths.INGESTION_LOG_FILE)) {
            await fileSystemUtils.writeFile(appConfig.paths.INGESTION_LOG_FILE, csvHeader);
        }

        await fileSystemUtils.appendFile(appConfig.paths.INGESTION_LOG_FILE, csvLine);
    }

    /**
     * Log system/application messages (API requests, errors, etc.)
     * @param {string} level - Log level (info, warn, error)
     * @param {string} message - Log message
     */
    logSystem(level, message) {
        const timestamp = new Date().toISOString();
        const logMessage = `[${timestamp}] ${level.toUpperCase()}: ${message}`;

        // Write to console
        switch (level) {
            case 'error':
                console.error(logMessage);
                break;
            case 'warn':
                console.warn(logMessage);
                break;
            case 'info':
            default:
                console.log(logMessage);
                break;
        }

        // Write to system log file asynchronously
        this.writeToFile(logMessage, this.systemLogFile).catch(err =>
            console.error('Failed to write system log to file:', err)
        );
    }

    /**
     * Log sensor/agricultural data messages (for frontend display)
     * @param {string} level - Log level (info, warn, error)
     * @param {string} message - Log message
     * @param {Object} metadata - Additional metadata (sensor_id, file_name, etc.)
     */
    logSensor(level, message, metadata = {}) {
        const timestamp = new Date().toISOString();
        const metadataStr = Object.keys(metadata).length > 0
            ? ` | ${JSON.stringify(metadata)}`
            : '';
        const logMessage = `[${timestamp}] ${level.toUpperCase()}: ${message}${metadataStr}`;

        // Write to console for development
        console.log(`[SENSOR] ${logMessage}`);

        // Write to sensor log file asynchronously
        this.writeToFile(logMessage, this.sensorLogFile).catch(err =>
            console.error('Failed to write sensor log to file:', err)
        );
    }

    /**
     * Backward compatibility: Default log method routes to system log
     */
    log(level, message) {
        this.logSystem(level, message);
    }    /**
     * Log info message (system log)
     * @param {string} message - Log message
     */
    info(message) {
        this.logSystem('info', message);
    }

    /**
     * Log warning message (system log)
     * @param {string} message - Log message
     */
    warn(message) {
        this.logSystem('warn', message);
    }

    /**
     * Log error message (system log)
     * @param {string} message - Log message
     */
    error(message) {
        this.logSystem('error', message);
    }

    /**
     * Log sensor data processing info
     * @param {string} message - Log message
     * @param {Object} metadata - Additional metadata
     */
    sensorInfo(message, metadata = {}) {
        this.logSensor('info', message, metadata);
    }

    /**
     * Log sensor data processing warning
     * @param {string} message - Log message
     * @param {Object} metadata - Additional metadata
     */
    sensorWarn(message, metadata = {}) {
        this.logSensor('warn', message, metadata);
    }

    /**
     * Log sensor data processing error
     * @param {string} message - Log message
     * @param {Object} metadata - Additional metadata
     */
    sensorError(message, metadata = {}) {
        this.logSensor('error', message, metadata);
    }

    /**
     * Get sensor log files list (for frontend display)
     */
    async getLogFiles() {
        try {
            const logDir = path.dirname(this.sensorLogFile);
            const files = await fs.readdir(logDir);
            const logFiles = files
                .filter(file => file.startsWith('sensor_data.log'))
                .map(file => {
                    const filePath = path.join(logDir, file);
                    return {
                        name: file,
                        path: filePath,
                        relativePath: path.relative(process.cwd(), filePath)
                    };
                })
                .sort((a, b) => a.name.localeCompare(b.name));

            return logFiles;
        } catch (error) {
            console.error('Error getting sensor log files:', error);
            return [];
        }
    }

    /**
     * Read sensor log file content (for frontend display)
     */
    async readLogFile(filename = 'sensor_data.log', lines = 1000) {
        try {
            const logDir = path.dirname(this.sensorLogFile);
            const filePath = path.join(logDir, filename);

            if (!await fs.pathExists(filePath)) {
                return { content: '', error: 'Log file not found' };
            }

            const content = await fs.readFile(filePath, 'utf8');
            const logLines = content.split('\n').filter(line => line.trim());

            // Return last N lines
            const recentLines = logLines.slice(-lines);

            return {
                content: recentLines.join('\n'),
                totalLines: logLines.length,
                filename: filename,
                size: (await fs.stat(filePath)).size
            };
        } catch (error) {
            console.error('Error reading sensor log file:', error);
            return { content: '', error: error.message };
        }
    }

    /**
     * Get sensor log file stats (for frontend display)
     */
    async getLogStats() {
        try {
            const files = await this.getLogFiles();
            const stats = [];

            for (const file of files) {
                const stat = await fs.stat(file.path);
                stats.push({
                    name: file.name,
                    path: file.relativePath,
                    size: stat.size,
                    created: stat.birthtime,
                    modified: stat.mtime
                });
            }

            return stats;
        } catch (error) {
            console.error('Error getting sensor log stats:', error);
            return [];
        }
    }

    /**
     * Clear sensor log file (for frontend functionality)
     */
    async clearLogFile(filename) {
        try {
            const logDir = path.dirname(this.sensorLogFile);
            const filePath = path.join(logDir, filename);

            if (await fs.pathExists(filePath)) {
                await fs.writeFile(filePath, '');
                return true;
            }
            return false;
        } catch (error) {
            console.error('Error clearing sensor log file:', error);
            return false;
        }
    }

    /**
     * Generate DuckDB aggregation logs for sensor data processing
     * @param {Object} aggregationData - Results from DuckDB aggregation
     * @param {string} sourceFile - Source parquet file name
     */
    async logSensorAggregation(aggregationData, sourceFile) {
        const metadata = {
            source_file: sourceFile,
            records_processed: aggregationData.totalRecords || 0,
            sensors_count: aggregationData.sensorCount || 0,
            reading_types: aggregationData.readingTypes || [],
            processing_time_ms: aggregationData.processingTime || 0
        };

        this.sensorInfo(
            `DuckDB aggregation completed for ${sourceFile}`,
            metadata
        );

        // Log any issues found during aggregation
        if (aggregationData.issues && aggregationData.issues.length > 0) {
            aggregationData.issues.forEach(issue => {
                this.sensorWarn(`Data quality issue: ${issue.message}`, {
                    source_file: sourceFile,
                    issue_type: issue.type,
                    affected_records: issue.count
                });
            });
        }
    }
}

module.exports = new LoggingUtils();
