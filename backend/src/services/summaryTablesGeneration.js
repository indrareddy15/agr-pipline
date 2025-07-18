const path = require('path');
const { database: dbConfig } = require('../config');
const { app: appConfig } = require('../config');
const { fileSystem, logging } = require('../utils');

/**
 * Summary Tables Generation Service for analytical data storage
 */
class SummaryTablesService {
    constructor () {
        this.summaryDir = path.join(appConfig.paths.PROCESSED_DIR, 'summaries');
    }

    /**
     * Generate all summary tables from processed data
     * @param {Array<Object>} data - Array of processed sensor data
     */
    async generateAllSummaryTables(data) {
        if (!data || data.length === 0) {
            logging.logSensor('WARN', 'No data available for summary table generation');
            return;
        }

        try {
            await fileSystem.ensureDir(this.summaryDir);

            logging.logSensor('INFO', 'Generating summary tables...');

            // Generate daily summary
            await this.generateDailySummary(data);

            // Generate sensor summary  
            await this.generateSensorSummary(data);

            // Generate reading type summary
            await this.generateReadingTypeSummary(data);

            // Generate hourly summary
            await this.generateHourlySummary(data);

            // Generate battery level summary
            await this.generateBatteryLevelSummary(data);

            // Generate anomaly summary
            await this.generateAnomalySummary(data);

            logging.info('✓ All summary tables generated successfully');

        } catch (error) {
            throw new Error(`Summary table generation failed: ${error.message}`);
        }
    }

    /**
     * Generate daily summary table
     * @param {Array<Object>} data - Sensor data
     */
    async generateDailySummary(data) {
        const db = dbConfig.createInMemoryDatabase();
        const con = dbConfig.getConnection(db);

        try {
            await this.createDataTable(con, data);

            const query = `
                SELECT 
                    DATE(timestamp) as date,
                    sensor_id,
                    reading_type,
                    COUNT(*) as record_count,
                    AVG(value) as avg_value,
                    MIN(value) as min_value,
                    MAX(value) as max_value,
                    STDDEV(value) as std_value,
                    AVG(calibrated_value) as avg_calibrated_value,
                    SUM(CASE WHEN anomalous_reading THEN 1 ELSE 0 END) as anomaly_count,
                    AVG(battery_level) as avg_battery_level,
                    MIN(battery_level) as min_battery_level,
                    MAX(battery_level) as max_battery_level,
                    SUM(CASE WHEN outlier_corrected THEN 1 ELSE 0 END) as outlier_count,
                    SUM(CASE WHEN missing_value_filled THEN 1 ELSE 0 END) as filled_count,
                    AVG(daily_average) as daily_avg_value,
                    AVG(rolling_7day_average) as rolling_7day_avg,
                    MIN(timestamp) as first_reading_time,
                    MAX(timestamp) as last_reading_time
                FROM sensor_data
                GROUP BY DATE(timestamp), sensor_id, reading_type
                ORDER BY date, sensor_id, reading_type
            `;

            await this.executeAndSaveQuery(con, query, 'daily_summary.parquet');
            await dbConfig.closeConnection(con);

            logging.info('✓ Daily summary table generated');

        } catch (error) {
            await dbConfig.closeConnection(con);
            throw error;
        }
    }

    /**
     * Generate sensor summary table
     * @param {Array<Object>} data - Sensor data
     */
    async generateSensorSummary(data) {
        const db = dbConfig.createInMemoryDatabase();
        const con = dbConfig.getConnection(db);

        try {
            await this.createDataTable(con, data);

            const query = `
                SELECT 
                    sensor_id,
                    reading_type,
                    COUNT(*) as total_records,
                    AVG(value) as avg_value,
                    STDDEV(value) as std_value,
                    MIN(value) as min_value,
                    MAX(value) as max_value,
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY value) as q1_value,
                    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY value) as median_value,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY value) as q3_value,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY value) as p95_value,
                    AVG(calibrated_value) as avg_calibrated_value,
                    SUM(CASE WHEN anomalous_reading THEN 1 ELSE 0 END) as total_anomalies,
                    (SUM(CASE WHEN anomalous_reading THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as anomaly_percentage,
                    SUM(CASE WHEN outlier_corrected THEN 1 ELSE 0 END) as total_outliers_corrected,
                    (SUM(CASE WHEN outlier_corrected THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as outlier_percentage,
                    SUM(CASE WHEN missing_value_filled THEN 1 ELSE 0 END) as total_missing_filled,
                    (SUM(CASE WHEN missing_value_filled THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as missing_percentage,
                    AVG(battery_level) as avg_battery_level,
                    STDDEV(battery_level) as std_battery_level,
                    MIN(battery_level) as min_battery_level,
                    MAX(battery_level) as max_battery_level,
                    MIN(timestamp) as first_reading,
                    MAX(timestamp) as last_reading,
                    COUNT(DISTINCT DATE(timestamp)) as active_days,
                    (DATE_DIFF('day', MIN(timestamp), MAX(timestamp)) + 1) as total_days_in_range
                FROM sensor_data
                GROUP BY sensor_id, reading_type
                ORDER BY sensor_id, reading_type
            `;

            await this.executeAndSaveQuery(con, query, 'sensor_summary.parquet');
            await dbConfig.closeConnection(con);

            logging.info('✓ Sensor summary table generated');

        } catch (error) {
            await dbConfig.closeConnection(con);
            throw error;
        }
    }

    /**
     * Generate reading type summary table
     * @param {Array<Object>} data - Sensor data
     */
    async generateReadingTypeSummary(data) {
        const db = dbConfig.createInMemoryDatabase();
        const con = dbConfig.getConnection(db);

        try {
            await this.createDataTable(con, data);

            const query = `
                SELECT 
                    reading_type,
                    COUNT(*) as total_records,
                    COUNT(DISTINCT sensor_id) as unique_sensors,
                    COUNT(DISTINCT DATE(timestamp)) as unique_dates,
                    AVG(value) as overall_avg_value,
                    STDDEV(value) as overall_std_value,
                    MIN(value) as overall_min_value,
                    MAX(value) as overall_max_value,
                    PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY value) as p5_value,
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY value) as q1_value,
                    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY value) as median_value,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY value) as q3_value,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY value) as p95_value,
                    SUM(CASE WHEN anomalous_reading THEN 1 ELSE 0 END) as total_anomalies,
                    (SUM(CASE WHEN anomalous_reading THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as anomaly_percentage,
                    SUM(CASE WHEN outlier_corrected THEN 1 ELSE 0 END) as total_outliers,
                    (SUM(CASE WHEN outlier_corrected THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as outlier_percentage,
                    SUM(CASE WHEN missing_value_filled THEN 1 ELSE 0 END) as total_missing_filled,
                    (SUM(CASE WHEN missing_value_filled THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as missing_percentage,
                    MIN(timestamp) as earliest_reading,
                    MAX(timestamp) as latest_reading,
                    AVG(daily_average) as avg_daily_value,
                    AVG(rolling_7day_average) as avg_rolling_7day_value
                FROM sensor_data
                GROUP BY reading_type
                ORDER BY reading_type
            `;

            await this.executeAndSaveQuery(con, query, 'reading_type_summary.parquet');
            await dbConfig.closeConnection(con);

            logging.info('✓ Reading type summary table generated');

        } catch (error) {
            await dbConfig.closeConnection(con);
            throw error;
        }
    }

    /**
     * Generate hourly summary table
     * @param {Array<Object>} data - Sensor data
     */
    async generateHourlySummary(data) {
        const db = dbConfig.createInMemoryDatabase();
        const con = dbConfig.getConnection(db);

        try {
            await this.createDataTable(con, data);

            const query = `
                SELECT 
                    DATE(timestamp) as date,
                    HOUR(timestamp) as hour,
                    reading_type,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT sensor_id) as active_sensors,
                    AVG(value) as avg_value,
                    STDDEV(value) as std_value,
                    MIN(value) as min_value,
                    MAX(value) as max_value,
                    AVG(calibrated_value) as avg_calibrated_value,
                    SUM(CASE WHEN anomalous_reading THEN 1 ELSE 0 END) as anomaly_count,
                    (SUM(CASE WHEN anomalous_reading THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as anomaly_percentage,
                    AVG(battery_level) as avg_battery_level
                FROM sensor_data
                GROUP BY DATE(timestamp), HOUR(timestamp), reading_type
                ORDER BY date, hour, reading_type
            `;

            await this.executeAndSaveQuery(con, query, 'hourly_summary.parquet');
            await dbConfig.closeConnection(con);

            logging.info('✓ Hourly summary table generated');

        } catch (error) {
            await dbConfig.closeConnection(con);
            throw error;
        }
    }

    /**
     * Generate battery level summary table
     * @param {Array<Object>} data - Sensor data
     */
    async generateBatteryLevelSummary(data) {
        const db = dbConfig.createInMemoryDatabase();
        const con = dbConfig.getConnection(db);

        try {
            await this.createDataTable(con, data);

            const query = `
                SELECT 
                    sensor_id,
                    DATE(timestamp) as date,
                    COUNT(*) as total_readings,
                    AVG(battery_level) as avg_battery_level,
                    MIN(battery_level) as min_battery_level,
                    MAX(battery_level) as max_battery_level,
                    STDDEV(battery_level) as std_battery_level,
                    MIN(timestamp) as first_reading_time,
                    MAX(timestamp) as last_reading_time,
                    CASE 
                        WHEN AVG(battery_level) >= 80 THEN 'High'
                        WHEN AVG(battery_level) >= 50 THEN 'Medium'
                        WHEN AVG(battery_level) >= 20 THEN 'Low'
                        ELSE 'Critical'
                    END as battery_status,
                    (MAX(battery_level) - MIN(battery_level)) as battery_drain_rate
                FROM sensor_data
                GROUP BY sensor_id, DATE(timestamp)
                ORDER BY sensor_id, date
            `;

            await this.executeAndSaveQuery(con, query, 'battery_level_summary.parquet');
            await dbConfig.closeConnection(con);

            logging.info('✓ Battery level summary table generated');

        } catch (error) {
            await dbConfig.closeConnection(con);
            throw error;
        }
    }

    /**
     * Generate anomaly summary table
     * @param {Array<Object>} data - Sensor data
     */
    async generateAnomalySummary(data) {
        const db = dbConfig.createInMemoryDatabase();
        const con = dbConfig.getConnection(db);

        try {
            await this.createDataTable(con, data);

            const query = `
                SELECT 
                    sensor_id,
                    reading_type,
                    DATE(timestamp) as date,
                    COUNT(*) as total_readings,
                    SUM(CASE WHEN anomalous_reading THEN 1 ELSE 0 END) as anomaly_count,
                    (SUM(CASE WHEN anomalous_reading THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as anomaly_percentage,
                    SUM(CASE WHEN outlier_corrected THEN 1 ELSE 0 END) as outlier_count,
                    (SUM(CASE WHEN outlier_corrected THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as outlier_percentage,
                    AVG(CASE WHEN anomalous_reading THEN value END) as avg_anomalous_value,
                    MIN(CASE WHEN anomalous_reading THEN value END) as min_anomalous_value,
                    MAX(CASE WHEN anomalous_reading THEN value END) as max_anomalous_value,
                    COUNT(CASE WHEN anomalous_reading AND value < (SELECT AVG(v.value) FROM sensor_data v WHERE v.reading_type = sensor_data.reading_type) THEN 1 END) as low_anomalies,
                    COUNT(CASE WHEN anomalous_reading AND value > (SELECT AVG(v.value) FROM sensor_data v WHERE v.reading_type = sensor_data.reading_type) THEN 1 END) as high_anomalies
                FROM sensor_data
                GROUP BY sensor_id, reading_type, DATE(timestamp)
                HAVING SUM(CASE WHEN anomalous_reading THEN 1 ELSE 0 END) > 0
                ORDER BY sensor_id, reading_type, date
            `;

            await this.executeAndSaveQuery(con, query, 'anomaly_summary.parquet');
            await dbConfig.closeConnection(con);

            logging.info('✓ Anomaly summary table generated');

        } catch (error) {
            await dbConfig.closeConnection(con);
            throw error;
        }
    }

    /**
     * Create data table for summary generation
     * @param {Connection} con - Database connection
     * @param {Array<Object>} data - Sensor data
     */
    async createDataTable(con, data) {
        await new Promise((resolve, reject) => {
            con.run(`CREATE TABLE sensor_data (
                sensor_id VARCHAR,
                timestamp TIMESTAMP,
                reading_type VARCHAR,
                value DOUBLE,
                battery_level DOUBLE,
                calibrated_value DOUBLE,
                anomalous_reading BOOLEAN,
                outlier_corrected BOOLEAN,
                missing_value_filled BOOLEAN,
                daily_average DOUBLE,
                rolling_7day_average DOUBLE
            )`, (err) => {
                if (err) reject(err);
                else resolve();
            });
        });

        // Insert data in batches
        const batchSize = appConfig.processing.batchSize;
        for (let i = 0; i < data.length; i += batchSize) {
            const batch = data.slice(i, i + batchSize);
            const valuesList = batch.map(r => `(
                '${r.sensor_id}',
                '${r.timestamp}',
                '${r.reading_type}',
                ${r.value},
                ${r.battery_level},
                ${r.calibrated_value || r.value},
                ${r.anomalous_reading || false},
                ${r.outlier_corrected || false},
                ${r.missing_value_filled || false},
                ${r.daily_average || r.value},
                ${r.rolling_7day_average || r.value}
            )`);

            const valuesStr = valuesList.join(',');
            await new Promise((resolve, reject) => {
                con.run(`INSERT INTO sensor_data VALUES ${valuesStr}`, (err) => {
                    if (err) reject(err);
                    else resolve();
                });
            });
        }
    }

    /**
     * Execute query and save result as Parquet
     * @param {Connection} con - Database connection
     * @param {string} query - SQL query
     * @param {string} filename - Output filename
     */
    async executeAndSaveQuery(con, query, filename) {
        const filePath = path.join(this.summaryDir, filename);
        const outputPath = filePath.replace(/\\/g, '/');

        await new Promise((resolve, reject) => {
            con.run(`COPY (${query}) TO '${outputPath}' (FORMAT PARQUET, COMPRESSION '${appConfig.processing.compressionType}')`, (err) => {
                if (err) reject(err);
                else resolve();
            });
        });
    }

    /**
     * Generate summary metadata file
     * @param {Array<Object>} data - Original data
     */
    async generateSummaryMetadata(data) {
        const metadata = {
            generated_at: new Date().toISOString(),
            source_records: data.length,
            unique_sensors: [...new Set(data.map(r => r.sensor_id))].length,
            unique_reading_types: [...new Set(data.map(r => r.reading_type))].length,
            date_range: {
                start: Math.min(...data.map(r => new Date(r.timestamp))),
                end: Math.max(...data.map(r => new Date(r.timestamp)))
            },
            summary_tables: [
                'daily_summary.parquet',
                'sensor_summary.parquet',
                'reading_type_summary.parquet',
                'hourly_summary.parquet',
                'battery_level_summary.parquet',
                'anomaly_summary.parquet'
            ]
        };

        const metadataPath = path.join(this.summaryDir, 'metadata.json');
        await fileSystem.writeFile(metadataPath, JSON.stringify(metadata, null, 2));

        logging.info('✓ Summary metadata generated');
    }

    /**
     * Get summary table statistics
     * @returns {Object} Summary table statistics
     */
    async getSummaryTableStats() {
        try {
            const metadataPath = path.join(this.summaryDir, 'metadata.json');
            if (await fileSystem.pathExists(metadataPath)) {
                const metadata = JSON.parse(await fileSystem.readFile(metadataPath));
                return metadata;
            }
            return null;
        } catch (error) {
            logging.error(`Failed to get summary table stats: ${error.message}`);
            return null;
        }
    }
}

module.exports = new SummaryTablesService();
