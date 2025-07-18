const { database: dbConfig } = require('../config');
const { app: appConfig } = require('../config');
const { statistics, logging } = require('../utils');

/**
 * Advanced Data Profiling Service for comprehensive data analysis
 */
class AdvancedDataProfilingService {
    /**
     * Generate comprehensive data profile
     * @param {Array<Object>} data - Array of sensor data records
     * @returns {Object} Comprehensive data profile
     */
    async generateDataProfile(data) {
        if (!data || data.length === 0) {
            return this.getEmptyProfile();
        }

        const db = dbConfig.createInMemoryDatabase();
        const con = dbConfig.getConnection(db);

        try {
            await this.createProfilingTable(con, data);

            const profile = {
                timestamp: new Date().toISOString(),
                total_records: data.length,
                data_overview: await this.getDataOverview(con),
                value_distributions: await this.getValueDistributions(con),
                temporal_analysis: await this.getTemporalAnalysis(con),
                sensor_analysis: await this.getSensorAnalysis(con),
                data_quality_metrics: await this.getDataQualityMetrics(con),
                correlation_analysis: await this.getCorrelationAnalysis(con),
                completeness_analysis: await this.getCompletenessAnalysis(con)
            };

            await dbConfig.closeConnection(con);
            return profile;

        } catch (error) {
            await dbConfig.closeConnection(con);
            throw new Error(`Data profiling failed: ${error.message}`);
        }
    }

    /**
     * Create and populate profiling table
     * @param {Connection} con - Database connection
     * @param {Array<Object>} data - Sensor data
     */
    async createProfilingTable(con, data) {
        await new Promise((resolve, reject) => {
            con.run(`CREATE TABLE sensor_profile_data (
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
                rolling_7day_average DOUBLE,
                date_part DATE,
                hour_part INTEGER,
                day_of_week INTEGER
            )`, (err) => {
                if (err) reject(err);
                else resolve();
            });
        });

        // Insert data with temporal partitions
        const batchSize = appConfig.processing.batchSize;
        for (let i = 0; i < data.length; i += batchSize) {
            const batch = data.slice(i, i + batchSize);
            const valuesList = batch.map(r => {
                const timestamp = new Date(r.timestamp);
                const datePart = timestamp.toISOString().split('T')[0];
                const hourPart = timestamp.getHours();
                const dayOfWeek = timestamp.getDay();

                return `(
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
                    ${r.rolling_7day_average || r.value},
                    '${datePart}',
                    ${hourPart},
                    ${dayOfWeek}
                )`;
            });

            const valuesStr = valuesList.join(',');
            await new Promise((resolve, reject) => {
                con.run(`INSERT INTO sensor_profile_data VALUES ${valuesStr}`, (err) => {
                    if (err) reject(err);
                    else resolve();
                });
            });
        }
    }

    /**
     * Get data overview statistics
     * @param {Connection} con - Database connection
     * @returns {Object} Data overview
     */
    async getDataOverview(con) {
        const query = `
            SELECT 
                CAST(COUNT(*) AS INTEGER) as total_records,
                CAST(COUNT(DISTINCT sensor_id) AS INTEGER) as unique_sensors,
                CAST(COUNT(DISTINCT reading_type) AS INTEGER) as unique_reading_types,
                CAST(COUNT(DISTINCT date_part) AS INTEGER) as unique_dates,
                MIN(timestamp) as earliest_timestamp,
                MAX(timestamp) as latest_timestamp,
                MIN(date_part) as earliest_date,
                MAX(date_part) as latest_date
            FROM sensor_profile_data
        `;

        return await new Promise((resolve, reject) => {
            con.all(query, (err, result) => {
                if (err) reject(err);
                else resolve(result[0]);
            });
        });
    }

    /**
     * Get value distribution statistics per reading type
     * @param {Connection} con - Database connection
     * @returns {Array<Object>} Value distributions
     */
    async getValueDistributions(con) {
        const query = `
            SELECT 
                reading_type,
                CAST(COUNT(*) AS INTEGER) as record_count,
                AVG(value) as mean_value,
                MIN(value) as min_value,
                MAX(value) as max_value,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY value) as q1,
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY value) as median,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY value) as q3,
                PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY value) as p90,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY value) as p95,
                STDDEV(value) as std_dev,
                VARIANCE(value) as variance
            FROM sensor_profile_data 
            GROUP BY reading_type
            ORDER BY reading_type
        `;

        return await new Promise((resolve, reject) => {
            con.all(query, (err, result) => {
                if (err) reject(err);
                else resolve(result || []);
            });
        });
    }

    /**
     * Get temporal analysis patterns
     * @param {Connection} con - Database connection
     * @returns {Object} Temporal analysis
     */
    async getTemporalAnalysis(con) {
        // Hourly patterns
        const hourlyPatterns = await new Promise((resolve, reject) => {
            con.all(`
                SELECT 
                    hour_part,
                    reading_type,
                    CAST(COUNT(*) AS INTEGER) as record_count,
                    AVG(value) as avg_value,
                    STDDEV(value) as std_dev
                FROM sensor_profile_data 
                GROUP BY hour_part, reading_type
                ORDER BY hour_part, reading_type
            `, (err, result) => {
                if (err) reject(err);
                else resolve(result || []);
            });
        });

        // Daily patterns
        const dailyPatterns = await new Promise((resolve, reject) => {
            con.all(`
                SELECT 
                    day_of_week,
                    reading_type,
                    CAST(COUNT(*) AS INTEGER) as record_count,
                    AVG(value) as avg_value,
                    STDDEV(value) as std_dev
                FROM sensor_profile_data 
                GROUP BY day_of_week, reading_type
                ORDER BY day_of_week, reading_type
            `, (err, result) => {
                if (err) reject(err);
                else resolve(result || []);
            });
        });

        // Data volume by date
        const volumeByDate = await new Promise((resolve, reject) => {
            con.all(`
                SELECT 
                    date_part,
                    CAST(COUNT(*) AS INTEGER) as record_count,
                    CAST(COUNT(DISTINCT sensor_id) AS INTEGER) as active_sensors
                FROM sensor_profile_data 
                GROUP BY date_part
                ORDER BY date_part
            `, (err, result) => {
                if (err) reject(err);
                else resolve(result || []);
            });
        });

        return {
            hourly_patterns: hourlyPatterns,
            daily_patterns: dailyPatterns,
            volume_by_date: volumeByDate
        };
    }

    /**
     * Get sensor-level analysis
     * @param {Connection} con - Database connection
     * @returns {Array<Object>} Sensor analysis
     */
    async getSensorAnalysis(con) {
        const query = `
            SELECT 
                sensor_id,
                reading_type,
                CAST(COUNT(*) AS INTEGER) as total_records,
                AVG(value) as avg_value,
                STDDEV(value) as std_dev,
                MIN(value) as min_value,
                MAX(value) as max_value,
                MIN(timestamp) as first_reading,
                MAX(timestamp) as last_reading,
                AVG(battery_level) as avg_battery_level,
                MIN(battery_level) as min_battery_level,
                CAST(SUM(CASE WHEN anomalous_reading THEN 1 ELSE 0 END) AS INTEGER) as anomaly_count,
                CAST(SUM(CASE WHEN outlier_corrected THEN 1 ELSE 0 END) AS INTEGER) as outlier_count,
                CAST(SUM(CASE WHEN missing_value_filled THEN 1 ELSE 0 END) AS INTEGER) as filled_count
            FROM sensor_profile_data 
            GROUP BY sensor_id, reading_type
            ORDER BY sensor_id, reading_type
        `;

        return await new Promise((resolve, reject) => {
            con.all(query, (err, result) => {
                if (err) reject(err);
                else resolve(result || []);
            });
        });
    }

    /**
     * Get data quality metrics
     * @param {Connection} con - Database connection
     * @returns {Object} Data quality metrics
     */
    async getDataQualityMetrics(con) {
        const missingValues = await new Promise((resolve, reject) => {
            con.all(`
                SELECT 
                    reading_type,
                    CAST(COUNT(*) AS INTEGER) as total_records,
                    CAST(SUM(CASE WHEN missing_value_filled THEN 1 ELSE 0 END) AS INTEGER) as missing_count,
                    (CAST(SUM(CASE WHEN missing_value_filled THEN 1 ELSE 0 END) AS FLOAT) * 100.0 / CAST(COUNT(*) AS FLOAT)) as missing_percentage
                FROM sensor_profile_data 
                GROUP BY reading_type
            `, (err, result) => {
                if (err) reject(err);
                else resolve(result || []);
            });
        });

        const anomalies = await new Promise((resolve, reject) => {
            con.all(`
                SELECT 
                    reading_type,
                    CAST(COUNT(*) AS INTEGER) as total_records,
                    CAST(SUM(CASE WHEN anomalous_reading THEN 1 ELSE 0 END) AS INTEGER) as anomaly_count,
                    (CAST(SUM(CASE WHEN anomalous_reading THEN 1 ELSE 0 END) AS FLOAT) * 100.0 / CAST(COUNT(*) AS FLOAT)) as anomaly_percentage
                FROM sensor_profile_data 
                GROUP BY reading_type
            `, (err, result) => {
                if (err) reject(err);
                else resolve(result || []);
            });
        });

        const outliers = await new Promise((resolve, reject) => {
            con.all(`
                SELECT 
                    reading_type,
                    CAST(COUNT(*) AS INTEGER) as total_records,
                    CAST(SUM(CASE WHEN outlier_corrected THEN 1 ELSE 0 END) AS INTEGER) as outlier_count,
                    (CAST(SUM(CASE WHEN outlier_corrected THEN 1 ELSE 0 END) AS FLOAT) * 100.0 / CAST(COUNT(*) AS FLOAT)) as outlier_percentage
                FROM sensor_profile_data 
                GROUP BY reading_type
            `, (err, result) => {
                if (err) reject(err);
                else resolve(result || []);
            });
        });

        return {
            missing_values: missingValues,
            anomalies: anomalies,
            outliers: outliers
        };
    }

    /**
     * Get correlation analysis between reading types
     * @param {Connection} con - Database connection
     * @returns {Array<Object>} Correlation analysis
     */
    async getCorrelationAnalysis(con) {
        // Get correlations between different reading types for the same sensor and time
        const query = `
            WITH correlation_data AS (
                SELECT 
                    sensor_id,
                    date_part,
                    hour_part,
                    reading_type,
                    AVG(value) as avg_value
                FROM sensor_profile_data
                GROUP BY sensor_id, date_part, hour_part, reading_type
            ),
            pivot_data AS (
                SELECT 
                    sensor_id,
                    date_part,
                    hour_part,
                    MAX(CASE WHEN reading_type = 'temperature' THEN avg_value END) as temperature,
                    MAX(CASE WHEN reading_type = 'humidity' THEN avg_value END) as humidity,
                    MAX(CASE WHEN reading_type = 'soil_moisture' THEN avg_value END) as soil_moisture,
                    MAX(CASE WHEN reading_type = 'light_intensity' THEN avg_value END) as light_intensity
                FROM correlation_data
                GROUP BY sensor_id, date_part, hour_part
                HAVING CAST(COUNT(DISTINCT reading_type) AS INTEGER) > 1
            )
            SELECT 
                CAST(COUNT(*) AS INTEGER) as sample_size,
                CORR(temperature, humidity) as temp_humidity_corr,
                CORR(temperature, soil_moisture) as temp_soil_corr,
                CORR(temperature, light_intensity) as temp_light_corr,
                CORR(humidity, soil_moisture) as humidity_soil_corr,
                CORR(humidity, light_intensity) as humidity_light_corr,
                CORR(soil_moisture, light_intensity) as soil_light_corr
            FROM pivot_data
        `;

        return await new Promise((resolve, reject) => {
            con.all(query, (err, result) => {
                if (err) reject(err);
                else resolve(result[0] || {});
            });
        });
    }

    /**
     * Get completeness analysis
     * @param {Connection} con - Database connection
     * @returns {Object} Completeness analysis
     */
    async getCompletenessAnalysis(con) {
        const expectedVsActual = await new Promise((resolve, reject) => {
            con.all(`
                SELECT 
                    sensor_id,
                    reading_type,
                    CAST(COUNT(DISTINCT date_part) AS INTEGER) as days_with_data,
                    CAST(COUNT(*) AS INTEGER) as total_records,
                    MIN(date_part) as first_date,
                    MAX(date_part) as last_date
                FROM sensor_profile_data
                GROUP BY sensor_id, reading_type
            `, (err, result) => {
                if (err) reject(err);
                else resolve(result || []);
            });
        });

        // Calculate expected records based on time range
        const enrichedCompleteness = expectedVsActual.map(item => {
            const firstDate = new Date(item.first_date);
            const lastDate = new Date(item.last_date);
            const daysDiff = Math.floor((lastDate - firstDate) / (1000 * 60 * 60 * 24)) + 1;
            const expectedDays = daysDiff;
            const completenessPercentage = (item.days_with_data / expectedDays * 100).toFixed(2);

            return {
                ...item,
                expected_days: expectedDays,
                completeness_percentage: completenessPercentage
            };
        });

        return {
            sensor_completeness: enrichedCompleteness,
            summary: {
                total_sensor_type_combinations: enrichedCompleteness.length,
                avg_completeness: enrichedCompleteness.length > 0 ?
                    (enrichedCompleteness.reduce((sum, item) => sum + parseFloat(item.completeness_percentage), 0) / enrichedCompleteness.length).toFixed(2) : 0
            }
        };
    }

    /**
     * Get empty profile structure
     * @returns {Object} Empty profile structure
     */
    getEmptyProfile() {
        return {
            timestamp: new Date().toISOString(),
            total_records: 0,
            data_overview: {},
            value_distributions: [],
            temporal_analysis: {},
            sensor_analysis: [],
            data_quality_metrics: {},
            correlation_analysis: {},
            completeness_analysis: {}
        };
    }

    /**
     * Generate profile summary for reporting
     * @param {Object} profile - Full data profile
     * @returns {Object} Profile summary
     */
    generateProfileSummary(profile) {
        return {
            timestamp: profile.timestamp,
            total_records: profile.total_records,
            sensors: profile.data_overview.unique_sensors,
            reading_types: profile.data_overview.unique_reading_types,
            date_range: {
                start: profile.data_overview.earliest_date,
                end: profile.data_overview.latest_date
            },
            data_quality: {
                avg_missing_percentage: profile.value_distributions.reduce((sum, dist) =>
                    sum + (profile.data_quality_metrics.missing_values.find(mv => mv.reading_type === dist.reading_type)?.missing_percentage || 0), 0) / profile.value_distributions.length || 0,
                avg_anomaly_percentage: profile.value_distributions.reduce((sum, dist) =>
                    sum + (profile.data_quality_metrics.anomalies.find(a => a.reading_type === dist.reading_type)?.anomaly_percentage || 0), 0) / profile.value_distributions.length || 0
            },
            completeness: profile.completeness_analysis.summary?.avg_completeness || 0
        };
    }
}

module.exports = new AdvancedDataProfilingService();
