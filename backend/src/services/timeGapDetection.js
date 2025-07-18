const { database: dbConfig } = require('../config');
const { app: appConfig } = require('../config');
const { statistics, logging } = require('../utils');

/**
 * Time Gap Detection Service for analyzing data continuity
 */
class TimeGapDetectionService {
    constructor () {
        this.gapThresholdHours = appConfig.qualityThresholds.maxTimeGapHours;
    }

    /**
     * Detect time gaps in sensor data using DuckDB
     * @param {Array<Object>} data - Array of sensor data records
     * @returns {Object} Time gap analysis results
     */
    async detectTimeGaps(data) {
        if (!data || data.length === 0) {
            return { gaps: {}, summary: {} };
        }

        const db = dbConfig.createInMemoryDatabase();
        const con = dbConfig.getConnection(db);

        try {
            // Create table and insert data
            await this.createTimeGapTable(con, data);

            // Detect gaps for each sensor-reading_type combination
            const gapResults = await this.analyzeTimeGaps(con);

            // Generate summary statistics
            const summary = this.generateTimeGapSummary(gapResults);

            await dbConfig.closeConnection(con);

            return {
                gaps: gapResults,
                summary,
                threshold_hours: this.gapThresholdHours
            };

        } catch (error) {
            await dbConfig.closeConnection(con);
            throw new Error(`Time gap detection failed: ${error.message}`);
        }
    }

    /**
     * Create and populate time gap analysis table
     * @param {Connection} con - Database connection
     * @param {Array<Object>} data - Sensor data
     */
    async createTimeGapTable(con, data) {
        // Create table
        await new Promise((resolve, reject) => {
            con.run(`CREATE TABLE sensor_time_data (
                sensor_id VARCHAR,
                reading_type VARCHAR,
                timestamp TIMESTAMP,
                value DOUBLE,
                hour_timestamp TIMESTAMP
            )`, (err) => {
                if (err) reject(err);
                else resolve();
            });
        });

        // Insert data with hourly timestamps
        const batchSize = appConfig.processing.batchSize;
        for (let i = 0; i < data.length; i += batchSize) {
            const batch = data.slice(i, i + batchSize);
            const valuesList = batch.map(r => {
                const timestamp = new Date(r.timestamp).toISOString();
                const hourTimestamp = new Date(r.timestamp);
                hourTimestamp.setMinutes(0, 0, 0);
                return `('${r.sensor_id}', '${r.reading_type}', '${timestamp}', ${r.value}, '${hourTimestamp.toISOString()}')`;
            });

            const valuesStr = valuesList.join(',');
            await new Promise((resolve, reject) => {
                con.run(`INSERT INTO sensor_time_data VALUES ${valuesStr}`, (err) => {
                    if (err) reject(err);
                    else resolve();
                });
            });
        }
    }

    /**
     * Analyze time gaps using SQL queries
     * @param {Connection} con - Database connection
     * @returns {Object} Gap analysis results
     */
    async analyzeTimeGaps(con) {
        const gapResults = {};

        // Get unique sensor-reading_type combinations
        const combinations = await new Promise((resolve, reject) => {
            con.all(`SELECT DISTINCT sensor_id, reading_type FROM sensor_time_data`, (err, result) => {
                if (err) reject(err);
                else resolve(result);
            });
        });

        // Analyze gaps for each combination
        for (const combo of combinations) {
            const gaps = await this.detectGapsForCombination(con, combo.sensor_id, combo.reading_type);
            const key = `${combo.sensor_id}_${combo.reading_type}`;
            gapResults[key] = gaps;
        }

        return gapResults;
    }

    /**
     * Detect gaps for a specific sensor-reading_type combination
     * @param {Connection} con - Database connection
     * @param {string} sensorId - Sensor ID
     * @param {string} readingType - Reading type
     * @returns {Object} Gap analysis for the combination
     */
    async detectGapsForCombination(con, sensorId, readingType) {
        // Get time range and actual data points
        const timeRange = await new Promise((resolve, reject) => {
            con.all(`
                SELECT 
                    MIN(hour_timestamp) as min_time,
                    MAX(hour_timestamp) as max_time,
                    COUNT(DISTINCT hour_timestamp) as actual_hours
                FROM sensor_time_data 
                WHERE sensor_id = '${sensorId}' AND reading_type = '${readingType}'
            `, (err, result) => {
                if (err) reject(err);
                else resolve(result[0]);
            });
        });

        if (!timeRange.min_time || !timeRange.max_time) {
            return { expected_hours: 0, actual_hours: 0, missing_hours: 0, gaps: [] };
        }

        // Calculate expected hours
        const startTime = new Date(timeRange.min_time);
        const endTime = new Date(timeRange.max_time);
        const expectedHours = Math.floor((endTime - startTime) / (1000 * 60 * 60)) + 1;
        const actualHours = parseInt(timeRange.actual_hours);

        // Find missing hour slots
        const missingHours = expectedHours - actualHours;

        // Get specific gap periods
        const gapPeriods = await this.findGapPeriods(con, sensorId, readingType, startTime, endTime);

        return {
            sensor_id: sensorId,
            reading_type: readingType,
            expected_hours: expectedHours,
            actual_hours: actualHours,
            missing_hours: missingHours,
            coverage_percentage: (actualHours / expectedHours * 100).toFixed(2),
            gaps: gapPeriods,
            time_range: {
                start: timeRange.min_time,
                end: timeRange.max_time
            }
        };
    }

    /**
     * Find specific gap periods
     * @param {Connection} con - Database connection
     * @param {string} sensorId - Sensor ID
     * @param {string} readingType - Reading type
     * @param {Date} startTime - Start time
     * @param {Date} endTime - End time
     * @returns {Array<Object>} Array of gap periods
     */
    async findGapPeriods(con, sensorId, readingType, startTime, endTime) {
        // Generate expected hourly timestamps
        const expectedHours = statistics.generateHourlyTimestamps(startTime, endTime);

        // Get actual hours from database
        const actualHours = await new Promise((resolve, reject) => {
            con.all(`
                SELECT DISTINCT hour_timestamp
                FROM sensor_time_data 
                WHERE sensor_id = '${sensorId}' AND reading_type = '${readingType}'
                ORDER BY hour_timestamp
            `, (err, result) => {
                if (err) reject(err);
                else resolve(result.map(r => new Date(r.hour_timestamp)));
            });
        });

        // Find gaps
        const gaps = [];
        let gapStart = null;

        for (const expectedHour of expectedHours) {
            const hasData = actualHours.some(actualHour =>
                Math.abs(actualHour - expectedHour) < 60000 // Within 1 minute
            );

            if (!hasData) {
                if (!gapStart) {
                    gapStart = expectedHour;
                }
            } else {
                if (gapStart) {
                    const gapEnd = new Date(expectedHour - 60 * 60 * 1000); // Previous hour
                    const gapDurationHours = (gapEnd - gapStart) / (1000 * 60 * 60) + 1;

                    if (gapDurationHours >= 1) {
                        gaps.push({
                            start_time: gapStart.toISOString(),
                            end_time: gapEnd.toISOString(),
                            duration_hours: gapDurationHours,
                            is_significant: gapDurationHours >= this.gapThresholdHours
                        });
                    }
                    gapStart = null;
                }
            }
        }

        // Handle gap that extends to the end
        if (gapStart) {
            const gapDurationHours = (endTime - gapStart) / (1000 * 60 * 60) + 1;
            gaps.push({
                start_time: gapStart.toISOString(),
                end_time: endTime.toISOString(),
                duration_hours: gapDurationHours,
                is_significant: gapDurationHours >= this.gapThresholdHours
            });
        }

        return gaps;
    }

    /**
     * Generate summary statistics for time gap analysis
     * @param {Object} gapResults - Gap analysis results
     * @returns {Object} Summary statistics
     */
    generateTimeGapSummary(gapResults) {
        const combinations = Object.keys(gapResults);
        let totalExpectedHours = 0;
        let totalActualHours = 0;
        let totalSignificantGaps = 0;
        let totalGaps = 0;
        const coverageByType = {};

        combinations.forEach(key => {
            const result = gapResults[key];
            totalExpectedHours += result.expected_hours;
            totalActualHours += result.actual_hours;
            totalGaps += result.gaps.length;
            totalSignificantGaps += result.gaps.filter(g => g.is_significant).length;

            // Coverage by reading type
            if (!coverageByType[result.reading_type]) {
                coverageByType[result.reading_type] = {
                    expected: 0,
                    actual: 0,
                    sensors: 0
                };
            }
            coverageByType[result.reading_type].expected += result.expected_hours;
            coverageByType[result.reading_type].actual += result.actual_hours;
            coverageByType[result.reading_type].sensors += 1;
        });

        // Calculate percentages for each reading type
        Object.keys(coverageByType).forEach(type => {
            const typeData = coverageByType[type];
            typeData.coverage_percentage = (typeData.actual / typeData.expected * 100).toFixed(2);
        });

        return {
            total_combinations: combinations.length,
            overall_coverage_percentage: totalExpectedHours > 0 ?
                (totalActualHours / totalExpectedHours * 100).toFixed(2) : 0,
            total_expected_hours: totalExpectedHours,
            total_actual_hours: totalActualHours,
            total_missing_hours: totalExpectedHours - totalActualHours,
            total_gaps: totalGaps,
            significant_gaps: totalSignificantGaps,
            coverage_by_reading_type: coverageByType
        };
    }

    /**
     * Get time gap metrics for quality reporting
     * @param {Array<Object>} data - Sensor data
     * @returns {Object} Time gap metrics for quality report
     */
    async getTimeGapMetrics(data) {
        try {
            const gapAnalysis = await this.detectTimeGaps(data);

            return {
                timestamp: new Date().toISOString(),
                overall_coverage: gapAnalysis.summary.overall_coverage_percentage,
                total_gaps: gapAnalysis.summary.total_gaps,
                significant_gaps: gapAnalysis.summary.significant_gaps,
                coverage_by_type: gapAnalysis.summary.coverage_by_reading_type,
                threshold_hours: this.gapThresholdHours
            };
        } catch (error) {
            logging.error(`Failed to get time gap metrics: ${error.message}`);
            return null;
        }
    }
}

module.exports = new TimeGapDetectionService();
