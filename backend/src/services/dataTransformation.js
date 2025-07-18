const { logging } = require('../utils');

/**
 * Data Transformation Service
 * Step 2: Cleans and transforms sensor data
 */
class DataTransformationService {
    /**
     * Transform raw sensor data
     * @param {Array} data - Raw sensor data
     * @returns {Object} Transformation results
     */
    async transformData(data) {
        try {
            logging.info(`Transforming ${data.length} records`);

            const transformedData = data.map(record => {
                try {
                    return this.transformRecord(record);
                } catch (error) {
                    logging.error(`Failed to transform record: ${error.message}`);
                    return null;
                }
            }).filter(record => record !== null);

            const stats = {
                inputRecords: data.length,
                outputRecords: transformedData.length,
                failedRecords: data.length - transformedData.length
            };

            logging.info(`Transformation complete: ${stats.outputRecords}/${stats.inputRecords} records processed`);

            return {
                transformedData,
                transformationStats: stats
            };

        } catch (error) {
            logging.error(`Data transformation failed: ${error.message}`);
            throw error;
        }
    }

    /**
     * Transform a single record
     * @param {Object} record - Raw record
     * @returns {Object} Transformed record
     */
    transformRecord(record) {
        const transformed = {
            sensor_id: String(record.sensor_id || '').trim(),
            timestamp: this.normalizeTimestamp(record.timestamp),
            reading_type: String(record.reading_type || '').trim().toLowerCase(),
            value: this.normalizeValue(record.value),
            battery_level: this.normalizeValue(record.battery_level),
            location: record.location || null,
            anomalous_reading: false,
            missing_value_filled: false,
            outlier_corrected: false,
            processed_timestamp: new Date().toISOString()
        };

        // Calculate derived fields
        transformed.daily_avg = this.calculateDailyAverage(transformed.value);
        transformed.rolling_avg_7d = this.calculateRollingAverage(transformed.value);

        return transformed;
    }

    /**
     * Normalize timestamp to ISO format
     * @param {any} timestamp - Raw timestamp
     * @returns {string} ISO timestamp
     */
    normalizeTimestamp(timestamp) {
        try {
            let date;
            if (timestamp instanceof Date) {
                date = timestamp;
            } else if (typeof timestamp === 'string') {
                date = new Date(timestamp);
            } else if (typeof timestamp === 'number') {
                date = new Date(timestamp);
            } else {
                throw new Error('Invalid timestamp format');
            }

            if (isNaN(date.getTime())) {
                throw new Error('Invalid date value');
            }

            return date.toISOString();
        } catch (error) {
            throw new Error(`Timestamp normalization failed: ${error.message}`);
        }
    }

    /**
     * Normalize numeric values
     * @param {any} value - Raw value
     * @returns {number} Normalized value
     */
    normalizeValue(value) {
        if (value === null || value === undefined || value === '') {
            return null;
        }

        const numValue = parseFloat(value);
        if (isNaN(numValue)) {
            return null;
        }

        return numValue;
    }

    /**
     * Calculate daily average (simplified)
     * @param {number} value - Current value
     * @returns {number} Daily average estimate
     */
    calculateDailyAverage(value) {
        if (value === null || value === undefined) {
            return null;
        }
        return value; // Simplified - would normally calculate from historical data
    }

    /**
     * Calculate 7-day rolling average (simplified)
     * @param {number} value - Current value
     * @returns {number} Rolling average estimate
     */
    calculateRollingAverage(value) {
        if (value === null || value === undefined) {
            return null;
        }
        return value; // Simplified - would normally calculate from historical data
    }

    /**
     * Remove duplicate records
     * @param {Array} data - Data array
     * @returns {Array} Deduplicated data
     */
    removeDuplicates(data) {
        const seen = new Set();
        return data.filter(record => {
            const key = `${record.sensor_id}-${record.timestamp}-${record.reading_type}`;
            if (seen.has(key)) {
                return false;
            }
            seen.add(key);
            return true;
        });
    }

    /**
     * Fill missing values
     * @param {Array} data - Data array
     * @returns {Array} Data with filled values
     */
    fillMissingValues(data) {
        return data.map(record => {
            if (record.value === null || record.value === undefined) {
                record.value = this.getDefaultValue(record.reading_type);
                record.missing_value_filled = true;
            }
            return record;
        });
    }

    /**
     * Get default value for reading type
     * @param {string} readingType - Type of reading
     * @returns {number} Default value
     */
    getDefaultValue(readingType) {
        const defaults = {
            temperature: 25.0,
            humidity: 50.0,
            soil_moisture: 40.0,
            ph_level: 7.0,
            light_intensity: 500.0
        };
        return defaults[readingType] || 0;
    }

    /**
     * Get transformation statistics
     * @returns {Object} Current statistics
     */
    getStats() {
        return {
            recordsProcessed: 0,
            recordsTransformed: 0,
            recordsFailed: 0
        };
    }
}

module.exports = new DataTransformationService();
