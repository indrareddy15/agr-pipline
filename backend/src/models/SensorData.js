const parquet = require('parquetjs-lite');

/**
 * Sensor data model and schema definitions
 */
class SensorDataModel {
    constructor () {
        this.baseSchema = {
            sensor_id: { type: 'UTF8' },
            timestamp: { type: 'UTF8' },
            reading_type: { type: 'UTF8' },
            value: { type: 'DOUBLE' },
            battery_level: { type: 'DOUBLE' }
        };

        this.enrichedSchema = {
            ...this.baseSchema,
            calibrated_value: { type: 'DOUBLE' },
            anomalous_reading: { type: 'BOOLEAN' },
            outlier_corrected: { type: 'BOOLEAN' },
            missing_value_filled: { type: 'BOOLEAN' },
            daily_average: { type: 'DOUBLE' },
            rolling_7day_average: { type: 'DOUBLE' }
        };
    }

    /**
     * Create Parquet schema for base sensor data
     * @returns {ParquetSchema} Parquet schema object
     */
    createBaseParquetSchema() {
        return new parquet.ParquetSchema(this.baseSchema);
    }

    /**
     * Create Parquet schema for enriched sensor data
     * @returns {ParquetSchema} Parquet schema object
     */
    createEnrichedParquetSchema() {
        return new parquet.ParquetSchema(this.enrichedSchema);
    }

    /**
     * Validate sensor data record structure
     * @param {Object} record - Sensor data record
     * @returns {boolean} True if valid, false otherwise
     */
    validateRecord(record) {
        const requiredFields = Object.keys(this.baseSchema);
        return requiredFields.every(field => record.hasOwnProperty(field));
    }

    /**
     * Create a sensor data record
     * @param {Object} data - Raw sensor data
     * @returns {Object} Formatted sensor data record
     */
    createRecord(data) {
        return {
            sensor_id: data.sensor_id,
            timestamp: data.timestamp,
            reading_type: data.reading_type,
            value: data.value,
            battery_level: data.battery_level,
            // Enriched fields (optional)
            calibrated_value: data.calibrated_value || null,
            anomalous_reading: data.anomalous_reading || false,
            outlier_corrected: data.outlier_corrected || false,
            missing_value_filled: data.missing_value_filled || false,
            daily_average: data.daily_average || null,
            rolling_7day_average: data.rolling_7day_average || null
        };
    }
}

module.exports = new SensorDataModel();
