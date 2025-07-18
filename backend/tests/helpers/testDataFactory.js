/**
 * Test Data Factory
 * Generates mock data for testing various scenarios
 */

class TestDataFactory {
    /**
     * Generate valid sensor data
     * @param {number} count - Number of records to generate
     * @param {Object} options - Generation options
     * @returns {Array} Array of sensor data records
     */
    static generateValidSensorData(count = 10, options = {}) {
        const readingTypes = options.readingTypes || ['temperature', 'humidity', 'soil_moisture', 'ph_level', 'light_intensity'];
        const sensorIds = options.sensorIds || ['sensor_001', 'sensor_002', 'sensor_003'];

        const data = [];
        const baseTime = options.baseTime || new Date('2023-06-01T00:00:00Z');

        for (let i = 0; i < count; i++) {
            const timestamp = new Date(baseTime.getTime() + (i * 3600000)); // 1 hour intervals

            data.push({
                sensor_id: sensorIds[i % sensorIds.length],
                timestamp: timestamp.toISOString(),
                reading_type: readingTypes[i % readingTypes.length],
                value: this.generateValueForType(readingTypes[i % readingTypes.length]),
                battery_level: Math.random() * 100,
                location: options.includeLocation ? this.generateLocation() : undefined
            });
        }

        return data;
    }

    /**
     * Generate invalid sensor data for negative testing
     * @param {string} type - Type of invalid data
     * @returns {Array} Array of invalid sensor data records
     */
    static generateInvalidSensorData(type) {
        const baseRecord = {
            sensor_id: 'sensor_001',
            timestamp: '2023-06-01T00:00:00Z',
            reading_type: 'temperature',
            value: 25.5,
            battery_level: 85.2
        };

        switch (type) {
            case 'missing_sensor_id':
                return [{ ...baseRecord, sensor_id: null }];

            case 'invalid_timestamp':
                return [{ ...baseRecord, timestamp: 'invalid-date' }];

            case 'missing_timestamp':
                return [{ ...baseRecord, timestamp: null }];

            case 'invalid_value':
                return [{ ...baseRecord, value: 'not-a-number' }];

            case 'missing_reading_type':
                return [{ ...baseRecord, reading_type: null }];

            case 'empty_record':
                return [{}];

            case 'null_record':
                return [null];

            case 'extreme_values':
                return [
                    { ...baseRecord, value: Number.MAX_VALUE },
                    { ...baseRecord, value: Number.MIN_VALUE },
                    { ...baseRecord, value: Infinity },
                    { ...baseRecord, value: -Infinity }
                ];

            default:
                return [baseRecord];
        }
    }

    /**
     * Generate data with outliers
     * @param {number} count - Total number of records
     * @param {number} outlierPercentage - Percentage of outliers (0-100)
     * @returns {Array} Array with outliers
     */
    static generateDataWithOutliers(count = 100, outlierPercentage = 10) {
        const data = this.generateValidSensorData(count);
        const outlierCount = Math.floor(count * outlierPercentage / 100);

        for (let i = 0; i < outlierCount; i++) {
            const randomIndex = Math.floor(Math.random() * data.length);
            const record = data[randomIndex];

            // Create extreme outlier based on reading type
            switch (record.reading_type) {
                case 'temperature':
                    record.value = Math.random() > 0.5 ? 150 : -50; // Extreme temperatures
                    break;
                case 'humidity':
                    record.value = Math.random() > 0.5 ? 150 : -10; // Invalid humidity
                    break;
                case 'soil_moisture':
                    record.value = Math.random() > 0.5 ? 200 : -20; // Invalid moisture
                    break;
                default:
                    record.value = Math.random() > 0.5 ? 1000 : -1000; // Generic outlier
            }
        }

        return data;
    }

    /**
     * Generate data with missing values
     * @param {number} count - Total number of records
     * @param {number} missingPercentage - Percentage of missing values (0-100)
     * @returns {Array} Array with missing values
     */
    static generateDataWithMissingValues(count = 100, missingPercentage = 20) {
        const data = this.generateValidSensorData(count);
        const missingCount = Math.floor(count * missingPercentage / 100);

        for (let i = 0; i < missingCount; i++) {
            const randomIndex = Math.floor(Math.random() * data.length);
            const record = data[randomIndex];

            // Randomly make different fields missing
            const fieldsToMiss = ['value', 'battery_level'];
            const fieldToMiss = fieldsToMiss[Math.floor(Math.random() * fieldsToMiss.length)];
            record[fieldToMiss] = null;
        }

        return data;
    }

    /**
     * Generate duplicate data
     * @param {number} count - Number of unique records
     * @param {number} duplicatePercentage - Percentage of duplicates (0-100)
     * @returns {Array} Array with duplicates
     */
    static generateDataWithDuplicates(count = 50, duplicatePercentage = 30) {
        const uniqueData = this.generateValidSensorData(count);
        const duplicateCount = Math.floor(count * duplicatePercentage / 100);

        const duplicates = [];
        for (let i = 0; i < duplicateCount; i++) {
            const randomIndex = Math.floor(Math.random() * uniqueData.length);
            duplicates.push({ ...uniqueData[randomIndex] });
        }

        return [...uniqueData, ...duplicates];
    }

    /**
     * Generate value appropriate for reading type
     * @param {string} readingType - Type of reading
     * @returns {number} Generated value
     */
    static generateValueForType(readingType) {
        const ranges = {
            temperature: { min: -10, max: 45 },
            humidity: { min: 0, max: 100 },
            soil_moisture: { min: 0, max: 100 },
            ph_level: { min: 4, max: 9 },
            light_intensity: { min: 0, max: 1000 }
        };

        const range = ranges[readingType] || { min: 0, max: 100 };
        return Math.random() * (range.max - range.min) + range.min;
    }

    /**
     * Generate location data
     * @returns {Object} Location object
     */
    static generateLocation() {
        return {
            latitude: (Math.random() - 0.5) * 180,
            longitude: (Math.random() - 0.5) * 360,
            field_id: `field_${Math.floor(Math.random() * 100)}`
        };
    }

    /**
     * Generate time series data with gaps
     * @param {number} hours - Number of hours of data
     * @param {Array} gapHours - Array of hour indices to create gaps
     * @returns {Array} Time series data with gaps
     */
    static generateTimeSeriesWithGaps(hours = 24, gapHours = [5, 6, 15, 16, 17]) {
        const data = [];
        const baseTime = new Date('2023-06-01T00:00:00Z');

        for (let i = 0; i < hours; i++) {
            if (!gapHours.includes(i)) {
                const timestamp = new Date(baseTime.getTime() + (i * 3600000));
                data.push({
                    sensor_id: 'sensor_001',
                    timestamp: timestamp.toISOString(),
                    reading_type: 'temperature',
                    value: this.generateValueForType('temperature'),
                    battery_level: Math.random() * 100
                });
            }
        }

        return data;
    }

    /**
     * Generate edge case data
     * @returns {Array} Array of edge case records
     */
    static generateEdgeCaseData() {
        return [
            // Boundary values
            { sensor_id: 'sensor_001', timestamp: '2023-06-01T00:00:00Z', reading_type: 'temperature', value: 0, battery_level: 0 },
            { sensor_id: 'sensor_002', timestamp: '2023-06-01T01:00:00Z', reading_type: 'humidity', value: 100, battery_level: 100 },

            // Very long strings
            {
                sensor_id: 'a'.repeat(255),
                timestamp: '2023-06-01T02:00:00Z',
                reading_type: 'temperature',
                value: 25,
                battery_level: 50
            },

            // Special characters
            {
                sensor_id: 'sensor_special_!@#$%^&*()',
                timestamp: '2023-06-01T03:00:00Z',
                reading_type: 'temperature',
                value: 25,
                battery_level: 50
            },

            // Different timestamp formats
            { sensor_id: 'sensor_003', timestamp: '2023-06-01 04:00:00', reading_type: 'temperature', value: 25, battery_level: 50 },
            { sensor_id: 'sensor_004', timestamp: 1685577600000, reading_type: 'temperature', value: 25, battery_level: 50 }, // Unix timestamp

            // Very small and large numbers
            { sensor_id: 'sensor_005', timestamp: '2023-06-01T05:00:00Z', reading_type: 'temperature', value: 0.0001, battery_level: 0.1 },
            { sensor_id: 'sensor_006', timestamp: '2023-06-01T06:00:00Z', reading_type: 'temperature', value: 999999, battery_level: 99.99 }
        ];
    }
}

module.exports = TestDataFactory;
