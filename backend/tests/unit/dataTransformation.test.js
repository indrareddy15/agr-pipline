/**
 * Data Transformation Service Unit Tests
 * Comprehensive testing of transformation logic with positive, negative, and edge cases
 */

const DataTransformationService = require('../../src/services/dataTransformation');
const TestDataFactory = require('../helpers/testDataFactory');
const TestUtils = require('../helpers/testUtils');

describe('DataTransformationService', () => {
    describe('transformData - Positive Cases', () => {
        test('should successfully transform valid sensor data', async () => {
            const inputData = TestDataFactory.generateValidSensorData(10);

            const result = await DataTransformationService.transformData(inputData);

            expect(result).toHaveProperty('transformedData');
            expect(result).toHaveProperty('transformationStats');
            expect(result.transformedData).toHaveLength(10);
            expect(result.transformationStats.inputRecords).toBe(10);
            expect(result.transformationStats.outputRecords).toBe(10);
            expect(result.transformationStats.failedRecords).toBe(0);
        });

        test('should handle large datasets efficiently', async () => {
            const inputData = TestDataFactory.generateValidSensorData(1000);

            const performanceTestFn = TestUtils.performanceTest(
                () => DataTransformationService.transformData(inputData),
                5000 // 5 second maximum
            );

            const result = await performanceTestFn();

            expect(result.transformedData).toHaveLength(1000);
            expect(result.transformationStats.outputRecords).toBe(1000);
        });

        test('should preserve all required fields in transformed data', async () => {
            const inputData = TestDataFactory.generateValidSensorData(5);

            const result = await DataTransformationService.transformData(inputData);

            result.transformedData.forEach(record => {
                expect(record).toHaveProperty('sensor_id');
                expect(record).toHaveProperty('timestamp');
                expect(record).toHaveProperty('reading_type');
                expect(record).toHaveProperty('value');
                expect(record).toHaveProperty('battery_level');
                expect(record).toHaveProperty('anomalous_reading');
                expect(record).toHaveProperty('missing_value_filled');
                expect(record).toHaveProperty('outlier_corrected');
                expect(record).toHaveProperty('processed_timestamp');
                expect(record).toHaveProperty('daily_avg');
                expect(record).toHaveProperty('rolling_avg_7d');
            });
        });

        test('should calculate derived fields correctly', async () => {
            const inputData = [{
                sensor_id: 'sensor_001',
                timestamp: '2023-06-01T10:00:00Z',
                reading_type: 'temperature',
                value: 25.5,
                battery_level: 85.2
            }];

            const result = await DataTransformationService.transformData(inputData);
            const transformed = result.transformedData[0];

            expect(transformed.daily_avg).toBe(25.5);
            expect(transformed.rolling_avg_7d).toBe(25.5);
            expect(transformed.processed_timestamp).toBeValidISO8601();
        });
    });

    describe('transformData - Negative Cases', () => {
        test('should handle empty dataset gracefully', async () => {
            const result = await DataTransformationService.transformData([]);

            expect(result.transformedData).toHaveLength(0);
            expect(result.transformationStats.inputRecords).toBe(0);
            expect(result.transformationStats.outputRecords).toBe(0);
            expect(result.transformationStats.failedRecords).toBe(0);
        });

        test('should handle null input gracefully', async () => {
            await expect(DataTransformationService.transformData(null))
                .rejects.toThrow();
        });

        test('should skip invalid records and continue processing', async () => {
            const invalidData = TestDataFactory.generateInvalidSensorData('invalid_timestamp');
            const validData = TestDataFactory.generateValidSensorData(3);
            const mixedData = [...invalidData, ...validData];

            const result = await DataTransformationService.transformData(mixedData);

            expect(result.transformationStats.inputRecords).toBe(4);
            expect(result.transformationStats.outputRecords).toBe(3);
            expect(result.transformationStats.failedRecords).toBe(1);
        });

        test('should handle records with missing required fields', async () => {
            const invalidData = [
                TestDataFactory.generateInvalidSensorData('missing_sensor_id')[0],
                TestDataFactory.generateInvalidSensorData('missing_timestamp')[0],
                TestDataFactory.generateInvalidSensorData('missing_reading_type')[0]
            ];

            const result = await DataTransformationService.transformData(invalidData);

            expect(result.transformationStats.failedRecords).toBeGreaterThan(0);
        });

        test('should handle malformed data gracefully', async () => {
            const malformedData = [
                null,
                undefined,
                'not an object',
                123,
                [],
                { incomplete: 'data' }
            ];

            const result = await DataTransformationService.transformData(malformedData);

            expect(result.transformationStats.failedRecords).toBe(malformedData.length);
            expect(result.transformedData).toHaveLength(0);
        });
    });

    describe('transformData - Edge Cases', () => {
        test('should handle extreme values correctly', async () => {
            const extremeData = TestDataFactory.generateInvalidSensorData('extreme_values');

            const result = await DataTransformationService.transformData(extremeData);

            // Should handle extreme values by either transforming or rejecting them
            expect(result).toHaveProperty('transformedData');
            expect(result).toHaveProperty('transformationStats');
        });

        test('should handle boundary timestamp values', async () => {
            const boundaryData = [
                {
                    sensor_id: 'sensor_001',
                    timestamp: '1970-01-01T00:00:00Z', // Unix epoch
                    reading_type: 'temperature',
                    value: 25,
                    battery_level: 50
                },
                {
                    sensor_id: 'sensor_002',
                    timestamp: '2038-01-19T03:14:07Z', // Near Unix timestamp limit
                    reading_type: 'temperature',
                    value: 25,
                    battery_level: 50
                }
            ];

            const result = await DataTransformationService.transformData(boundaryData);

            expect(result.transformedData.length).toBeGreaterThan(0);
            result.transformedData.forEach(record => {
                expect(record.timestamp).toBeValidISO8601();
            });
        });

        test('should handle very long string values', async () => {
            const longStringData = [{
                sensor_id: 'a'.repeat(1000), // Very long sensor ID
                timestamp: '2023-06-01T10:00:00Z',
                reading_type: 'temperature',
                value: 25,
                battery_level: 50
            }];

            const result = await DataTransformationService.transformData(longStringData);

            expect(result).toHaveProperty('transformedData');
            if (result.transformedData.length > 0) {
                expect(result.transformedData[0].sensor_id).toBeDefined();
            }
        });

        test('should handle special characters in string fields', async () => {
            const specialCharData = [{
                sensor_id: 'sensor_!@#$%^&*()',
                timestamp: '2023-06-01T10:00:00Z',
                reading_type: 'temp-ñature_ös',
                value: 25,
                battery_level: 50
            }];

            const result = await DataTransformationService.transformData(specialCharData);

            expect(result.transformedData).toHaveLength(1);
            expect(result.transformedData[0].sensor_id).toBe('sensor_!@#$%^&*()');
        });
    });

    describe('transformRecord - Individual Record Testing', () => {
        test('should transform valid record correctly', () => {
            const record = {
                sensor_id: 'sensor_001',
                timestamp: '2023-06-01T10:00:00Z',
                reading_type: 'Temperature',
                value: 25.5,
                battery_level: 85.2
            };

            const transformed = DataTransformationService.transformRecord(record);

            expect(transformed.sensor_id).toBe('sensor_001');
            expect(transformed.timestamp).toBeValidISO8601();
            expect(transformed.reading_type).toBe('temperature'); // Should be lowercase
            expect(transformed.value).toBe(25.5);
            expect(transformed.battery_level).toBe(85.2);
            expect(transformed.anomalous_reading).toBe(false);
            expect(transformed.missing_value_filled).toBe(false);
            expect(transformed.outlier_corrected).toBe(false);
        });

        test('should handle missing optional fields', () => {
            const record = {
                sensor_id: 'sensor_001',
                timestamp: '2023-06-01T10:00:00Z',
                reading_type: 'temperature',
                value: 25.5,
                battery_level: 85.2
                // location is missing
            };

            const transformed = DataTransformationService.transformRecord(record);

            expect(transformed.location).toBeNull();
        });

        test('should trim whitespace from string fields', () => {
            const record = {
                sensor_id: '  sensor_001  ',
                timestamp: '2023-06-01T10:00:00Z',
                reading_type: '  Temperature  ',
                value: 25.5,
                battery_level: 85.2
            };

            const transformed = DataTransformationService.transformRecord(record);

            expect(transformed.sensor_id).toBe('sensor_001');
            expect(transformed.reading_type).toBe('temperature');
        });
    });

    describe('normalizeTimestamp', () => {
        test('should normalize ISO string timestamp', () => {
            const timestamp = '2023-06-01T10:00:00Z';
            const normalized = DataTransformationService.normalizeTimestamp(timestamp);

            expect(normalized).toBeValidISO8601();
            expect(normalized).toBe('2023-06-01T10:00:00.000Z');
        });

        test('should normalize Date object', () => {
            const timestamp = new Date('2023-06-01T10:00:00Z');
            const normalized = DataTransformationService.normalizeTimestamp(timestamp);

            expect(normalized).toBeValidISO8601();
        });

        test('should normalize Unix timestamp', () => {
            const timestamp = 1685534400000; // 2023-05-31T10:00:00.000Z
            const normalized = DataTransformationService.normalizeTimestamp(timestamp);

            expect(normalized).toBeValidISO8601();
        });

        test('should handle various timestamp formats', () => {
            const formats = [
                '2023-06-01 10:00:00',
                '06/01/2023 10:00:00',
                '2023-06-01T10:00:00+05:30',
                '2023-06-01T10:00:00.123Z'
            ];

            formats.forEach(format => {
                const normalized = DataTransformationService.normalizeTimestamp(format);
                expect(normalized).toBeValidISO8601();
            });
        });

        test('should throw error for invalid timestamps', () => {
            const invalidTimestamps = [
                'invalid-date',
                '',
                null,
                undefined,
                'not-a-date',
                '2023-13-01T10:00:00Z', // Invalid month
                '2023-06-32T10:00:00Z'  // Invalid day
            ];

            invalidTimestamps.forEach(timestamp => {
                expect(() => DataTransformationService.normalizeTimestamp(timestamp))
                    .toThrow();
            });
        });
    });

    describe('normalizeValue', () => {
        test('should normalize valid numeric values', () => {
            const values = [25, 25.5, '25', '25.5', 0, -25.5];

            values.forEach(value => {
                const normalized = DataTransformationService.normalizeValue(value);
                expect(typeof normalized).toBe('number');
                expect(isNaN(normalized)).toBe(false);
            });
        });

        test('should return null for invalid values', () => {
            const invalidValues = [null, undefined, '', 'not-a-number', NaN, {}, []];

            invalidValues.forEach(value => {
                const normalized = DataTransformationService.normalizeValue(value);
                expect(normalized).toBeNull();
            });
        });

        test('should handle edge numeric values', () => {
            const edgeValues = [
                Number.MAX_SAFE_INTEGER,
                Number.MIN_SAFE_INTEGER,
                Number.EPSILON,
                1e-10,
                1e10
            ];

            edgeValues.forEach(value => {
                const normalized = DataTransformationService.normalizeValue(value);
                expect(typeof normalized).toBe('number');
                expect(isNaN(normalized)).toBe(false);
            });
        });
    });

    describe('removeDuplicates', () => {
        test('should remove exact duplicates', () => {
            const data = TestDataFactory.generateDataWithDuplicates(10, 50);
            const uniqueData = DataTransformationService.removeDuplicates(data);

            expect(uniqueData.length).toBeLessThan(data.length);

            // Verify no duplicates remain
            const keys = new Set();
            uniqueData.forEach(record => {
                const key = `${record.sensor_id}-${record.timestamp}-${record.reading_type}`;
                expect(keys.has(key)).toBe(false);
                keys.add(key);
            });
        });

        test('should preserve unique records', () => {
            const data = TestDataFactory.generateValidSensorData(10);
            const uniqueData = DataTransformationService.removeDuplicates(data);

            expect(uniqueData).toHaveLength(10);
        });

        test('should handle empty array', () => {
            const uniqueData = DataTransformationService.removeDuplicates([]);
            expect(uniqueData).toHaveLength(0);
        });
    });

    describe('fillMissingValues', () => {
        test('should fill missing values with defaults', () => {
            const dataWithMissing = TestDataFactory.generateDataWithMissingValues(10, 50);
            const filledData = DataTransformationService.fillMissingValues(dataWithMissing);

            filledData.forEach(record => {
                if (record.missing_value_filled) {
                    expect(record.value).not.toBeNull();
                    expect(record.value).not.toBeUndefined();
                }
            });
        });

        test('should use correct default values for different reading types', () => {
            const testCases = [
                { reading_type: 'temperature', expectedDefault: 25.0 },
                { reading_type: 'humidity', expectedDefault: 50.0 },
                { reading_type: 'soil_moisture', expectedDefault: 40.0 },
                { reading_type: 'ph_level', expectedDefault: 7.0 },
                { reading_type: 'light_intensity', expectedDefault: 500.0 },
                { reading_type: 'unknown_type', expectedDefault: 0 }
            ];

            testCases.forEach(testCase => {
                const record = {
                    sensor_id: 'sensor_001',
                    timestamp: '2023-06-01T10:00:00Z',
                    reading_type: testCase.reading_type,
                    value: null,
                    battery_level: 50
                };

                const filled = DataTransformationService.fillMissingValues([record]);
                expect(filled[0].value).toBe(testCase.expectedDefault);
                expect(filled[0].missing_value_filled).toBe(true);
            });
        });

        test('should not modify records with valid values', () => {
            const data = TestDataFactory.generateValidSensorData(5);
            const filledData = DataTransformationService.fillMissingValues(data);

            filledData.forEach(record => {
                expect(record.missing_value_filled).toBe(false);
            });
        });
    });

    describe('Memory and Performance Tests', () => {
        test('should handle large datasets without memory leaks', async () => {
            const memoryTestFn = TestUtils.memoryTest(
                async () => {
                    const largeDataset = TestDataFactory.generateValidSensorData(10000);
                    return await DataTransformationService.transformData(largeDataset);
                },
                50 // 50 MB limit
            );

            const result = await memoryTestFn();
            expect(result.transformedData).toHaveLength(10000);
        });

        test('should process data incrementally for very large datasets', async () => {
            const veryLargeDataset = TestDataFactory.generateValidSensorData(50000);

            const startTime = Date.now();
            const result = await DataTransformationService.transformData(veryLargeDataset);
            const processingTime = Date.now() - startTime;

            expect(result.transformedData).toHaveLength(50000);
            expect(processingTime).toBeLessThan(30000); // Should complete in under 30 seconds
        });
    });

    describe('Error Handling and Recovery', () => {
        test('should recover from partial processing failures', async () => {
            // Mix of valid and invalid data
            const mixedData = [
                ...TestDataFactory.generateValidSensorData(5),
                ...TestDataFactory.generateInvalidSensorData('invalid_timestamp'),
                ...TestDataFactory.generateValidSensorData(5),
                null,
                undefined,
                ...TestDataFactory.generateValidSensorData(5)
            ];

            const result = await DataTransformationService.transformData(mixedData);

            expect(result.transformationStats.outputRecords).toBe(15); // Only valid records
            expect(result.transformationStats.failedRecords).toBeGreaterThan(0);
        });

        test('should provide detailed error information', async () => {
            const consoleMock = TestUtils.mockConsole();

            try {
                await DataTransformationService.transformData(null);
            } catch (error) {
                expect(error).toBeDefined();
            }

            expect(consoleMock.errors.length).toBeGreaterThan(0);
            consoleMock.restore();
        });
    });
});
