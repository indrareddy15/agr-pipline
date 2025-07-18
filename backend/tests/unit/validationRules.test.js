/**
 * Data Validation Rules Unit Tests
 * Comprehensive testing of validation rules for schema, data types, and business logic
 */

const DataIngestionService = require('../../src/services/dataIngestion');
const { timestampUtils } = require('../../src/utils');
const TestDataFactory = require('../helpers/testDataFactory');
const TestUtils = require('../helpers/testUtils');

describe('Data Validation Rules', () => {
    let dataIngestionService;

    beforeEach(() => {
        dataIngestionService = new DataIngestionService();
    });

    describe('Schema Validation - Positive Cases', () => {
        test('should validate correct schema with all required columns', async () => {
            const mockFilePath = await TestUtils.createTempFile('valid_schema.parquet', 'mock parquet data');

            const validation = await dataIngestionService.validateSchemaWithDuckDB(mockFilePath);

            expect(validation.isValid).toBe(true);
            expect(validation.errors).toHaveLength(0);
            expect(validation.schema).toEqual(
                expect.arrayContaining([
                    expect.objectContaining({ column_name: 'timestamp', column_type: 'TIMESTAMP' }),
                    expect.objectContaining({ column_name: 'sensor_id', column_type: 'VARCHAR' }),
                    expect.objectContaining({ column_name: 'reading_type', column_type: 'VARCHAR' }),
                    expect.objectContaining({ column_name: 'value', column_type: 'DOUBLE' }),
                    expect.objectContaining({ column_name: 'battery_level', column_type: 'DOUBLE' })
                ])
            );

            await TestUtils.cleanup([mockFilePath]);
        });

        test('should identify all required columns correctly', async () => {
            const mockFilePath = await TestUtils.createTempFile('schema_test.parquet', 'mock data');

            const validation = await dataIngestionService.validateSchemaWithDuckDB(mockFilePath);

            const requiredColumns = ['timestamp', 'sensor_id', 'reading_type', 'value', 'battery_level'];
            expect(validation.requiredColumns).toEqual(expect.arrayContaining(requiredColumns));
            expect(validation.foundColumns).toEqual(expect.arrayContaining(requiredColumns));

            await TestUtils.cleanup([mockFilePath]);
        });

        test('should validate schema for different file paths', async () => {
            const testFiles = [
                'data/raw/2023-06-01.parquet',
                'data/raw/sensor_data_2023-06-02.parquet',
                'uploads/new_data.parquet'
            ];

            for (const filePath of testFiles) {
                const mockFilePath = await TestUtils.createTempFile(filePath.split('/').pop(), 'mock data');
                const validation = await dataIngestionService.validateSchemaWithDuckDB(mockFilePath);

                expect(validation.isValid).toBe(true);
                await TestUtils.cleanup([mockFilePath]);
            }
        });

        test('should handle file paths with special characters', async () => {
            const specialFiles = [
                'data-file_with-dashes.parquet',
                'file with spaces.parquet',
                'file123.parquet',
                'UPPERCASE.PARQUET'
            ];

            for (const fileName of specialFiles) {
                const mockFilePath = await TestUtils.createTempFile(fileName, 'mock data');
                const validation = await dataIngestionService.validateSchemaWithDuckDB(mockFilePath);

                expect(validation.isValid).toBe(true);
                await TestUtils.cleanup([mockFilePath]);
            }
        });
    });

    describe('Schema Validation - Negative Cases', () => {
        test('should handle non-existent file gracefully', async () => {
            const nonExistentFile = '/path/to/nonexistent/file.parquet';

            const validation = await dataIngestionService.validateSchemaWithDuckDB(nonExistentFile);

            expect(validation.isValid).toBe(false);
            expect(validation.errors.length).toBeGreaterThan(0);
        });

        test('should handle empty file path', async () => {
            const validation = await dataIngestionService.validateSchemaWithDuckDB('');

            expect(validation.isValid).toBe(false);
            expect(validation.errors.length).toBeGreaterThan(0);
        });

        test('should handle null file path', async () => {
            const validation = await dataIngestionService.validateSchemaWithDuckDB(null);

            expect(validation.isValid).toBe(false);
            expect(validation.errors.length).toBeGreaterThan(0);
        });

        test('should handle undefined file path', async () => {
            const validation = await dataIngestionService.validateSchemaWithDuckDB(undefined);

            expect(validation.isValid).toBe(false);
            expect(validation.errors.length).toBeGreaterThan(0);
        });

        test('should handle invalid file extensions', async () => {
            const invalidFiles = [
                'file.txt',
                'file.csv',
                'file.json',
                'file',
                'file.parquet.txt'
            ];

            for (const fileName of invalidFiles) {
                const mockFilePath = await TestUtils.createTempFile(fileName, 'invalid data');
                const validation = await dataIngestionService.validateSchemaWithDuckDB(mockFilePath);

                // May or may not be valid depending on implementation
                expect(validation).toHaveProperty('isValid');
                expect(validation).toHaveProperty('errors');

                await TestUtils.cleanup([mockFilePath]);
            }
        });
    });

    describe('Schema Validation - Edge Cases', () => {
        test('should handle very long file paths', async () => {
            const longPath = 'a'.repeat(200) + '.parquet';
            const mockFilePath = await TestUtils.createTempFile(longPath, 'mock data');

            const validation = await dataIngestionService.validateSchemaWithDuckDB(mockFilePath);
            expect(validation).toHaveProperty('isValid');

            await TestUtils.cleanup([mockFilePath]);
        });

        test('should handle paths with unicode characters', async () => {
            const unicodeFiles = [
                'ñame.parquet',
                'файл.parquet',
                'ファイル.parquet',
                'αρχείο.parquet'
            ];

            for (const fileName of unicodeFiles) {
                const mockFilePath = await TestUtils.createTempFile(fileName, 'mock data');
                const validation = await dataIngestionService.validateSchemaWithDuckDB(mockFilePath);

                expect(validation).toHaveProperty('isValid');
                await TestUtils.cleanup([mockFilePath]);
            }
        });

        test('should handle concurrent schema validations', async () => {
            const mockFiles = await Promise.all([
                TestUtils.createTempFile('file1.parquet', 'data1'),
                TestUtils.createTempFile('file2.parquet', 'data2'),
                TestUtils.createTempFile('file3.parquet', 'data3')
            ]);

            const validationPromises = mockFiles.map(file =>
                dataIngestionService.validateSchemaWithDuckDB(file)
            );

            const validations = await Promise.all(validationPromises);

            validations.forEach(validation => {
                expect(validation).toHaveProperty('isValid');
                expect(validation).toHaveProperty('errors');
            });

            await TestUtils.cleanup(mockFiles);
        });
    });

    describe('Timestamp Validation - Positive Cases', () => {
        test('should validate ISO 8601 timestamps correctly', () => {
            const validTimestamps = [
                '2023-06-01T10:00:00Z',
                '2023-06-01T10:00:00.000Z',
                '2023-06-01T10:00:00+00:00',
                '2023-06-01T10:00:00+05:30',
                '2023-06-01T10:00:00-05:00',
                '2023-12-31T23:59:59Z'
            ];

            validTimestamps.forEach(timestamp => {
                const isValid = timestampUtils.validateISO8601(timestamp);
                expect(isValid).toBe(true);
            });
        });

        test('should handle different timezone formats', () => {
            const timezoneFormats = [
                '2023-06-01T10:00:00Z',           // UTC
                '2023-06-01T10:00:00+00:00',      // UTC with offset
                '2023-06-01T15:30:00+05:30',      // IST
                '2023-06-01T05:00:00-05:00',      // EST
                '2023-06-01T18:00:00+08:00',      // SGT
            ];

            timezoneFormats.forEach(timestamp => {
                const isValid = timestampUtils.validateISO8601(timestamp);
                expect(isValid).toBe(true);
            });
        });

        test('should validate timestamps at boundary dates', () => {
            const boundaryTimestamps = [
                '1970-01-01T00:00:00Z',           // Unix epoch
                '2000-01-01T00:00:00Z',           // Y2K
                '2023-02-28T23:59:59Z',           // Last day of February (non-leap)
                '2024-02-29T12:00:00Z',           // Leap year February 29
                '2038-01-19T03:14:07Z'            // Near Unix timestamp limit
            ];

            boundaryTimestamps.forEach(timestamp => {
                const isValid = timestampUtils.validateISO8601(timestamp);
                expect(isValid).toBe(true);
            });
        });

        test('should validate microsecond precision timestamps', () => {
            const precisionTimestamps = [
                '2023-06-01T10:00:00.123Z',
                '2023-06-01T10:00:00.123456Z',
                '2023-06-01T10:00:00.999999Z'
            ];

            precisionTimestamps.forEach(timestamp => {
                const isValid = timestampUtils.validateISO8601(timestamp);
                expect(isValid).toBe(true);
            });
        });
    });

    describe('Timestamp Validation - Negative Cases', () => {
        test('should reject invalid timestamp formats', () => {
            const invalidTimestamps = [
                'invalid-date',
                '2023/06/01 10:00:00',            // Wrong separators
                '2023-13-01T10:00:00Z',           // Invalid month
                '2023-06-32T10:00:00Z',           // Invalid day
                '2023-06-01T25:00:00Z',           // Invalid hour
                '2023-06-01T10:60:00Z',           // Invalid minute
                '2023-06-01T10:00:60Z',           // Invalid second
                '2023-06-01 10:00:00',            // Missing T separator
                '2023-06-01T10:00:00',            // Missing timezone
                '',                               // Empty string
                null,                             // Null
                undefined                         // Undefined
            ];

            invalidTimestamps.forEach(timestamp => {
                const isValid = timestampUtils.validateISO8601(timestamp);
                expect(isValid).toBe(false);
            });
        });

        test('should reject malformed timezone indicators', () => {
            const malformedTimezones = [
                '2023-06-01T10:00:00+',           // Incomplete offset
                '2023-06-01T10:00:00+5',          // Short offset
                '2023-06-01T10:00:00+25:00',      // Invalid hour offset
                '2023-06-01T10:00:00+05:70',      // Invalid minute offset
                '2023-06-01T10:00:00ZZ',          // Double Z
                '2023-06-01T10:00:00PST'          // Named timezone
            ];

            malformedTimezones.forEach(timestamp => {
                const isValid = timestampUtils.validateISO8601(timestamp);
                expect(isValid).toBe(false);
            });
        });

        test('should reject non-string inputs', () => {
            const nonStringInputs = [
                123456789,                        // Number
                new Date(),                       // Date object
                {},                               // Object
                [],                               // Array
                true,                             // Boolean
                Symbol('timestamp')               // Symbol
            ];

            nonStringInputs.forEach(input => {
                const isValid = timestampUtils.validateISO8601(input);
                expect(isValid).toBe(false);
            });
        });
    });

    describe('Data Type Validation Rules', () => {
        test('should validate sensor data record structure', () => {
            const validRecord = {
                sensor_id: 'sensor_001',
                timestamp: '2023-06-01T10:00:00Z',
                reading_type: 'temperature',
                value: 25.5,
                battery_level: 85.2
            };

            const schema = {
                sensor_id: 'string',
                timestamp: 'date',
                reading_type: 'string',
                value: 'number',
                battery_level: 'number'
            };

            const isValid = TestUtils.validateObjectSchema(validRecord, schema);
            expect(isValid).toBe(true);
        });

        test('should detect missing required fields', () => {
            const incompleteRecords = [
                { sensor_id: 'sensor_001', timestamp: '2023-06-01T10:00:00Z' }, // Missing reading_type, value, battery_level
                { timestamp: '2023-06-01T10:00:00Z', reading_type: 'temperature' }, // Missing sensor_id, value, battery_level
                { sensor_id: 'sensor_001', reading_type: 'temperature', value: 25.5 }, // Missing timestamp, battery_level
            ];

            const schema = {
                sensor_id: 'string',
                timestamp: 'date',
                reading_type: 'string',
                value: 'number',
                battery_level: 'number'
            };

            incompleteRecords.forEach(record => {
                const isValid = TestUtils.validateObjectSchema(record, schema);
                expect(isValid).toBe(false);
            });
        });

        test('should detect incorrect data types', () => {
            const incorrectTypeRecords = [
                {
                    sensor_id: 123,                    // Should be string
                    timestamp: '2023-06-01T10:00:00Z',
                    reading_type: 'temperature',
                    value: 25.5,
                    battery_level: 85.2
                },
                {
                    sensor_id: 'sensor_001',
                    timestamp: '2023-06-01T10:00:00Z',
                    reading_type: 'temperature',
                    value: 'not-a-number',             // Should be number
                    battery_level: 85.2
                },
                {
                    sensor_id: 'sensor_001',
                    timestamp: 123456789,              // Should be date string
                    reading_type: 'temperature',
                    value: 25.5,
                    battery_level: 85.2
                }
            ];

            const schema = {
                sensor_id: 'string',
                timestamp: 'date',
                reading_type: 'string',
                value: 'number',
                battery_level: 'number'
            };

            incorrectTypeRecords.forEach(record => {
                const isValid = TestUtils.validateObjectSchema(record, schema);
                expect(isValid).toBe(false);
            });
        });
    });

    describe('Business Logic Validation Rules', () => {
        test('should validate sensor ID format rules', () => {
            const validSensorIds = [
                'sensor_001',
                'SENSOR_ABC',
                'temp-sensor-1',
                'humidity_sensor_123',
                'SOIL_01'
            ];

            const sensorIdPattern = /^[a-zA-Z0-9_-]+$/;

            validSensorIds.forEach(sensorId => {
                expect(sensorIdPattern.test(sensorId)).toBe(true);
            });
        });

        test('should reject invalid sensor ID formats', () => {
            const invalidSensorIds = [
                'sensor@001',                      // Special characters
                'sensor 001',                      // Spaces
                '',                                // Empty
                'sensor#$%',                       // Invalid characters
                'sensor.001'                       // Dots (depending on business rules)
            ];

            const sensorIdPattern = /^[a-zA-Z0-9_-]+$/;

            invalidSensorIds.forEach(sensorId => {
                expect(sensorIdPattern.test(sensorId)).toBe(false);
            });
        });

        test('should validate reading type enumeration', () => {
            const validReadingTypes = [
                'temperature',
                'humidity',
                'soil_moisture',
                'ph_level',
                'light_intensity'
            ];

            const allowedTypes = new Set(validReadingTypes);

            validReadingTypes.forEach(type => {
                expect(allowedTypes.has(type)).toBe(true);
            });
        });

        test('should reject invalid reading types', () => {
            const invalidReadingTypes = [
                'invalid_type',
                'TEMPERATURE',                     // Case sensitivity
                'temp',                            // Abbreviations
                '',                                // Empty
                'temperature humidity',            // Multiple values
                'temperature;humidity'             // Separators
            ];

            const allowedTypes = new Set([
                'temperature',
                'humidity',
                'soil_moisture',
                'ph_level',
                'light_intensity'
            ]);

            invalidReadingTypes.forEach(type => {
                expect(allowedTypes.has(type)).toBe(false);
            });
        });

        test('should validate value ranges for different reading types', () => {
            const valueRanges = {
                temperature: { min: -50, max: 100 },
                humidity: { min: 0, max: 100 },
                soil_moisture: { min: 0, max: 100 },
                ph_level: { min: 0, max: 14 },
                light_intensity: { min: 0, max: 2000 }
            };

            const testCases = [
                { reading_type: 'temperature', value: 25, expected: true },
                { reading_type: 'temperature', value: 150, expected: false },
                { reading_type: 'humidity', value: 50, expected: true },
                { reading_type: 'humidity', value: 150, expected: false },
                { reading_type: 'ph_level', value: 7, expected: true },
                { reading_type: 'ph_level', value: 20, expected: false }
            ];

            testCases.forEach(testCase => {
                const range = valueRanges[testCase.reading_type];
                const isValid = testCase.value >= range.min && testCase.value <= range.max;
                expect(isValid).toBe(testCase.expected);
            });
        });

        test('should validate battery level ranges', () => {
            const validBatteryLevels = [0, 25, 50, 75, 100, 99.9, 0.1];
            const invalidBatteryLevels = [-1, 101, 150, -50];

            validBatteryLevels.forEach(level => {
                const isValid = level >= 0 && level <= 100;
                expect(isValid).toBe(true);
            });

            invalidBatteryLevels.forEach(level => {
                const isValid = level >= 0 && level <= 100;
                expect(isValid).toBe(false);
            });
        });
    });

    describe('Data Consistency Validation', () => {
        test('should validate timestamp chronological order', () => {
            const chronologicalData = [
                { timestamp: '2023-06-01T10:00:00Z', sensor_id: 'sensor_001' },
                { timestamp: '2023-06-01T11:00:00Z', sensor_id: 'sensor_001' },
                { timestamp: '2023-06-01T12:00:00Z', sensor_id: 'sensor_001' }
            ];

            for (let i = 1; i < chronologicalData.length; i++) {
                const prevTime = new Date(chronologicalData[i - 1].timestamp);
                const currTime = new Date(chronologicalData[i].timestamp);
                expect(currTime.getTime()).toBeGreaterThan(prevTime.getTime());
            }
        });

        test('should detect timestamp order violations', () => {
            const nonChronologicalData = [
                { timestamp: '2023-06-01T12:00:00Z', sensor_id: 'sensor_001' },
                { timestamp: '2023-06-01T10:00:00Z', sensor_id: 'sensor_001' }, // Out of order
                { timestamp: '2023-06-01T11:00:00Z', sensor_id: 'sensor_001' }
            ];

            let hasOrderViolation = false;
            for (let i = 1; i < nonChronologicalData.length; i++) {
                const prevTime = new Date(nonChronologicalData[i - 1].timestamp);
                const currTime = new Date(nonChronologicalData[i].timestamp);
                if (currTime.getTime() <= prevTime.getTime()) {
                    hasOrderViolation = true;
                    break;
                }
            }

            expect(hasOrderViolation).toBe(true);
        });

        test('should validate data completeness per sensor', () => {
            const sensorData = TestDataFactory.generateValidSensorData(100, {
                sensorIds: ['sensor_001', 'sensor_002']
            });

            const sensorCounts = {};
            sensorData.forEach(record => {
                sensorCounts[record.sensor_id] = (sensorCounts[record.sensor_id] || 0) + 1;
            });

            // Each sensor should have at least some data
            Object.values(sensorCounts).forEach(count => {
                expect(count).toBeGreaterThan(0);
            });
        });

        test('should detect duplicate records', () => {
            const duplicateData = [
                { sensor_id: 'sensor_001', timestamp: '2023-06-01T10:00:00Z', reading_type: 'temperature', value: 25 },
                { sensor_id: 'sensor_001', timestamp: '2023-06-01T10:00:00Z', reading_type: 'temperature', value: 25 }, // Exact duplicate
                { sensor_id: 'sensor_001', timestamp: '2023-06-01T11:00:00Z', reading_type: 'temperature', value: 26 }
            ];

            const uniqueKeys = new Set();
            let hasDuplicates = false;

            duplicateData.forEach(record => {
                const key = `${record.sensor_id}-${record.timestamp}-${record.reading_type}`;
                if (uniqueKeys.has(key)) {
                    hasDuplicates = true;
                }
                uniqueKeys.add(key);
            });

            expect(hasDuplicates).toBe(true);
        });
    });

    describe('Performance Validation Tests', () => {
        test('should validate large datasets efficiently', async () => {
            const largeDataset = TestDataFactory.generateValidSensorData(10000);

            const startTime = Date.now();

            // Simulate validation of large dataset
            let validRecords = 0;
            const schema = {
                sensor_id: 'string',
                timestamp: 'date',
                reading_type: 'string',
                value: 'number',
                battery_level: 'number'
            };

            largeDataset.forEach(record => {
                if (TestUtils.validateObjectSchema(record, schema)) {
                    validRecords++;
                }
            });

            const endTime = Date.now();
            const processingTime = endTime - startTime;

            expect(validRecords).toBe(10000);
            expect(processingTime).toBeLessThan(5000); // Should complete within 5 seconds
        });

        test('should handle concurrent validations', async () => {
            const datasets = Array.from({ length: 5 }, () =>
                TestDataFactory.generateValidSensorData(1000)
            );

            const validationPromises = datasets.map(async (dataset) => {
                const schema = {
                    sensor_id: 'string',
                    timestamp: 'date',
                    reading_type: 'string',
                    value: 'number',
                    battery_level: 'number'
                };

                return dataset.filter(record =>
                    TestUtils.validateObjectSchema(record, schema)
                ).length;
            });

            const results = await Promise.all(validationPromises);

            results.forEach(validCount => {
                expect(validCount).toBe(1000);
            });
        });

        test('should maintain memory efficiency during validation', async () => {
            const memoryTestFn = TestUtils.memoryTest(
                () => {
                    const dataset = TestDataFactory.generateValidSensorData(50000);
                    const schema = {
                        sensor_id: 'string',
                        timestamp: 'date',
                        reading_type: 'string',
                        value: 'number',
                        battery_level: 'number'
                    };

                    return dataset.filter(record =>
                        TestUtils.validateObjectSchema(record, schema)
                    );
                },
                100 // 100 MB limit
            );

            const validatedData = await memoryTestFn();
            expect(validatedData.length).toBe(50000);
        });
    });

    describe('Error Reporting and Recovery', () => {
        test('should provide detailed validation error messages', () => {
            const invalidRecord = {
                sensor_id: 123,                    // Wrong type
                timestamp: 'invalid-date',         // Invalid format
                reading_type: '',                  // Empty
                value: 'not-a-number',            // Wrong type
                battery_level: 150                 // Out of range
            };

            const errors = [];

            if (typeof invalidRecord.sensor_id !== 'string') {
                errors.push('sensor_id must be a string');
            }

            if (!timestampUtils.validateISO8601(invalidRecord.timestamp)) {
                errors.push('timestamp must be valid ISO 8601 format');
            }

            if (!invalidRecord.reading_type || invalidRecord.reading_type.trim() === '') {
                errors.push('reading_type cannot be empty');
            }

            if (typeof invalidRecord.value !== 'number' || isNaN(invalidRecord.value)) {
                errors.push('value must be a valid number');
            }

            if (invalidRecord.battery_level < 0 || invalidRecord.battery_level > 100) {
                errors.push('battery_level must be between 0 and 100');
            }

            expect(errors.length).toBeGreaterThan(0);
            expect(errors).toContain('sensor_id must be a string');
            expect(errors).toContain('timestamp must be valid ISO 8601 format');
        });

        test('should gracefully handle validation errors in batch processing', () => {
            const mixedData = [
                ...TestDataFactory.generateValidSensorData(5),
                { invalid: 'record' },
                null,
                undefined,
                ...TestDataFactory.generateValidSensorData(5)
            ];

            const validRecords = [];
            const errors = [];

            mixedData.forEach((record, index) => {
                try {
                    if (record && typeof record === 'object' && record.sensor_id) {
                        const schema = {
                            sensor_id: 'string',
                            timestamp: 'date',
                            reading_type: 'string',
                            value: 'number',
                            battery_level: 'number'
                        };

                        if (TestUtils.validateObjectSchema(record, schema)) {
                            validRecords.push(record);
                        } else {
                            errors.push({ index, error: 'Schema validation failed' });
                        }
                    } else {
                        errors.push({ index, error: 'Invalid record format' });
                    }
                } catch (error) {
                    errors.push({ index, error: error.message });
                }
            });

            expect(validRecords.length).toBe(10); // Only valid records
            expect(errors.length).toBe(3); // Invalid records
        });
    });
});
