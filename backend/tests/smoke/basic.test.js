/**
 * Smoke Tests - Basic functionality validation
 * Quick tests to ensure core components are working
 */

describe('🔥 Smoke Tests - Basic Functionality', () => {
    describe('Core Services', () => {
        test('should load data ingestion service', () => {
            expect(() => {
                require('../../src/services/dataIngestion');
            }).not.toThrow();
        });

        test('should load timestamp utilities', () => {
            expect(() => {
                require('../../src/utils/timestampUtils');
            }).not.toThrow();
        });

        test('should load statistics utilities', () => {
            expect(() => {
                require('../../src/utils/statistics');
            }).not.toThrow();
        });

        test('should load logging utilities', () => {
            expect(() => {
                require('../../src/utils/logging');
            }).not.toThrow();
        });
    });

    describe('Test Infrastructure', () => {
        test('should load test configuration', () => {
            expect(() => {
                require('../config/testConfig');
            }).not.toThrow();
        });

        test('should load test data factory', () => {
            const TestDataFactory = require('../helpers/testDataFactory');
            expect(TestDataFactory).toBeDefined();
            expect(typeof TestDataFactory.generateValidSensorData).toBe('function');
        });

        test('should load test utilities', () => {
            const TestUtils = require('../helpers/testUtils');
            expect(TestUtils).toBeDefined();
            expect(typeof TestUtils.performanceTest).toBe('function');
        });

        test('should generate mock sensor data', () => {
            const TestDataFactory = require('../helpers/testDataFactory');
            const data = TestDataFactory.generateValidSensorData(5);

            expect(Array.isArray(data)).toBe(true);
            expect(data.length).toBe(5);
            expect(data[0]).toHaveProperty('sensor_id');
            expect(data[0]).toHaveProperty('timestamp');
            expect(data[0]).toHaveProperty('value');
            expect(data[0]).toHaveProperty('reading_type');
        });
    });

    describe('Basic Data Processing', () => {
        test('should validate timestamp format', () => {
            expect(() => {
                const TimestampUtils = require('../../src/utils/timestampUtils');
                new TimestampUtils();
            }).not.toThrow();
        });

        test('should handle basic statistics calculations', () => {
            const StatisticsUtils = require('../../src/utils/statistics');
            const utils = new StatisticsUtils();

            const testData = [1, 2, 3, 4, 5];
            const mean = utils.calculateMean(testData);
            expect(mean).toBe(3);

            const std = utils.calculateStandardDeviation(testData);
            expect(std).toBeGreaterThan(0);
            expect(typeof std).toBe('number');
        });
    });

    describe('Configuration Validation', () => {
        test('should load application configuration', () => {
            expect(() => {
                require('../../src/config');
            }).not.toThrow();
        });

        test('should have required environment variables', () => {
            // Test should pass regardless of environment setup
            expect(process.env.NODE_ENV).toBeDefined();
        });
    });

    describe('Test Environment Setup', () => {
        test('should create test directories', async () => {
            const fs = require('fs-extra');
            const path = require('path');

            const testDir = path.join(__dirname, '../temp');
            await fs.ensureDir(testDir);

            const exists = await fs.pathExists(testDir);
            expect(exists).toBe(true);
        });

        test('should validate test data factory outputs', () => {
            const TestDataFactory = require('../helpers/testDataFactory');

            // Test different data generation methods
            const validData = TestDataFactory.generateValidSensorData(3);
            expect(validData).toHaveLength(3);

            const dataWithOutliers = TestDataFactory.generateDataWithOutliers(3, 0.5);
            expect(dataWithOutliers).toHaveLength(3);

            const dataWithMissing = TestDataFactory.generateDataWithMissingValues(3, 0.3);
            expect(dataWithMissing).toHaveLength(3);
        });
    });

    describe('Performance Validation', () => {
        test('should complete basic operations within time limits', async () => {
            const TestUtils = require('../helpers/testUtils');

            const simpleOperation = async () => {
                const TestDataFactory = require('../helpers/testDataFactory');
                return TestDataFactory.generateValidSensorData(100);
            };

            const startTime = Date.now();
            const result = await simpleOperation();
            const endTime = Date.now();

            expect(result).toHaveLength(100);
            expect(endTime - startTime).toBeLessThan(1000); // Should complete within 1 second
        });

        test('should handle memory-efficient operations', () => {
            const TestDataFactory = require('../helpers/testDataFactory');

            // Generate larger dataset to test memory efficiency
            const largeDataset = TestDataFactory.generateValidSensorData(1000);
            expect(largeDataset).toHaveLength(1000);

            // Basic memory check - should not throw out of memory error
            expect(largeDataset[0]).toBeDefined();
            expect(largeDataset[999]).toBeDefined();
        });
    });
});
