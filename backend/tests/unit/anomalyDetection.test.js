/**
 * Anomaly Detection Algorithms Unit Tests
 * Comprehensive testing of outlier detection, z-score, and anomaly classification
 */

const StatisticsUtils = require('../../src/utils/statistics');
const TestDataFactory = require('../helpers/testDataFactory');
const TestUtils = require('../helpers/testUtils');

describe('Anomaly Detection Algorithms', () => {
    describe('Z-Score Calculation - Positive Cases', () => {
        test('should calculate z-scores correctly for normal distribution', () => {
            const values = [10, 12, 23, 23, 16, 23, 21, 16];
            const zScores = StatisticsUtils.calculateZScore(values);

            expect(zScores).toHaveLength(values.length);
            zScores.forEach(score => {
                expect(typeof score).toBe('number');
                expect(isNaN(score)).toBe(false);
                expect(score).toBeGreaterThanOrEqual(0);
            });
        });

        test('should identify outliers correctly using z-score method', () => {
            // Create data with known outliers
            const normalValues = Array.from({ length: 100 }, () => Math.random() * 10 + 20); // Normal range 20-30
            const outliers = [100, -50, 150]; // Clear outliers
            const allValues = [...normalValues, ...outliers];

            const zScores = StatisticsUtils.calculateZScore(allValues);

            // The outlier z-scores should be significantly higher
            const outlierIndices = [100, 101, 102]; // Positions of outliers
            outlierIndices.forEach(index => {
                expect(zScores[index]).toBeGreaterThan(3); // Standard threshold
            });
        });

        test('should handle uniform data (no variance)', () => {
            const uniformValues = [5, 5, 5, 5, 5];
            const zScores = StatisticsUtils.calculateZScore(uniformValues);

            zScores.forEach(score => {
                expect(score).toBe(0); // No variance means z-score = 0
            });
        });

        test('should handle very small variance correctly', () => {
            const smallVarianceValues = [10.001, 10.002, 10.001, 10.002, 10.001];
            const zScores = StatisticsUtils.calculateZScore(smallVarianceValues);

            expect(zScores).toHaveLength(5);
            zScores.forEach(score => {
                expect(isFinite(score)).toBe(true);
            });
        });

        test('should be mathematically consistent', () => {
            const values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
            const zScores = StatisticsUtils.calculateZScore(values);

            // Z-scores should be symmetric around the mean
            const mean = values.reduce((a, b) => a + b) / values.length;
            const centerIndex = Math.floor(values.length / 2);

            expect(zScores[0]).toBeCloseTo(zScores[zScores.length - 1], 2);
            expect(zScores[centerIndex]).toBeLessThan(zScores[0]);
        });
    });

    describe('Z-Score Calculation - Negative Cases', () => {
        test('should handle empty array gracefully', () => {
            expect(() => StatisticsUtils.calculateZScore([])).toThrow();
        });

        test('should handle null/undefined input', () => {
            expect(() => StatisticsUtils.calculateZScore(null)).toThrow();
            expect(() => StatisticsUtils.calculateZScore(undefined)).toThrow();
        });

        test('should handle array with single value', () => {
            const singleValue = [42];
            expect(() => StatisticsUtils.calculateZScore(singleValue)).toThrow();
        });

        test('should handle non-numeric values', () => {
            const invalidValues = [1, 'string', 3, null, 5];
            expect(() => StatisticsUtils.calculateZScore(invalidValues)).toThrow();
        });

        test('should handle infinite values', () => {
            const infiniteValues = [1, 2, Infinity, 4, 5];
            const zScores = StatisticsUtils.calculateZScore(infiniteValues);

            expect(zScores.some(score => !isFinite(score))).toBe(true);
        });
    });

    describe('Z-Score Calculation - Edge Cases', () => {
        test('should handle very large numbers', () => {
            const largeNumbers = [1e10, 1e10 + 1, 1e10 + 2, 1e10 + 3, 1e10 + 4];
            const zScores = StatisticsUtils.calculateZScore(largeNumbers);

            expect(zScores).toHaveLength(5);
            zScores.forEach(score => {
                expect(isFinite(score)).toBe(true);
            });
        });

        test('should handle very small numbers', () => {
            const smallNumbers = [1e-10, 2e-10, 3e-10, 4e-10, 5e-10];
            const zScores = StatisticsUtils.calculateZScore(smallNumbers);

            expect(zScores).toHaveLength(5);
            zScores.forEach(score => {
                expect(isFinite(score)).toBe(true);
            });
        });

        test('should handle negative numbers', () => {
            const negativeNumbers = [-10, -5, 0, 5, 10];
            const zScores = StatisticsUtils.calculateZScore(negativeNumbers);

            expect(zScores).toHaveLength(5);
            zScores.forEach(score => {
                expect(score).toBeGreaterThanOrEqual(0);
                expect(isFinite(score)).toBe(true);
            });
        });

        test('should handle alternating extreme values', () => {
            const alternatingValues = [0, 100, 0, 100, 0, 100];
            const zScores = StatisticsUtils.calculateZScore(alternatingValues);

            expect(zScores).toHaveLength(6);
            // All values should have the same z-score in this pattern
            const firstScore = zScores[0];
            zScores.forEach(score => {
                expect(score).toBeCloseTo(firstScore, 2);
            });
        });
    });

    describe('Outlier Detection - Positive Cases', () => {
        test('should detect outliers in sensor data correctly', () => {
            const testData = TestDataFactory.generateDataWithOutliers(100, 10);

            const result = StatisticsUtils.detectOutliers(testData, 3);

            expect(result).toHaveProperty('cleaned');
            expect(result).toHaveProperty('outliers');
            expect(result.outliers.length).toBeGreaterThan(0);
            expect(result.cleaned.length).toBe(testData.length);

            // Verify outliers have z_score > 3
            result.outliers.forEach(outlier => {
                expect(outlier.z_score).toBeGreaterThan(3);
            });
        });

        test('should group by reading_type for outlier detection', () => {
            const mixedData = [
                ...TestDataFactory.generateValidSensorData(20, { readingTypes: ['temperature'] }),
                ...TestDataFactory.generateValidSensorData(20, { readingTypes: ['humidity'] })
            ];

            // Add specific outliers for each type
            mixedData.push({
                sensor_id: 'sensor_001',
                timestamp: '2023-06-01T10:00:00Z',
                reading_type: 'temperature',
                value: 1000, // Extreme temperature outlier
                battery_level: 50
            });

            mixedData.push({
                sensor_id: 'sensor_002',
                timestamp: '2023-06-01T11:00:00Z',
                reading_type: 'humidity',
                value: 500, // Extreme humidity outlier
                battery_level: 50
            });

            const result = StatisticsUtils.detectOutliers(mixedData, 2);

            expect(result.outliers.length).toBeGreaterThan(0);

            // Check that outliers were detected for both reading types
            const tempOutliers = result.outliers.filter(o => o.reading_type === 'temperature');
            const humidityOutliers = result.outliers.filter(o => o.reading_type === 'humidity');

            expect(tempOutliers.length).toBeGreaterThan(0);
            expect(humidityOutliers.length).toBeGreaterThan(0);
        });

        test('should replace outliers with median values', () => {
            const testData = [
                { sensor_id: 'sensor_001', reading_type: 'temperature', value: 20, timestamp: '2023-06-01T01:00:00Z', battery_level: 50 },
                { sensor_id: 'sensor_001', reading_type: 'temperature', value: 21, timestamp: '2023-06-01T02:00:00Z', battery_level: 50 },
                { sensor_id: 'sensor_001', reading_type: 'temperature', value: 22, timestamp: '2023-06-01T03:00:00Z', battery_level: 50 },
                { sensor_id: 'sensor_001', reading_type: 'temperature', value: 23, timestamp: '2023-06-01T04:00:00Z', battery_level: 50 },
                { sensor_id: 'sensor_001', reading_type: 'temperature', value: 1000, timestamp: '2023-06-01T05:00:00Z', battery_level: 50 } // Outlier
            ];

            const result = StatisticsUtils.detectOutliers(testData, 2);

            // Find the corrected outlier record
            const correctedRecord = result.cleaned.find(r => r.outlier_corrected === true);
            expect(correctedRecord).toBeDefined();
            expect(correctedRecord.value).not.toBe(1000); // Should be replaced
            expect(correctedRecord.value).toBeCloseTo(22, 1); // Should be close to median
        });

        test('should handle different threshold values', () => {
            const testData = TestDataFactory.generateDataWithOutliers(50, 20);

            const strictResult = StatisticsUtils.detectOutliers(testData, 2); // Strict threshold
            const lenientResult = StatisticsUtils.detectOutliers(testData, 4); // Lenient threshold

            expect(strictResult.outliers.length).toBeGreaterThanOrEqual(lenientResult.outliers.length);
        });

        test('should preserve non-outlier data unchanged', () => {
            const normalData = TestDataFactory.generateValidSensorData(20);

            const result = StatisticsUtils.detectOutliers(normalData, 3);

            const unchangedRecords = result.cleaned.filter(r => r.outlier_corrected === false);
            expect(unchangedRecords.length).toBeGreaterThan(0);

            unchangedRecords.forEach(record => {
                const original = normalData.find(d =>
                    d.sensor_id === record.sensor_id &&
                    d.timestamp === record.timestamp &&
                    d.reading_type === record.reading_type
                );
                expect(record.value).toBe(original.value);
            });
        });
    });

    describe('Outlier Detection - Negative Cases', () => {
        test('should handle empty dataset', () => {
            const result = StatisticsUtils.detectOutliers([], 3);

            expect(result.cleaned).toHaveLength(0);
            expect(result.outliers).toHaveLength(0);
        });

        test('should handle dataset with all identical values', () => {
            const identicalData = Array.from({ length: 10 }, (_, i) => ({
                sensor_id: 'sensor_001',
                timestamp: `2023-06-01T${i.toString().padStart(2, '0')}:00:00Z`,
                reading_type: 'temperature',
                value: 25, // All identical
                battery_level: 50
            }));

            const result = StatisticsUtils.detectOutliers(identicalData, 3);

            expect(result.outliers).toHaveLength(0); // No outliers with identical values
            expect(result.cleaned).toHaveLength(10);
        });

        test('should handle invalid threshold values', () => {
            const testData = TestDataFactory.generateValidSensorData(10);

            // Negative threshold should still work but detect everything as outlier
            const negativeThresholdResult = StatisticsUtils.detectOutliers(testData, -1);
            expect(negativeThresholdResult.outliers.length).toBe(testData.length);

            // Zero threshold should detect everything as outlier
            const zeroThresholdResult = StatisticsUtils.detectOutliers(testData, 0);
            expect(zeroThresholdResult.outliers.length).toBe(testData.length);
        });

        test('should handle missing value field', () => {
            const invalidData = [
                { sensor_id: 'sensor_001', reading_type: 'temperature', timestamp: '2023-06-01T01:00:00Z', battery_level: 50 },
                { sensor_id: 'sensor_002', reading_type: 'temperature', value: null, timestamp: '2023-06-01T02:00:00Z', battery_level: 50 }
            ];

            expect(() => StatisticsUtils.detectOutliers(invalidData, 3)).toThrow();
        });

        test('should handle non-numeric values', () => {
            const invalidData = [
                { sensor_id: 'sensor_001', reading_type: 'temperature', value: 'not-a-number', timestamp: '2023-06-01T01:00:00Z', battery_level: 50 },
                { sensor_id: 'sensor_002', reading_type: 'temperature', value: 25, timestamp: '2023-06-01T02:00:00Z', battery_level: 50 }
            ];

            expect(() => StatisticsUtils.detectOutliers(invalidData, 3)).toThrow();
        });
    });

    describe('Outlier Detection - Edge Cases', () => {
        test('should handle dataset with only one record per reading type', () => {
            const singleRecordData = [
                { sensor_id: 'sensor_001', reading_type: 'temperature', value: 25, timestamp: '2023-06-01T01:00:00Z', battery_level: 50 },
                { sensor_id: 'sensor_002', reading_type: 'humidity', value: 60, timestamp: '2023-06-01T02:00:00Z', battery_level: 50 }
            ];

            const result = StatisticsUtils.detectOutliers(singleRecordData, 3);

            // Single records cannot be outliers
            expect(result.outliers).toHaveLength(0);
            expect(result.cleaned).toHaveLength(2);
        });

        test('should handle dataset with only two records per reading type', () => {
            const twoRecordData = [
                { sensor_id: 'sensor_001', reading_type: 'temperature', value: 25, timestamp: '2023-06-01T01:00:00Z', battery_level: 50 },
                { sensor_id: 'sensor_001', reading_type: 'temperature', value: 26, timestamp: '2023-06-01T02:00:00Z', battery_level: 50 }
            ];

            const result = StatisticsUtils.detectOutliers(twoRecordData, 3);

            // With only two values, standard deviation is minimal, so no outliers should be detected
            expect(result.outliers).toHaveLength(0);
            expect(result.cleaned).toHaveLength(2);
        });

        test('should handle extreme threshold values', () => {
            const testData = TestDataFactory.generateDataWithOutliers(100, 10);

            // Very high threshold should detect no outliers
            const veryHighThreshold = StatisticsUtils.detectOutliers(testData, 1000);
            expect(veryHighThreshold.outliers).toHaveLength(0);

            // Very low threshold should detect many outliers
            const veryLowThreshold = StatisticsUtils.detectOutliers(testData, 0.1);
            expect(veryLowThreshold.outliers.length).toBeGreaterThan(testData.length * 0.5);
        });

        test('should handle reading types with special characters', () => {
            const specialData = [
                { sensor_id: 'sensor_001', reading_type: 'temp-ñature_ös', value: 25, timestamp: '2023-06-01T01:00:00Z', battery_level: 50 },
                { sensor_id: 'sensor_002', reading_type: 'temp-ñature_ös', value: 26, timestamp: '2023-06-01T02:00:00Z', battery_level: 50 },
                { sensor_id: 'sensor_003', reading_type: 'temp-ñature_ös', value: 1000, timestamp: '2023-06-01T03:00:00Z', battery_level: 50 }
            ];

            const result = StatisticsUtils.detectOutliers(specialData, 2);

            expect(result.outliers.length).toBe(1);
            expect(result.outliers[0].reading_type).toBe('temp-ñature_ös');
        });

        test('should handle floating point precision issues', () => {
            const precisionData = [
                { sensor_id: 'sensor_001', reading_type: 'temperature', value: 0.1 + 0.2, timestamp: '2023-06-01T01:00:00Z', battery_level: 50 },
                { sensor_id: 'sensor_002', reading_type: 'temperature', value: 0.3, timestamp: '2023-06-01T02:00:00Z', battery_level: 50 },
                { sensor_id: 'sensor_003', reading_type: 'temperature', value: 0.30000000000000004, timestamp: '2023-06-01T03:00:00Z', battery_level: 50 }
            ];

            const result = StatisticsUtils.detectOutliers(precisionData, 3);

            // Should handle floating point precision gracefully
            expect(result.cleaned).toHaveLength(3);
        });
    });

    describe('Statistical Accuracy Tests', () => {
        test('should maintain statistical accuracy with large datasets', () => {
            const largeDataset = TestDataFactory.generateValidSensorData(10000, { readingTypes: ['temperature'] });

            const performanceTestFn = TestUtils.performanceTest(
                () => StatisticsUtils.detectOutliers(largeDataset, 3),
                5000 // 5 second maximum
            );

            const result = performanceTestFn();
            expect(result).toBeDefined();
        });

        test('should be consistent across multiple runs', () => {
            const testData = TestDataFactory.generateDataWithOutliers(100, 15);

            const result1 = StatisticsUtils.detectOutliers(testData, 3);
            const result2 = StatisticsUtils.detectOutliers(testData, 3);

            expect(result1.outliers.length).toBe(result2.outliers.length);
            expect(result1.cleaned.length).toBe(result2.cleaned.length);
        });

        test('should properly calculate statistics for different distributions', () => {
            // Normal distribution
            const normalData = Array.from({ length: 1000 }, () => ({
                sensor_id: 'sensor_001',
                reading_type: 'temperature',
                value: TestUtils.generateRandomNumber(20, 30), // Normal range
                timestamp: new Date().toISOString(),
                battery_level: 50
            }));

            // Add some clear outliers
            normalData.push({
                sensor_id: 'sensor_001',
                reading_type: 'temperature',
                value: 100, // Clear outlier
                timestamp: new Date().toISOString(),
                battery_level: 50
            });

            const result = StatisticsUtils.detectOutliers(normalData, 3);

            expect(result.outliers.length).toBeGreaterThan(0);
            expect(result.outliers.length).toBeLessThan(normalData.length * 0.1); // Less than 10% should be outliers
        });
    });

    describe('Memory and Performance Tests', () => {
        test('should handle memory efficiently for large datasets', async () => {
            const memoryTestFn = TestUtils.memoryTest(
                () => {
                    const largeDataset = TestDataFactory.generateDataWithOutliers(50000, 5);
                    return StatisticsUtils.detectOutliers(largeDataset, 3);
                },
                100 // 100 MB limit
            );

            const result = await memoryTestFn();
            expect(result.cleaned.length).toBe(50000);
        });

        test('should scale linearly with dataset size', () => {
            const smallDataset = TestDataFactory.generateValidSensorData(1000);
            const largeDataset = TestDataFactory.generateValidSensorData(10000);

            const startSmall = Date.now();
            StatisticsUtils.detectOutliers(smallDataset, 3);
            const timeSmall = Date.now() - startSmall;

            const startLarge = Date.now();
            StatisticsUtils.detectOutliers(largeDataset, 3);
            const timeLarge = Date.now() - startLarge;

            // Large dataset should not take more than 20x longer (allowing for overhead)
            expect(timeLarge).toBeLessThan(timeSmall * 20);
        });
    });

    describe('Integration with Real-World Agricultural Data', () => {
        test('should handle typical agricultural sensor value ranges', () => {
            const agriculturalData = [
                // Temperature readings (typical range: -10 to 45°C)
                ...Array.from({ length: 50 }, (_, i) => ({
                    sensor_id: 'temp_sensor_001',
                    reading_type: 'temperature',
                    value: TestUtils.generateRandomNumber(-5, 40),
                    timestamp: new Date(Date.now() + i * 3600000).toISOString(),
                    battery_level: TestUtils.generateRandomNumber(50, 100)
                })),
                // Humidity readings (typical range: 0-100%)
                ...Array.from({ length: 50 }, (_, i) => ({
                    sensor_id: 'humidity_sensor_001',
                    reading_type: 'humidity',
                    value: TestUtils.generateRandomNumber(30, 90),
                    timestamp: new Date(Date.now() + i * 3600000).toISOString(),
                    battery_level: TestUtils.generateRandomNumber(50, 100)
                })),
                // Add realistic outliers
                { sensor_id: 'temp_sensor_001', reading_type: 'temperature', value: 85, timestamp: new Date().toISOString(), battery_level: 50 }, // Heat wave
                { sensor_id: 'humidity_sensor_001', reading_type: 'humidity', value: -5, timestamp: new Date().toISOString(), battery_level: 50 } // Sensor malfunction
            ];

            const result = StatisticsUtils.detectOutliers(agriculturalData, 3);

            expect(result.outliers.length).toBeGreaterThan(0);

            // Verify that realistic outliers are detected
            const tempOutliers = result.outliers.filter(o => o.reading_type === 'temperature');
            const humidityOutliers = result.outliers.filter(o => o.reading_type === 'humidity');

            expect(tempOutliers.some(o => o.value > 60)).toBe(true); // Heat wave detected
            expect(humidityOutliers.some(o => o.value < 0)).toBe(true); // Negative humidity detected
        });

        test('should maintain data integrity during outlier correction', () => {
            const originalData = TestDataFactory.generateDataWithOutliers(100, 10);
            const originalDataCopy = JSON.parse(JSON.stringify(originalData));

            const result = StatisticsUtils.detectOutliers(originalData, 3);

            // Original data should not be modified
            expect(originalData).toEqual(originalDataCopy);

            // Result should have same number of records
            expect(result.cleaned.length).toBe(originalData.length);

            // All cleaned records should have required fields
            result.cleaned.forEach(record => {
                expect(record).toHaveProperty('sensor_id');
                expect(record).toHaveProperty('reading_type');
                expect(record).toHaveProperty('value');
                expect(record).toHaveProperty('timestamp');
                expect(record).toHaveProperty('battery_level');
                expect(record).toHaveProperty('outlier_corrected');
            });
        });
    });
});
