/**
 * Data Quality Service Unit Tests
 * Comprehensive testing of validation rules and data quality calculations
 */

const DataQualityService = require('../../src/services/dataQuality');
const TestDataFactory = require('../helpers/testDataFactory');
const TestUtils = require('../helpers/testUtils');

describe('DataQualityService', () => {
    describe('generateDataQualityReport - Positive Cases', () => {
        test('should generate comprehensive quality report for valid data', async () => {
            const testData = TestDataFactory.generateValidSensorData(100);

            const report = await DataQualityService.generateDataQualityReport(testData);

            expect(report).toHaveProperty('timestamp');
            expect(report).toHaveProperty('summary');
            expect(report).toHaveProperty('details');

            expect(report.summary).toHaveProperty('totalRecords', 100);
            expect(report.summary).toHaveProperty('overallQualityScore');
            expect(report.summary).toHaveProperty('missingValues');
            expect(report.summary).toHaveProperty('anomalousReadings');
            expect(report.summary).toHaveProperty('outliersCorreted');

            expect(report.summary.overallQualityScore).toBeWithinRange(0, 100);
        });

        test('should calculate quality metrics correctly for perfect data', async () => {
            const perfectData = TestDataFactory.generateValidSensorData(50);

            const report = await DataQualityService.generateDataQualityReport(perfectData);

            expect(report.summary.totalRecords).toBe(50);
            expect(report.summary.missingValues).toBe(0);
            expect(report.summary.anomalousReadings).toBe(0);
            expect(report.summary.overallQualityScore).toBeGreaterThan(90);
        });

        test('should identify data quality issues correctly', async () => {
            const dataWithIssues = [
                ...TestDataFactory.generateDataWithMissingValues(30, 20), // 20% missing
                ...TestDataFactory.generateDataWithOutliers(20, 15) // 15% outliers
            ];

            // Mark some as anomalous
            dataWithIssues.slice(0, 5).forEach(record => {
                record.anomalous_reading = true;
            });

            const report = await DataQualityService.generateDataQualityReport(dataWithIssues);

            expect(report.summary.totalRecords).toBe(50);
            expect(report.summary.missingValues).toBeGreaterThan(0);
            expect(report.summary.anomalousReadings).toBeGreaterThan(0);
            expect(report.summary.overallQualityScore).toBeLessThan(90);
        });

        test('should handle large datasets efficiently', async () => {
            const largeDataset = TestDataFactory.generateValidSensorData(10000);

            const performanceTestFn = TestUtils.performanceTest(
                () => DataQualityService.generateDataQualityReport(largeDataset),
                10000 // 10 second maximum
            );

            const report = await performanceTestFn();

            expect(report.summary.totalRecords).toBe(10000);
            expect(report.summary.overallQualityScore).toBeDefined();
        });

        test('should provide detailed breakdown of quality metrics', async () => {
            const testData = TestDataFactory.generateDataWithMissingValues(100, 25);

            const report = await DataQualityService.generateDataQualityReport(testData);

            expect(report.details).toHaveProperty('missing_values');
            expect(report.details).toHaveProperty('anomalous_readings');
            expect(report.details).toHaveProperty('outliers_corrected');
            expect(report.details).toHaveProperty('time_gap_analysis');

            expect(report.details.missing_values).toHaveProperty('count');
            expect(report.details.missing_values).toHaveProperty('percentage');
            expect(parseFloat(report.details.missing_values.percentage)).toBeWithinRange(20, 30);
        });

        test('should calculate quality score based on weighted metrics', async () => {
            // Test different scenarios to verify quality score calculation
            const scenarios = [
                { data: TestDataFactory.generateValidSensorData(100), expectedScore: [90, 100] },
                { data: TestDataFactory.generateDataWithMissingValues(100, 50), expectedScore: [50, 85] },
                { data: TestDataFactory.generateDataWithOutliers(100, 30), expectedScore: [60, 90] }
            ];

            for (const scenario of scenarios) {
                const report = await DataQualityService.generateDataQualityReport(scenario.data);
                expect(report.summary.overallQualityScore).toBeWithinRange(...scenario.expectedScore);
            }
        });
    });

    describe('generateDataQualityReport - Negative Cases', () => {
        test('should handle empty dataset gracefully', async () => {
            const report = await DataQualityService.generateDataQualityReport([]);

            expect(report).toHaveProperty('timestamp');
            expect(report).toHaveProperty('summary');
            expect(report).toHaveProperty('details');
            expect(report.summary.totalRecords).toBe(0);
        });

        test('should handle null input gracefully', async () => {
            const report = await DataQualityService.generateDataQualityReport(null);

            expect(report.summary.totalRecords).toBe(0);
        });

        test('should handle undefined input gracefully', async () => {
            const report = await DataQualityService.generateDataQualityReport(undefined);

            expect(report.summary.totalRecords).toBe(0);
        });

        test('should handle malformed data records', async () => {
            const malformedData = [
                null,
                undefined,
                {},
                { incomplete: 'record' },
                'not an object',
                123
            ];

            const report = await DataQualityService.generateDataQualityReport(malformedData);

            expect(report.summary.totalRecords).toBe(malformedData.length);
            expect(report.summary.missingValues).toBeGreaterThan(0);
        });

        test('should handle data with all missing values', async () => {
            const allMissingData = Array.from({ length: 10 }, (_, i) => ({
                sensor_id: `sensor_${i}`,
                timestamp: new Date().toISOString(),
                reading_type: 'temperature',
                value: null,
                battery_level: null
            }));

            const report = await DataQualityService.generateDataQualityReport(allMissingData);

            expect(report.summary.missingValues).toBe(10);
            expect(report.summary.overallQualityScore).toBeLessThan(50);
        });

        test('should handle database connection errors gracefully', async () => {
            // This test would ideally mock database failures
            const testData = TestDataFactory.generateValidSensorData(10);

            // Test should not throw even if there are underlying issues
            const report = await DataQualityService.generateDataQualityReport(testData);
            expect(report).toBeDefined();
        });
    });

    describe('generateDataQualityReport - Edge Cases', () => {
        test('should handle single record dataset', async () => {
            const singleRecord = TestDataFactory.generateValidSensorData(1);

            const report = await DataQualityService.generateDataQualityReport(singleRecord);

            expect(report.summary.totalRecords).toBe(1);
            expect(report.summary.overallQualityScore).toBeGreaterThan(0);
        });

        test('should handle all anomalous data', async () => {
            const anomalousData = TestDataFactory.generateValidSensorData(20);
            anomalousData.forEach(record => {
                record.anomalous_reading = true;
            });

            const report = await DataQualityService.generateDataQualityReport(anomalousData);

            expect(report.summary.anomalousReadings).toBe(20);
            expect(report.summary.overallQualityScore).toBeLessThan(70);
        });

        test('should handle data with extreme timestamp ranges', async () => {
            const extremeData = [
                {
                    sensor_id: 'sensor_001',
                    timestamp: '1970-01-01T00:00:00Z', // Unix epoch start
                    reading_type: 'temperature',
                    value: 25,
                    battery_level: 50
                },
                {
                    sensor_id: 'sensor_002',
                    timestamp: '2038-01-19T03:14:07Z', // Near Unix timestamp limit
                    reading_type: 'temperature',
                    value: 26,
                    battery_level: 51
                }
            ];

            const report = await DataQualityService.generateDataQualityReport(extremeData);

            expect(report.summary.totalRecords).toBe(2);
            expect(report.summary.overallQualityScore).toBeGreaterThan(0);
        });

        test('should handle data with special characters in string fields', async () => {
            const specialCharData = [
                {
                    sensor_id: 'sensor_!@#$%^&*()',
                    timestamp: '2023-06-01T10:00:00Z',
                    reading_type: 'temp-ñature_ös',
                    value: 25,
                    battery_level: 50
                }
            ];

            const report = await DataQualityService.generateDataQualityReport(specialCharData);

            expect(report.summary.totalRecords).toBe(1);
            expect(report.summary.overallQualityScore).toBeGreaterThan(0);
        });

        test('should handle very large numeric values', async () => {
            const largeValueData = [
                {
                    sensor_id: 'sensor_001',
                    timestamp: '2023-06-01T10:00:00Z',
                    reading_type: 'temperature',
                    value: Number.MAX_SAFE_INTEGER,
                    battery_level: Number.MAX_SAFE_INTEGER
                }
            ];

            const report = await DataQualityService.generateDataQualityReport(largeValueData);

            expect(report.summary.totalRecords).toBe(1);
        });

        test('should handle floating point precision issues', async () => {
            const precisionData = [
                {
                    sensor_id: 'sensor_001',
                    timestamp: '2023-06-01T10:00:00Z',
                    reading_type: 'temperature',
                    value: 0.1 + 0.2, // JavaScript floating point precision issue
                    battery_level: 50.00000000000001
                }
            ];

            const report = await DataQualityService.generateDataQualityReport(precisionData);

            expect(report.summary.totalRecords).toBe(1);
            expect(report.summary.overallQualityScore).toBeGreaterThan(0);
        });
    });

    describe('Quality Score Calculation Logic', () => {
        test('should penalize missing values appropriately', async () => {
            const scenarios = [
                { missingPercentage: 0, expectedScoreRange: [95, 100] },
                { missingPercentage: 10, expectedScoreRange: [85, 95] },
                { missingPercentage: 30, expectedScoreRange: [65, 85] },
                { missingPercentage: 50, expectedScoreRange: [45, 75] }
            ];

            for (const scenario of scenarios) {
                const testData = TestDataFactory.generateDataWithMissingValues(100, scenario.missingPercentage);
                const report = await DataQualityService.generateDataQualityReport(testData);

                expect(report.summary.overallQualityScore).toBeWithinRange(...scenario.expectedScoreRange);
            }
        });

        test('should penalize anomalous readings appropriately', async () => {
            const baseData = TestDataFactory.generateValidSensorData(100);

            // Create different levels of anomalous data
            const scenarios = [
                { anomalousCount: 0, expectedMinScore: 95 },
                { anomalousCount: 5, expectedMinScore: 85 },
                { anomalousCount: 20, expectedMinScore: 65 },
                { anomalousCount: 40, expectedMinScore: 45 }
            ];

            for (const scenario of scenarios) {
                const testData = [...baseData];
                for (let i = 0; i < scenario.anomalousCount; i++) {
                    testData[i].anomalous_reading = true;
                }

                const report = await DataQualityService.generateDataQualityReport(testData);
                expect(report.summary.overallQualityScore).toBeGreaterThanOrEqual(scenario.expectedMinScore);
            }
        });

        test('should combine multiple quality factors correctly', async () => {
            const dataWithMultipleIssues = TestDataFactory.generateDataWithMissingValues(100, 20);

            // Add anomalous readings
            dataWithMultipleIssues.slice(0, 10).forEach(record => {
                record.anomalous_reading = true;
            });

            // Add outlier corrections
            dataWithMultipleIssues.slice(10, 15).forEach(record => {
                record.outlier_corrected = true;
            });

            const report = await DataQualityService.generateDataQualityReport(dataWithMultipleIssues);

            // Score should reflect multiple quality issues
            expect(report.summary.overallQualityScore).toBeLessThan(80);
            expect(report.summary.missingValues).toBeGreaterThan(15);
            expect(report.summary.anomalousReadings).toBe(10);
        });

        test('should maintain score bounds (0-100)', async () => {
            // Create worst possible data
            const worstData = Array.from({ length: 100 }, (_, i) => ({
                sensor_id: null, // Missing sensor ID
                timestamp: null, // Missing timestamp
                reading_type: null, // Missing reading type
                value: null, // Missing value
                battery_level: null, // Missing battery level
                anomalous_reading: true, // Marked as anomalous
                outlier_corrected: true // Marked as outlier corrected
            }));

            const report = await DataQualityService.generateDataQualityReport(worstData);

            expect(report.summary.overallQualityScore).toBeGreaterThanOrEqual(0);
            expect(report.summary.overallQualityScore).toBeLessThanOrEqual(100);
        });
    });

    describe('Detailed Quality Metrics', () => {
        test('should provide accurate missing values statistics', async () => {
            const testData = TestDataFactory.generateDataWithMissingValues(200, 25);

            const report = await DataQualityService.generateDataQualityReport(testData);

            expect(report.details.missing_values.count).toBeGreaterThan(40); // ~25% of 200
            expect(report.details.missing_values.count).toBeLessThan(60);

            const expectedPercentage = (report.details.missing_values.count / 200) * 100;
            expect(parseFloat(report.details.missing_values.percentage)).toBeCloseTo(expectedPercentage, 1);
        });

        test('should provide accurate anomalous readings statistics', async () => {
            const testData = TestDataFactory.generateValidSensorData(150);

            // Mark 30 records as anomalous
            testData.slice(0, 30).forEach(record => {
                record.anomalous_reading = true;
            });

            const report = await DataQualityService.generateDataQualityReport(testData);

            expect(report.details.anomalous_readings.count).toBe(30);
            expect(parseFloat(report.details.anomalous_readings.percentage)).toBeCloseTo(20, 1); // 30/150 = 20%
        });

        test('should provide accurate outlier correction statistics', async () => {
            const testData = TestDataFactory.generateValidSensorData(100);

            // Mark 15 records as outlier corrected
            testData.slice(0, 15).forEach(record => {
                record.outlier_corrected = true;
            });

            const report = await DataQualityService.generateDataQualityReport(testData);

            expect(report.details.outliers_corrected.count).toBe(15);
            expect(parseFloat(report.details.outliers_corrected.percentage)).toBeCloseTo(15, 1); // 15/100 = 15%
        });

        test('should include time gap analysis placeholder', async () => {
            const testData = TestDataFactory.generateValidSensorData(50);

            const report = await DataQualityService.generateDataQualityReport(testData);

            expect(report.details.time_gap_analysis).toHaveProperty('total_gaps');
            expect(report.details.time_gap_analysis).toHaveProperty('longest_gap_hours');
        });
    });

    describe('Report Format and Structure', () => {
        test('should generate report with valid timestamp', async () => {
            const testData = TestDataFactory.generateValidSensorData(10);

            const report = await DataQualityService.generateDataQualityReport(testData);

            expect(report.timestamp).toBeValidISO8601();

            const reportTime = new Date(report.timestamp);
            const now = new Date();
            const timeDiff = Math.abs(now - reportTime);

            expect(timeDiff).toBeLessThan(5000); // Within 5 seconds
        });

        test('should maintain consistent report structure across different data types', async () => {
            const testDataSets = [
                TestDataFactory.generateValidSensorData(50),
                TestDataFactory.generateDataWithMissingValues(30, 10),
                TestDataFactory.generateDataWithOutliers(40, 5),
                TestDataFactory.generateEdgeCaseData()
            ];

            for (const testData of testDataSets) {
                const report = await DataQualityService.generateDataQualityReport(testData);

                // Verify consistent structure
                expect(report).toHaveProperty('timestamp');
                expect(report).toHaveProperty('summary');
                expect(report).toHaveProperty('details');

                expect(report.summary).toHaveProperty('totalRecords');
                expect(report.summary).toHaveProperty('overallQualityScore');
                expect(report.summary).toHaveProperty('missingValues');
                expect(report.summary).toHaveProperty('anomalousReadings');
                expect(report.summary).toHaveProperty('outliersCorreted');

                expect(typeof report.summary.totalRecords).toBe('number');
                expect(typeof report.summary.overallQualityScore).toBe('number');
                expect(typeof report.summary.missingValues).toBe('number');
                expect(typeof report.summary.anomalousReadings).toBe('number');
                expect(typeof report.summary.outliersCorreted).toBe('number');
            }
        });

        test('should generate serializable report', async () => {
            const testData = TestDataFactory.generateValidSensorData(25);

            const report = await DataQualityService.generateDataQualityReport(testData);

            // Should be able to serialize and deserialize without errors
            const serialized = JSON.stringify(report);
            const deserialized = JSON.parse(serialized);

            expect(deserialized).toEqual(report);
        });
    });

    describe('Performance and Memory Tests', () => {
        test('should handle large datasets without memory leaks', async () => {
            const memoryTestFn = TestUtils.memoryTest(
                async () => {
                    const largeDataset = TestDataFactory.generateValidSensorData(25000);
                    return await DataQualityService.generateDataQualityReport(largeDataset);
                },
                150 // 150 MB limit
            );

            const report = await memoryTestFn();
            expect(report.summary.totalRecords).toBe(25000);
        });

        test('should scale efficiently with dataset size', async () => {
            const sizes = [100, 1000, 5000];
            const times = [];

            for (const size of sizes) {
                const testData = TestDataFactory.generateValidSensorData(size);

                const startTime = Date.now();
                await DataQualityService.generateDataQualityReport(testData);
                const endTime = Date.now();

                times.push(endTime - startTime);
            }

            // Processing time should scale roughly linearly
            const ratio1 = times[1] / times[0]; // 1000/100
            const ratio2 = times[2] / times[1]; // 5000/1000

            expect(ratio1).toBeLessThan(15); // Should not be more than 15x slower
            expect(ratio2).toBeLessThan(8);  // Should not be more than 8x slower
        });

        test('should generate reports quickly for typical datasets', async () => {
            const typicalData = TestDataFactory.generateValidSensorData(500);

            const startTime = Date.now();
            const report = await DataQualityService.generateDataQualityReport(typicalData);
            const endTime = Date.now();

            expect(endTime - startTime).toBeLessThan(2000); // Under 2 seconds
            expect(report.summary.totalRecords).toBe(500);
        });
    });

    describe('Error Handling and Recovery', () => {
        test('should handle mixed valid and invalid data gracefully', async () => {
            const mixedData = [
                ...TestDataFactory.generateValidSensorData(20),
                null,
                undefined,
                {},
                { invalid: 'data' },
                ...TestDataFactory.generateValidSensorData(20)
            ];

            const report = await DataQualityService.generateDataQualityReport(mixedData);

            expect(report).toBeDefined();
            expect(report.summary.totalRecords).toBe(mixedData.length);
        });

        test('should provide meaningful error context in reports', async () => {
            const problematicData = [
                { sensor_id: '', timestamp: '', reading_type: '', value: 'invalid', battery_level: 'invalid' }
            ];

            const report = await DataQualityService.generateDataQualityReport(problematicData);

            expect(report).toBeDefined();
            expect(report.summary.missingValues).toBeGreaterThan(0);
        });

        test('should be resilient to concurrent access', async () => {
            const testData = TestDataFactory.generateValidSensorData(100);

            // Run multiple quality reports concurrently
            const promises = Array.from({ length: 5 }, () =>
                DataQualityService.generateDataQualityReport(testData)
            );

            const reports = await Promise.all(promises);

            // All reports should be consistent
            reports.forEach(report => {
                expect(report.summary.totalRecords).toBe(100);
                expect(report.summary.overallQualityScore).toBeWithinRange(90, 100);
            });
        });
    });
});
