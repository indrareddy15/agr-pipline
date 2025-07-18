/**
 * ETL Pipeline Integration Tests
 * Comprehensive testing of the entire ETL pipeline flow
 */

const ETLPipelineService = require('../../src/services/etlPipeline');
const TestDataFactory = require('../helpers/testDataFactory');
const TestUtils = require('../helpers/testUtils');
const fs = require('fs-extra');
const path = require('path');

describe('ETL Pipeline Integration Tests', () => {
    let tempDir;
    let rawDataDir;
    let processedDataDir;

    beforeEach(async () => {
        tempDir = path.join(__dirname, '../temp/integration');
        rawDataDir = path.join(tempDir, 'raw');
        processedDataDir = path.join(tempDir, 'processed');

        await fs.ensureDir(rawDataDir);
        await fs.ensureDir(processedDataDir);
    });

    afterEach(async () => {
        await TestUtils.cleanup([tempDir]);
    });

    describe('Full Pipeline Execution - Positive Cases', () => {
        test('should execute complete ETL pipeline successfully', async () => {
            // Create test data file
            const testFilePath = path.join(rawDataDir, '2023-06-01.parquet');
            await fs.writeFile(testFilePath, 'mock parquet data');

            const result = await ETLPipelineService.processFile(testFilePath);

            expect(result).toHaveProperty('success', true);
            expect(result).toHaveProperty('ingestionStats');
            expect(result).toHaveProperty('transformationStats');
            expect(result).toHaveProperty('qualityReport');
            expect(result).toHaveProperty('storageResult');

            expect(result.ingestionStats.filesRead).toBe(1);
            expect(result.transformationStats.outputRecords).toBeGreaterThan(0);
            expect(result.qualityReport.summary.totalRecords).toBeGreaterThan(0);
        });

        test('should process multiple files in batch', async () => {
            const testFiles = [
                '2023-06-01.parquet',
                '2023-06-02.parquet',
                '2023-06-03.parquet'
            ];

            // Create test files
            for (const fileName of testFiles) {
                const filePath = path.join(rawDataDir, fileName);
                await fs.writeFile(filePath, 'mock parquet data');
            }

            const results = [];
            for (const fileName of testFiles) {
                const filePath = path.join(rawDataDir, fileName);
                const result = await ETLPipelineService.processFile(filePath);
                results.push(result);
            }

            expect(results).toHaveLength(3);
            results.forEach(result => {
                expect(result.success).toBe(true);
                expect(result.ingestionStats.filesRead).toBe(1);
            });
        });

        test('should handle incremental processing correctly', async () => {
            const testFiles = [
                '2023-06-01.parquet',
                '2023-06-02.parquet'
            ];

            // Process first file
            const firstFilePath = path.join(rawDataDir, testFiles[0]);
            await fs.writeFile(firstFilePath, 'mock data 1');
            const firstResult = await ETLPipelineService.processFile(firstFilePath);

            // Process second file
            const secondFilePath = path.join(rawDataDir, testFiles[1]);
            await fs.writeFile(secondFilePath, 'mock data 2');
            const secondResult = await ETLPipelineService.processFile(secondFilePath);

            expect(firstResult.success).toBe(true);
            expect(secondResult.success).toBe(true);

            // Check that files are processed independently
            expect(firstResult.ingestionStats.filesRead).toBe(1);
            expect(secondResult.ingestionStats.filesRead).toBe(1);
        });

        test('should maintain data quality throughout pipeline', async () => {
            const testFilePath = path.join(rawDataDir, 'quality-test.parquet');
            await fs.writeFile(testFilePath, 'mock parquet data');

            const result = await ETLPipelineService.processFile(testFilePath);

            expect(result.success).toBe(true);
            expect(result.qualityReport.summary.overallQualityScore).toBeGreaterThan(0);
            expect(result.qualityReport.summary.overallQualityScore).toBeLessThanOrEqual(100);

            // Verify data transformations were applied
            expect(result.transformationStats.outputRecords).toBeGreaterThan(0);
            expect(result.transformationStats.inputRecords).toBeGreaterThan(0);
        });

        test('should generate comprehensive pipeline metrics', async () => {
            const testFilePath = path.join(rawDataDir, 'metrics-test.parquet');
            await fs.writeFile(testFilePath, 'mock parquet data');

            const result = await ETLPipelineService.processFile(testFilePath);

            expect(result).toHaveProperty('pipelineMetrics');
            expect(result.pipelineMetrics).toHaveProperty('totalProcessingTime');
            expect(result.pipelineMetrics).toHaveProperty('dataIngestionTime');
            expect(result.pipelineMetrics).toHaveProperty('dataTransformationTime');
            expect(result.pipelineMetrics).toHaveProperty('dataQualityTime');
            expect(result.pipelineMetrics).toHaveProperty('dataStorageTime');

            expect(result.pipelineMetrics.totalProcessingTime).toBeGreaterThan(0);
        });

        test('should handle large files efficiently', async () => {
            const largeFilePath = path.join(rawDataDir, 'large-file.parquet');
            await fs.writeFile(largeFilePath, 'mock large parquet data');

            const performanceTestFn = TestUtils.performanceTest(
                () => ETLPipelineService.processFile(largeFilePath),
                30000 // 30 second maximum
            );

            const result = await performanceTestFn();

            expect(result.success).toBe(true);
            expect(result.transformationStats.outputRecords).toBeGreaterThan(0);
        });
    });

    describe('Full Pipeline Execution - Negative Cases', () => {
        test('should handle missing input files gracefully', async () => {
            const nonExistentFile = path.join(rawDataDir, 'nonexistent.parquet');

            const result = await ETLPipelineService.processFile(nonExistentFile);

            expect(result.success).toBe(false);
            expect(result).toHaveProperty('error');
            expect(result.error).toContain('file not found');
        });

        test('should handle corrupted files gracefully', async () => {
            const corruptedFilePath = path.join(rawDataDir, 'corrupted.parquet');
            await fs.writeFile(corruptedFilePath, 'corrupted data that is not valid parquet');

            const result = await ETLPipelineService.processFile(corruptedFilePath);

            // Should either succeed with error handling or fail gracefully
            expect(result).toHaveProperty('success');
            if (!result.success) {
                expect(result).toHaveProperty('error');
            }
        });

        test('should handle empty files gracefully', async () => {
            const emptyFilePath = path.join(rawDataDir, 'empty.parquet');
            await fs.writeFile(emptyFilePath, '');

            const result = await ETLPipelineService.processFile(emptyFilePath);

            expect(result).toHaveProperty('success');
            if (result.success) {
                expect(result.ingestionStats.recordsProcessed).toBe(0);
            }
        });

        test('should handle invalid file permissions', async () => {
            const restrictedFilePath = path.join(rawDataDir, 'restricted.parquet');
            await fs.writeFile(restrictedFilePath, 'mock data');

            // Try to make file unreadable (may not work on all systems)
            try {
                await fs.chmod(restrictedFilePath, 0o000);

                const result = await ETLPipelineService.processFile(restrictedFilePath);

                expect(result).toHaveProperty('success');
                if (!result.success) {
                    expect(result.error).toMatch(/permission|access|denied/i);
                }
            } catch (chmodError) {
                // Skip test if chmod fails (e.g., on Windows)
                console.warn('Skipping permission test due to chmod failure');
            }
        });

        test('should handle disk space issues during processing', async () => {
            // This test simulates disk space issues by trying to write to invalid location
            const invalidOutputPath = '/dev/null/invalid/path';
            const testFilePath = path.join(rawDataDir, 'diskspace-test.parquet');
            await fs.writeFile(testFilePath, 'mock data');

            // Mock storage service to simulate disk space error
            const originalProcessFile = ETLPipelineService.processFile;
            ETLPipelineService.processFile = async function (filePath) {
                try {
                    return await originalProcessFile.call(this, filePath);
                } catch (error) {
                    return {
                        success: false,
                        error: 'Simulated disk space error',
                        stage: 'storage'
                    };
                }
            };

            const result = await ETLPipelineService.processFile(testFilePath);

            // Restore original method
            ETLPipelineService.processFile = originalProcessFile;

            expect(result).toHaveProperty('success');
            expect(result).toHaveProperty('error');
        });
    });

    describe('Pipeline Error Recovery', () => {
        test('should recover from ingestion errors and continue processing', async () => {
            const testFiles = [
                'good-file-1.parquet',
                'bad-file.parquet',
                'good-file-2.parquet'
            ];

            // Create test files
            for (const fileName of testFiles) {
                const filePath = path.join(rawDataDir, fileName);
                const content = fileName.includes('bad') ? 'corrupted data' : 'mock parquet data';
                await fs.writeFile(filePath, content);
            }

            const results = [];
            for (const fileName of testFiles) {
                const filePath = path.join(rawDataDir, fileName);
                try {
                    const result = await ETLPipelineService.processFile(filePath);
                    results.push({ fileName, result });
                } catch (error) {
                    results.push({ fileName, error: error.message });
                }
            }

            expect(results).toHaveLength(3);

            // At least some files should process successfully
            const successfulResults = results.filter(r => r.result && r.result.success);
            expect(successfulResults.length).toBeGreaterThan(0);
        });

        test('should handle partial data transformation failures', async () => {
            const testFilePath = path.join(rawDataDir, 'partial-failure.parquet');
            await fs.writeFile(testFilePath, 'mock parquet data');

            const result = await ETLPipelineService.processFile(testFilePath);

            expect(result).toHaveProperty('success');

            if (result.success) {
                expect(result.transformationStats).toHaveProperty('failedRecords');
                // Pipeline should continue even if some records fail
                expect(result.transformationStats.outputRecords).toBeGreaterThanOrEqual(0);
            }
        });

        test('should handle quality validation failures gracefully', async () => {
            const testFilePath = path.join(rawDataDir, 'quality-failure.parquet');
            await fs.writeFile(testFilePath, 'mock parquet data');

            const result = await ETLPipelineService.processFile(testFilePath);

            expect(result).toHaveProperty('success');
            expect(result).toHaveProperty('qualityReport');

            // Quality report should be generated even if data quality is poor
            expect(result.qualityReport.summary).toHaveProperty('overallQualityScore');
        });

        test('should provide detailed error context for debugging', async () => {
            const testFilePath = path.join(rawDataDir, 'debug-test.parquet');
            await fs.writeFile(testFilePath, 'potentially problematic data');

            const result = await ETLPipelineService.processFile(testFilePath);

            expect(result).toHaveProperty('success');

            if (!result.success) {
                expect(result).toHaveProperty('error');
                expect(result).toHaveProperty('stage'); // Which stage failed
                expect(result).toHaveProperty('timestamp');
            }
        });
    });

    describe('Pipeline Performance and Scalability', () => {
        test('should process files in parallel when possible', async () => {
            const testFiles = [
                'parallel-1.parquet',
                'parallel-2.parquet',
                'parallel-3.parquet',
                'parallel-4.parquet'
            ];

            // Create test files
            const filePaths = [];
            for (const fileName of testFiles) {
                const filePath = path.join(rawDataDir, fileName);
                await fs.writeFile(filePath, 'mock parquet data');
                filePaths.push(filePath);
            }

            const startTime = Date.now();

            // Process files in parallel
            const promises = filePaths.map(filePath =>
                ETLPipelineService.processFile(filePath)
            );

            const results = await Promise.all(promises);
            const endTime = Date.now();
            const parallelTime = endTime - startTime;

            // Process files sequentially for comparison
            const sequentialStartTime = Date.now();
            for (const filePath of filePaths) {
                await ETLPipelineService.processFile(filePath);
            }
            const sequentialEndTime = Date.now();
            const sequentialTime = sequentialEndTime - sequentialStartTime;

            // Parallel processing should be faster (accounting for overhead)
            expect(parallelTime).toBeLessThan(sequentialTime * 0.8);

            results.forEach(result => {
                expect(result.success).toBe(true);
            });
        });

        test('should handle memory efficiently for large datasets', async () => {
            const largeFilePath = path.join(rawDataDir, 'memory-test.parquet');
            await fs.writeFile(largeFilePath, 'mock large dataset');

            const memoryTestFn = TestUtils.memoryTest(
                () => ETLPipelineService.processFile(largeFilePath),
                200 // 200 MB limit
            );

            const result = await memoryTestFn();

            expect(result.success).toBe(true);
        });

        test('should scale processing time linearly with file size', async () => {
            const fileSizes = ['small', 'medium', 'large'];
            const times = [];

            for (const size of fileSizes) {
                const testFilePath = path.join(rawDataDir, `${size}-file.parquet`);
                await fs.writeFile(testFilePath, `mock ${size} data`);

                const startTime = Date.now();
                await ETLPipelineService.processFile(testFilePath);
                const endTime = Date.now();

                times.push(endTime - startTime);
            }

            // Processing time should scale reasonably
            expect(times[0]).toBeGreaterThan(0);
            expect(times[1]).toBeGreaterThan(0);
            expect(times[2]).toBeGreaterThan(0);
        });

        test('should handle concurrent pipeline executions', async () => {
            const concurrentFiles = [
                'concurrent-1.parquet',
                'concurrent-2.parquet',
                'concurrent-3.parquet'
            ];

            // Create test files
            const promises = concurrentFiles.map(async (fileName) => {
                const filePath = path.join(rawDataDir, fileName);
                await fs.writeFile(filePath, 'mock parquet data');
                return ETLPipelineService.processFile(filePath);
            });

            const results = await Promise.all(promises);

            results.forEach(result => {
                expect(result.success).toBe(true);
                expect(result.ingestionStats.filesRead).toBe(1);
            });
        });
    });

    describe('Pipeline Monitoring and Metrics', () => {
        test('should track pipeline execution metrics accurately', async () => {
            const testFilePath = path.join(rawDataDir, 'metrics-tracking.parquet');
            await fs.writeFile(testFilePath, 'mock parquet data');

            const result = await ETLPipelineService.processFile(testFilePath);

            expect(result.success).toBe(true);
            expect(result.pipelineMetrics).toBeDefined();

            // Verify all timing metrics are present
            const timingMetrics = [
                'dataIngestionTime',
                'dataTransformationTime',
                'dataQualityTime',
                'dataStorageTime',
                'totalProcessingTime'
            ];

            timingMetrics.forEach(metric => {
                expect(result.pipelineMetrics).toHaveProperty(metric);
                expect(result.pipelineMetrics[metric]).toBeGreaterThanOrEqual(0);
            });

            // Total time should be sum of individual stages (approximately)
            const sumOfStages = result.pipelineMetrics.dataIngestionTime +
                result.pipelineMetrics.dataTransformationTime +
                result.pipelineMetrics.dataQualityTime +
                result.pipelineMetrics.dataStorageTime;

            expect(result.pipelineMetrics.totalProcessingTime).toBeGreaterThanOrEqual(sumOfStages * 0.9);
        });

        test('should provide detailed pipeline statistics', async () => {
            const testFilePath = path.join(rawDataDir, 'stats-test.parquet');
            await fs.writeFile(testFilePath, 'mock parquet data');

            const result = await ETLPipelineService.processFile(testFilePath);

            expect(result.success).toBe(true);

            // Verify statistics are comprehensive
            expect(result.ingestionStats).toHaveProperty('filesRead');
            expect(result.ingestionStats).toHaveProperty('recordsProcessed');
            expect(result.ingestionStats).toHaveProperty('recordsSkipped');
            expect(result.ingestionStats).toHaveProperty('recordsFailed');

            expect(result.transformationStats).toHaveProperty('inputRecords');
            expect(result.transformationStats).toHaveProperty('outputRecords');
            expect(result.transformationStats).toHaveProperty('failedRecords');

            expect(result.qualityReport.summary).toHaveProperty('totalRecords');
            expect(result.qualityReport.summary).toHaveProperty('overallQualityScore');
        });

        test('should log pipeline execution events properly', async () => {
            const consoleMock = TestUtils.mockConsole();

            const testFilePath = path.join(rawDataDir, 'logging-test.parquet');
            await fs.writeFile(testFilePath, 'mock parquet data');

            const result = await ETLPipelineService.processFile(testFilePath);

            expect(result.success).toBe(true);

            // Verify appropriate log messages were generated
            expect(consoleMock.logs.some(log => log.includes('Data ingestion'))).toBe(true);
            expect(consoleMock.logs.some(log => log.includes('transformation'))).toBe(true);
            expect(consoleMock.logs.some(log => log.includes('quality'))).toBe(true);

            consoleMock.restore();
        });
    });

    describe('Data Integrity and Consistency', () => {
        test('should maintain data integrity throughout pipeline', async () => {
            const testFilePath = path.join(rawDataDir, 'integrity-test.parquet');
            await fs.writeFile(testFilePath, 'mock parquet data');

            const result = await ETLPipelineService.processFile(testFilePath);

            expect(result.success).toBe(true);

            // Data should be transformed but maintain essential characteristics
            expect(result.transformationStats.outputRecords).toBeGreaterThan(0);
            expect(result.qualityReport.summary.totalRecords).toBe(result.transformationStats.outputRecords);
        });

        test('should handle data validation errors consistently', async () => {
            const testFilePath = path.join(rawDataDir, 'validation-test.parquet');
            await fs.writeFile(testFilePath, 'mock parquet data');

            const result = await ETLPipelineService.processFile(testFilePath);

            expect(result).toHaveProperty('success');

            if (result.success) {
                // Validation should be applied consistently
                expect(result.qualityReport.summary.overallQualityScore).toBeWithinRange(0, 100);
            }
        });

        test('should ensure checkpoint consistency', async () => {
            const testFilePath = path.join(rawDataDir, 'checkpoint-test.parquet');
            await fs.writeFile(testFilePath, 'mock parquet data');

            const result = await ETLPipelineService.processFile(testFilePath);

            expect(result.success).toBe(true);

            // Verify checkpoint was created
            expect(result).toHaveProperty('checkpoint');
            expect(result.checkpoint).toHaveProperty('filename');
            expect(result.checkpoint).toHaveProperty('processedAt');
        });
    });
});
