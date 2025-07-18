/**
 * API Controllers Unit Tests
 * Comprehensive testing of all API endpoints and controller logic
 */

const request = require('supertest');
const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs-extra');

// Import controllers
const dataController = require('../../src/controllers/dataController');
const uploadController = require('../../src/controllers/uploadController');
const pipelineController = require('../../src/controllers/pipelineController');
const statusController = require('../../src/controllers/statusController');

const TestDataFactory = require('../helpers/testDataFactory');
const TestUtils = require('../helpers/testUtils');

describe('API Controllers', () => {
    let app;
    let tempDir;

    beforeEach(async () => {
        // Create Express app for testing
        app = express();
        app.use(express.json());
        app.use(express.urlencoded({ extended: true }));

        // Setup multer for file uploads
        tempDir = path.join(__dirname, '../temp/uploads');
        await fs.ensureDir(tempDir);

        const upload = multer({ dest: tempDir });

        // Setup routes
        app.get('/api/data', dataController.getData);
        app.get('/api/data/processed', dataController.getProcessedData);
        app.get('/api/data/quality-report', dataController.getQualityReport);
        app.get('/api/data/summary', dataController.getSummary);

        app.post('/api/upload', upload.single('file'), uploadController.uploadFile);
        app.get('/api/upload/history', uploadController.getUploadHistory);

        app.post('/api/pipeline/run', pipelineController.runPipeline);
        app.get('/api/pipeline/status', pipelineController.getPipelineStatus);
        app.get('/api/pipeline/metrics', pipelineController.getPipelineMetrics);

        app.get('/api/status', statusController.getSystemStatus);
        app.get('/api/status/health', statusController.getHealthCheck);
    });

    afterEach(async () => {
        await TestUtils.cleanup([tempDir]);
    });

    describe('Data Controller', () => {
        describe('GET /api/data - Positive Cases', () => {
            test('should return sensor data successfully', async () => {
                const response = await request(app)
                    .get('/api/data')
                    .expect(200);

                expect(response.body).toHaveProperty('success', true);
                expect(response.body).toHaveProperty('data');
                expect(Array.isArray(response.body.data)).toBe(true);
            });

            test('should handle pagination parameters', async () => {
                const response = await request(app)
                    .get('/api/data?page=1&limit=10')
                    .expect(200);

                expect(response.body).toHaveProperty('success', true);
                expect(response.body).toHaveProperty('data');
                expect(response.body).toHaveProperty('pagination');
                expect(response.body.pagination).toHaveProperty('page');
                expect(response.body.pagination).toHaveProperty('limit');
            });

            test('should handle filtering by sensor_id', async () => {
                const response = await request(app)
                    .get('/api/data?sensor_id=sensor_001')
                    .expect(200);

                expect(response.body).toHaveProperty('success', true);
                expect(response.body).toHaveProperty('data');

                if (response.body.data.length > 0) {
                    response.body.data.forEach(record => {
                        expect(record.sensor_id).toBe('sensor_001');
                    });
                }
            });

            test('should handle filtering by reading_type', async () => {
                const response = await request(app)
                    .get('/api/data?reading_type=temperature')
                    .expect(200);

                expect(response.body).toHaveProperty('success', true);
                expect(response.body).toHaveProperty('data');

                if (response.body.data.length > 0) {
                    response.body.data.forEach(record => {
                        expect(record.reading_type).toBe('temperature');
                    });
                }
            });

            test('should handle date range filtering', async () => {
                const startDate = '2023-06-01';
                const endDate = '2023-06-02';

                const response = await request(app)
                    .get(`/api/data?start_date=${startDate}&end_date=${endDate}`)
                    .expect(200);

                expect(response.body).toHaveProperty('success', true);
                expect(response.body).toHaveProperty('data');
            });

            test('should handle sorting parameters', async () => {
                const response = await request(app)
                    .get('/api/data?sort=timestamp&order=desc')
                    .expect(200);

                expect(response.body).toHaveProperty('success', true);
                expect(response.body).toHaveProperty('data');

                if (response.body.data.length > 1) {
                    // Verify descending order
                    for (let i = 1; i < response.body.data.length; i++) {
                        const prev = new Date(response.body.data[i - 1].timestamp);
                        const curr = new Date(response.body.data[i].timestamp);
                        expect(prev.getTime()).toBeGreaterThanOrEqual(curr.getTime());
                    }
                }
            });
        });

        describe('GET /api/data - Negative Cases', () => {
            test('should handle invalid pagination parameters', async () => {
                const response = await request(app)
                    .get('/api/data?page=-1&limit=0')
                    .expect(400);

                expect(response.body).toHaveProperty('success', false);
                expect(response.body).toHaveProperty('error');
            });

            test('should handle invalid date formats', async () => {
                const response = await request(app)
                    .get('/api/data?start_date=invalid-date&end_date=2023-06-02')
                    .expect(400);

                expect(response.body).toHaveProperty('success', false);
                expect(response.body).toHaveProperty('error');
            });

            test('should handle invalid sort parameters', async () => {
                const response = await request(app)
                    .get('/api/data?sort=invalid_column&order=invalid_order')
                    .expect(400);

                expect(response.body).toHaveProperty('success', false);
                expect(response.body).toHaveProperty('error');
            });

            test('should handle very large limit values', async () => {
                const response = await request(app)
                    .get('/api/data?limit=999999')
                    .expect(400);

                expect(response.body).toHaveProperty('success', false);
                expect(response.body).toHaveProperty('error');
            });
        });

        describe('GET /api/data/processed - Processing Data', () => {
            test('should return processed data successfully', async () => {
                const response = await request(app)
                    .get('/api/data/processed')
                    .expect(200);

                expect(response.body).toHaveProperty('success', true);
                expect(response.body).toHaveProperty('data');
                expect(Array.isArray(response.body.data)).toBe(true);
            });

            test('should include transformation metadata', async () => {
                const response = await request(app)
                    .get('/api/data/processed')
                    .expect(200);

                expect(response.body).toHaveProperty('success', true);

                if (response.body.data.length > 0) {
                    const record = response.body.data[0];
                    expect(record).toHaveProperty('processed_timestamp');
                    expect(record).toHaveProperty('outlier_corrected');
                    expect(record).toHaveProperty('missing_value_filled');
                }
            });
        });

        describe('GET /api/data/quality-report - Quality Reports', () => {
            test('should return quality report successfully', async () => {
                const response = await request(app)
                    .get('/api/data/quality-report')
                    .expect(200);

                expect(response.body).toHaveProperty('success', true);
                expect(response.body).toHaveProperty('report');
                expect(response.body.report).toHaveProperty('summary');
                expect(response.body.report).toHaveProperty('details');
            });

            test('should include quality metrics', async () => {
                const response = await request(app)
                    .get('/api/data/quality-report')
                    .expect(200);

                expect(response.body.report.summary).toHaveProperty('totalRecords');
                expect(response.body.report.summary).toHaveProperty('overallQualityScore');
                expect(response.body.report.summary).toHaveProperty('missingValues');
                expect(response.body.report.summary).toHaveProperty('anomalousReadings');
            });

            test('should handle date range for quality reports', async () => {
                const response = await request(app)
                    .get('/api/data/quality-report?start_date=2023-06-01&end_date=2023-06-02')
                    .expect(200);

                expect(response.body).toHaveProperty('success', true);
                expect(response.body).toHaveProperty('report');
            });
        });

        describe('GET /api/data/summary - Data Summary', () => {
            test('should return data summary successfully', async () => {
                const response = await request(app)
                    .get('/api/data/summary')
                    .expect(200);

                expect(response.body).toHaveProperty('success', true);
                expect(response.body).toHaveProperty('summary');
            });

            test('should include aggregated statistics', async () => {
                const response = await request(app)
                    .get('/api/data/summary')
                    .expect(200);

                expect(response.body.summary).toHaveProperty('totalRecords');
                expect(response.body.summary).toHaveProperty('dateRange');
                expect(response.body.summary).toHaveProperty('sensorCount');
                expect(response.body.summary).toHaveProperty('readingTypes');
            });
        });
    });

    describe('Upload Controller', () => {
        describe('POST /api/upload - File Upload', () => {
            test('should upload valid parquet file successfully', async () => {
                const mockParquetData = Buffer.from('mock parquet file content');

                const response = await request(app)
                    .post('/api/upload')
                    .attach('file', mockParquetData, 'test-data.parquet')
                    .expect(200);

                expect(response.body).toHaveProperty('success', true);
                expect(response.body).toHaveProperty('message');
                expect(response.body).toHaveProperty('fileInfo');
                expect(response.body.fileInfo).toHaveProperty('filename', 'test-data.parquet');
            });

            test('should validate file extension', async () => {
                const mockData = Buffer.from('invalid file content');

                const response = await request(app)
                    .post('/api/upload')
                    .attach('file', mockData, 'test-data.txt')
                    .expect(400);

                expect(response.body).toHaveProperty('success', false);
                expect(response.body).toHaveProperty('error');
                expect(response.body.error).toMatch(/file type|extension/i);
            });

            test('should handle large file uploads', async () => {
                const largeData = Buffer.alloc(10 * 1024 * 1024); // 10MB
                largeData.fill('a');

                const response = await request(app)
                    .post('/api/upload')
                    .attach('file', largeData, 'large-file.parquet')
                    .expect(413); // Payload too large or success depending on config

                expect(response.body).toHaveProperty('success');
            });

            test('should handle missing file', async () => {
                const response = await request(app)
                    .post('/api/upload')
                    .expect(400);

                expect(response.body).toHaveProperty('success', false);
                expect(response.body).toHaveProperty('error');
            });

            test('should handle empty file', async () => {
                const emptyData = Buffer.alloc(0);

                const response = await request(app)
                    .post('/api/upload')
                    .attach('file', emptyData, 'empty.parquet')
                    .expect(400);

                expect(response.body).toHaveProperty('success', false);
                expect(response.body).toHaveProperty('error');
            });

            test('should sanitize filename', async () => {
                const mockData = Buffer.from('mock parquet data');
                const maliciousFilename = '../../../malicious.parquet';

                const response = await request(app)
                    .post('/api/upload')
                    .attach('file', mockData, maliciousFilename)
                    .expect(200);

                expect(response.body).toHaveProperty('success', true);
                expect(response.body.fileInfo.filename).not.toContain('../');
            });
        });

        describe('GET /api/upload/history - Upload History', () => {
            test('should return upload history successfully', async () => {
                const response = await request(app)
                    .get('/api/upload/history')
                    .expect(200);

                expect(response.body).toHaveProperty('success', true);
                expect(response.body).toHaveProperty('uploads');
                expect(Array.isArray(response.body.uploads)).toBe(true);
            });

            test('should include upload metadata', async () => {
                const response = await request(app)
                    .get('/api/upload/history')
                    .expect(200);

                if (response.body.uploads.length > 0) {
                    const upload = response.body.uploads[0];
                    expect(upload).toHaveProperty('filename');
                    expect(upload).toHaveProperty('uploadTime');
                    expect(upload).toHaveProperty('fileSize');
                    expect(upload).toHaveProperty('status');
                }
            });

            test('should handle pagination for upload history', async () => {
                const response = await request(app)
                    .get('/api/upload/history?page=1&limit=5')
                    .expect(200);

                expect(response.body).toHaveProperty('success', true);
                expect(response.body).toHaveProperty('pagination');
            });
        });
    });

    describe('Pipeline Controller', () => {
        describe('POST /api/pipeline/run - Pipeline Execution', () => {
            test('should start pipeline execution successfully', async () => {
                const response = await request(app)
                    .post('/api/pipeline/run')
                    .send({ filename: 'test-data.parquet' })
                    .expect(200);

                expect(response.body).toHaveProperty('success', true);
                expect(response.body).toHaveProperty('message');
                expect(response.body).toHaveProperty('pipelineId');
            });

            test('should validate pipeline parameters', async () => {
                const response = await request(app)
                    .post('/api/pipeline/run')
                    .send({ filename: '' })
                    .expect(400);

                expect(response.body).toHaveProperty('success', false);
                expect(response.body).toHaveProperty('error');
            });

            test('should handle concurrent pipeline requests', async () => {
                const requests = Array.from({ length: 3 }, (_, i) =>
                    request(app)
                        .post('/api/pipeline/run')
                        .send({ filename: `test-data-${i}.parquet` })
                );

                const responses = await Promise.all(requests);

                responses.forEach(response => {
                    expect(response.status).toBeOneOf([200, 409]); // Success or conflict
                    expect(response.body).toHaveProperty('success');
                });
            });

            test('should handle invalid file for pipeline', async () => {
                const response = await request(app)
                    .post('/api/pipeline/run')
                    .send({ filename: 'nonexistent.parquet' })
                    .expect(404);

                expect(response.body).toHaveProperty('success', false);
                expect(response.body).toHaveProperty('error');
            });
        });

        describe('GET /api/pipeline/status - Pipeline Status', () => {
            test('should return pipeline status successfully', async () => {
                const response = await request(app)
                    .get('/api/pipeline/status')
                    .expect(200);

                expect(response.body).toHaveProperty('success', true);
                expect(response.body).toHaveProperty('status');
                expect(response.body.status).toHaveProperty('currentStage');
                expect(response.body.status).toHaveProperty('progress');
            });

            test('should include execution details', async () => {
                const response = await request(app)
                    .get('/api/pipeline/status')
                    .expect(200);

                expect(response.body.status).toHaveProperty('startTime');
                expect(response.body.status).toHaveProperty('isRunning');
                expect(response.body.status).toHaveProperty('lastUpdate');
            });

            test('should handle pipeline status for specific execution', async () => {
                const response = await request(app)
                    .get('/api/pipeline/status?pipelineId=test-pipeline-123')
                    .expect(200);

                expect(response.body).toHaveProperty('success', true);
                expect(response.body).toHaveProperty('status');
            });
        });

        describe('GET /api/pipeline/metrics - Pipeline Metrics', () => {
            test('should return pipeline metrics successfully', async () => {
                const response = await request(app)
                    .get('/api/pipeline/metrics')
                    .expect(200);

                expect(response.body).toHaveProperty('success', true);
                expect(response.body).toHaveProperty('metrics');
            });

            test('should include performance metrics', async () => {
                const response = await request(app)
                    .get('/api/pipeline/metrics')
                    .expect(200);

                const metrics = response.body.metrics;
                expect(metrics).toHaveProperty('totalExecutions');
                expect(metrics).toHaveProperty('averageExecutionTime');
                expect(metrics).toHaveProperty('successRate');
                expect(metrics).toHaveProperty('recordsProcessed');
            });

            test('should handle date range for metrics', async () => {
                const response = await request(app)
                    .get('/api/pipeline/metrics?start_date=2023-06-01&end_date=2023-06-02')
                    .expect(200);

                expect(response.body).toHaveProperty('success', true);
                expect(response.body).toHaveProperty('metrics');
            });
        });
    });

    describe('Status Controller', () => {
        describe('GET /api/status - System Status', () => {
            test('should return system status successfully', async () => {
                const response = await request(app)
                    .get('/api/status')
                    .expect(200);

                expect(response.body).toHaveProperty('success', true);
                expect(response.body).toHaveProperty('status');
                expect(response.body.status).toHaveProperty('server');
                expect(response.body.status).toHaveProperty('database');
                expect(response.body.status).toHaveProperty('services');
            });

            test('should include system metrics', async () => {
                const response = await request(app)
                    .get('/api/status')
                    .expect(200);

                const status = response.body.status;
                expect(status).toHaveProperty('uptime');
                expect(status).toHaveProperty('memory');
                expect(status).toHaveProperty('version');
                expect(status).toHaveProperty('environment');
            });

            test('should indicate service health', async () => {
                const response = await request(app)
                    .get('/api/status')
                    .expect(200);

                const services = response.body.status.services;
                Object.values(services).forEach(service => {
                    expect(service).toHaveProperty('status');
                    expect(['healthy', 'unhealthy', 'unknown']).toContain(service.status);
                });
            });
        });

        describe('GET /api/status/health - Health Check', () => {
            test('should return health check successfully', async () => {
                const response = await request(app)
                    .get('/api/status/health')
                    .expect(200);

                expect(response.body).toHaveProperty('status', 'ok');
                expect(response.body).toHaveProperty('timestamp');
                expect(response.body).toHaveProperty('checks');
            });

            test('should include component health checks', async () => {
                const response = await request(app)
                    .get('/api/status/health')
                    .expect(200);

                const checks = response.body.checks;
                expect(checks).toHaveProperty('database');
                expect(checks).toHaveProperty('fileSystem');
                expect(checks).toHaveProperty('services');

                Object.values(checks).forEach(check => {
                    expect(check).toHaveProperty('status');
                    expect(check).toHaveProperty('responseTime');
                });
            });

            test('should respond quickly to health checks', async () => {
                const startTime = Date.now();

                const response = await request(app)
                    .get('/api/status/health')
                    .expect(200);

                const responseTime = Date.now() - startTime;
                expect(responseTime).toBeLessThan(1000); // Should respond within 1 second
                expect(response.body.status).toBe('ok');
            });
        });
    });

    describe('Error Handling and Edge Cases', () => {
        test('should handle malformed JSON requests', async () => {
            const response = await request(app)
                .post('/api/pipeline/run')
                .set('Content-Type', 'application/json')
                .send('invalid json{')
                .expect(400);

            expect(response.body).toHaveProperty('success', false);
            expect(response.body).toHaveProperty('error');
        });

        test('should handle extremely large request payloads', async () => {
            const largePayload = { data: 'x'.repeat(10 * 1024 * 1024) }; // 10MB string

            const response = await request(app)
                .post('/api/pipeline/run')
                .send(largePayload)
                .expect(413); // Payload too large

            expect(response.body).toHaveProperty('success', false);
        });

        test('should handle SQL injection attempts', async () => {
            const maliciousInput = "'; DROP TABLE sensor_data; --";

            const response = await request(app)
                .get(`/api/data?sensor_id=${encodeURIComponent(maliciousInput)}`)
                .expect(400);

            expect(response.body).toHaveProperty('success', false);
        });

        test('should handle XSS attempts in parameters', async () => {
            const xssPayload = '<script>alert("xss")</script>';

            const response = await request(app)
                .get(`/api/data?sensor_id=${encodeURIComponent(xssPayload)}`)
                .expect(400);

            expect(response.body).toHaveProperty('success', false);
            expect(response.body.error).not.toContain('<script>');
        });

        test('should validate request headers', async () => {
            const response = await request(app)
                .get('/api/data')
                .set('X-Malicious-Header', 'evil-value')
                .expect(200); // Should still work but ignore malicious headers

            expect(response.body).toHaveProperty('success', true);
        });

        test('should handle concurrent requests gracefully', async () => {
            const requests = Array.from({ length: 10 }, () =>
                request(app).get('/api/status/health')
            );

            const responses = await Promise.all(requests);

            responses.forEach(response => {
                expect(response.status).toBe(200);
                expect(response.body.status).toBe('ok');
            });
        });

        test('should handle timeout scenarios', async () => {
            // This test would require mocking long-running operations
            const response = await request(app)
                .get('/api/data')
                .timeout(5000) // 5 second timeout
                .expect(200);

            expect(response.body).toHaveProperty('success', true);
        });
    });

    describe('API Security and Validation', () => {
        test('should validate required parameters', async () => {
            const response = await request(app)
                .post('/api/pipeline/run')
                .send({}) // Missing required filename
                .expect(400);

            expect(response.body).toHaveProperty('success', false);
            expect(response.body).toHaveProperty('error');
        });

        test('should sanitize user inputs', async () => {
            const response = await request(app)
                .get('/api/data?sensor_id=normal_sensor_001')
                .expect(200);

            expect(response.body).toHaveProperty('success', true);
        });

        test('should handle rate limiting gracefully', async () => {
            // Simulate rapid requests
            const rapidRequests = Array.from({ length: 50 }, () =>
                request(app).get('/api/status/health')
            );

            const responses = await Promise.all(rapidRequests);

            // Most requests should succeed, but some might be rate limited
            const successfulRequests = responses.filter(r => r.status === 200);
            expect(successfulRequests.length).toBeGreaterThan(0);
        });

        test('should validate file upload security', async () => {
            const maliciousFilename = '../../etc/passwd';
            const mockData = Buffer.from('malicious content');

            const response = await request(app)
                .post('/api/upload')
                .attach('file', mockData, maliciousFilename)
                .expect(400);

            expect(response.body).toHaveProperty('success', false);
        });
    });
});
