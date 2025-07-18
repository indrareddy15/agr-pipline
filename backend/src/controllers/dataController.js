/**
 * Data Controller
 * Handles data retrieval and management operations
 */

const ETLPipelineService = require('../services/etlPipeline');
const { logging } = require('../utils');

// Initialize ETL Pipeline Service
const etlPipeline = new ETLPipelineService();

class DataController {
    /**
     * Get sensor data with optional filtering
     * GET /api/data
     */
    static async getData(req, res) {
        try {
            const {
                limit = 100,
                offset = 0,
                date_from,
                date_to,
                sensor_id,
                reading_type
            } = req.query;

            const result = await etlPipeline.getFilteredData({
                limit: parseInt(limit),
                offset: parseInt(offset),
                date_from,
                date_to,
                sensor_id,
                reading_type
            });

            res.json({
                status: 'success',
                data: {
                    records: result.data,
                    count: result.data.length,
                    pagination: result.pagination,
                    timestamp: new Date().toISOString()
                }
            });
        } catch (error) {
            logging.error(`Data retrieval error: ${error.message}`);
            res.status(500).json({
                status: 'error',
                message: 'Failed to retrieve data',
                error: error.message
            });
        }
    }

    /**
     * Get recent sensor data
     * GET /api/data/recent
     */
    static async getRecentData(req, res) {
        try {
            const { hours = 24, limit = 100 } = req.query;
            const data = await etlPipeline.getRecentData(parseInt(hours), parseInt(limit));

            res.json({
                status: 'success',
                data: {
                    records: data,
                    count: data.length,
                    hours: parseInt(hours),
                    timestamp: new Date().toISOString()
                }
            });
        } catch (error) {
            logging.error(`Recent data retrieval error: ${error.message}`);
            res.status(500).json({
                status: 'error',
                message: 'Failed to retrieve recent data',
                error: error.message
            });
        }
    }

    /**
     * Get summary statistics for data
     * GET /api/data/summary
     */
    static async getDataSummary(req, res) {
        try {
            const { date_start, date_end } = req.query;
            const summary = await etlPipeline.getDataSummary(date_start, date_end);

            res.json({
                status: 'success',
                data: {
                    summary,
                    filters: { date_start, date_end },
                    timestamp: new Date().toISOString()
                }
            });
        } catch (error) {
            logging.error(`Data summary error: ${error.message}`);
            res.status(500).json({
                status: 'error',
                message: 'Failed to get data summary',
                error: error.message
            });
        }
    }

    /**
     * Export data with analytical optimizations
     * POST /api/data/export
     */
    static async exportData(req, res) {
        try {
            const {
                format = 'json', // json, csv, parquet
                compression = 'none', // none, gzip, snappy
                partition_by = 'date', // date, sensor_id, both
                date_from,
                date_to,
                sensor_id,
                reading_type,
                columnar = false
            } = req.body;

            const result = await etlPipeline.exportOptimizedData({
                format,
                compression,
                partition_by,
                date_from,
                date_to,
                sensor_id,
                reading_type,
                columnar
            });

            res.json({
                status: 'success',
                data: {
                    downloadUrl: result.downloadUrl,
                    filename: result.filename,
                    size: result.size,
                    recordCount: result.recordCount,
                    format,
                    compression,
                    partitioning: partition_by,
                    timestamp: new Date().toISOString()
                }
            });
        } catch (error) {
            logging.error(`Data export error: ${error.message}`);
            res.status(500).json({
                status: 'error',
                message: 'Failed to export data',
                error: error.message
            });
        }
    }

    /**
     * Download processed data files
     * GET /api/data/download/:format
     */
    static async downloadData(req, res) {
        try {
            const { format } = req.params;
            const {
                compression = 'none',
                partition_by = 'date',
                date_from,
                date_to,
                sensor_id,
                reading_type
            } = req.query;

            const result = await etlPipeline.generateDownloadFile({
                format,
                compression,
                partition_by,
                date_from,
                date_to,
                sensor_id,
                reading_type
            });

            // Set appropriate headers for file download
            const contentType = format === 'parquet' ? 'application/octet-stream' :
                format === 'csv' ? 'text/csv' : 'application/json';

            res.setHeader('Content-Type', contentType);
            res.setHeader('Content-Disposition', `attachment; filename="${result.filename}"`);

            if (compression === 'gzip') {
                res.setHeader('Content-Encoding', 'gzip');
            }

            res.sendFile(result.filePath);
        } catch (error) {
            logging.error(`Data download error: ${error.message}`);
            res.status(500).json({
                status: 'error',
                message: 'Failed to download data',
                error: error.message
            });
        }
    }
}

module.exports = DataController;
