/**
 * Status Controller
 * Handles pipeline status and health endpoints
 */

const ETLPipelineService = require('../services/etlPipeline');
const { logging } = require('../utils');

// Initialize ETL Pipeline Service
const etlPipeline = new ETLPipelineService();

class StatusController {
    /**
     * Get pipeline status and statistics
     * GET /api/status
     */
    static async getStatus(req, res) {
        try {
            const stats = await etlPipeline.getStats();
            const processedFiles = await etlPipeline.getProcessedFilesList();

            res.json({
                status: 'success',
                data: {
                    stats,
                    processedFiles: Array.from(processedFiles),
                    timestamp: new Date().toISOString()
                }
            });
        } catch (error) {
            logging.logSystem('ERROR', `Error getting status: ${error.message}`);
            res.status(500).json({
                status: 'error',
                message: 'Failed to get pipeline status',
                error: error.message
            });
        }
    }

    /**
     * Get quality report
     * GET /api/quality-report
     */
    static async getQualityReport(req, res) {
        try {
            // First try to get existing report
            let report = await etlPipeline.getDataQualityReport();

            // If no report exists, try to generate one from processed data
            if (!report) {
                report = await etlPipeline.generateDataQualityReport();
            }

            res.json({
                status: 'success',
                data: {
                    report,
                    timestamp: new Date().toISOString()
                }
            });
        } catch (error) {
            logging.error(`Quality report error: ${error.message}`);
            res.status(500).json({
                status: 'error',
                message: 'Failed to get quality report',
                error: error.message
            });
        }
    }

    /**
     * Reset the entire pipeline
     * DELETE /api/reset
     */
    static async resetPipeline(req, res) {
        try {
            await etlPipeline.resetPipeline();

            res.json({
                status: 'success',
                message: 'Pipeline reset completed',
                timestamp: new Date().toISOString()
            });
        } catch (error) {
            logging.error(`Reset error: ${error.message}`);
            res.status(500).json({
                status: 'error',
                message: 'Failed to reset pipeline',
                error: error.message
            });
        }
    }
}

module.exports = StatusController;
