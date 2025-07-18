/**
 * Checkpoint Controller
 * Handles checkpoint and processed files tracking operations
 */

const ETLPipelineService = require('../services/etlPipeline');
const { logging } = require('../utils');

// Initialize ETL Pipeline Service
const etlPipeline = new ETLPipelineService();

class CheckpointController {
    /**
     * Get checkpoint status
     * GET /api/checkpoints/status
     */
    static async getCheckpointStatus(req, res) {
        try {
            const processedFiles = await etlPipeline.getProcessedFilesList();
            const stats = await etlPipeline.getStats();

            res.json({
                status: 'success',
                data: {
                    processedFiles: Array.from(processedFiles),
                    totalProcessed: processedFiles.size,
                    stats,
                    timestamp: new Date().toISOString()
                }
            });
        } catch (error) {
            logging.error(`Checkpoint status error: ${error.message}`);
            res.status(500).json({
                status: 'error',
                message: 'Failed to get checkpoint status',
                error: error.message
            });
        }
    }

    /**
     * Add files to processed list
     * POST /api/checkpoints/mark-processed
     */
    static async markFilesAsProcessed(req, res) {
        try {
            const { filenames } = req.body;

            if (!filenames || !Array.isArray(filenames) || filenames.length === 0) {
                return res.status(400).json({
                    status: 'error',
                    message: 'No filenames provided to mark as processed'
                });
            }

            await etlPipeline.markFilesAsProcessed(filenames);

            logging.logSystem('INFO', `Marked ${filenames.length} files as processed`);

            res.json({
                status: 'success',
                message: `${filenames.length} files marked as processed`,
                data: {
                    processedFiles: filenames,
                    timestamp: new Date().toISOString()
                }
            });
        } catch (error) {
            logging.error(`Mark processed error: ${error.message}`);
            res.status(500).json({
                status: 'error',
                message: 'Failed to mark files as processed',
                error: error.message
            });
        }
    }

    /**
     * Remove files from processed list
     * POST /api/checkpoints/unmark-processed
     */
    static async unmarkFilesAsProcessed(req, res) {
        try {
            const { filenames } = req.body;

            if (!filenames || !Array.isArray(filenames) || filenames.length === 0) {
                return res.status(400).json({
                    status: 'error',
                    message: 'No filenames provided to unmark as processed'
                });
            }

            await etlPipeline.unmarkFilesAsProcessed(filenames);

            logging.logSystem('INFO', `Unmarked ${filenames.length} files as processed`);

            res.json({
                status: 'success',
                message: `${filenames.length} files unmarked as processed`,
                data: {
                    unmarkedFiles: filenames,
                    timestamp: new Date().toISOString()
                }
            });
        } catch (error) {
            logging.error(`Unmark processed error: ${error.message}`);
            res.status(500).json({
                status: 'error',
                message: 'Failed to unmark files as processed',
                error: error.message
            });
        }
    }

    /**
     * Clear all processed files checkpoint
     * DELETE /api/checkpoints/clear
     */
    static async clearCheckpoints(req, res) {
        try {
            await etlPipeline.clearProcessedFiles();

            logging.logSystem('INFO', 'All checkpoints cleared');

            res.json({
                status: 'success',
                message: 'All checkpoints cleared successfully',
                data: {
                    timestamp: new Date().toISOString()
                }
            });
        } catch (error) {
            logging.error(`Clear checkpoints error: ${error.message}`);
            res.status(500).json({
                status: 'error',
                message: 'Failed to clear checkpoints',
                error: error.message
            });
        }
    }

    /**
     * Get processing history
     * GET /api/checkpoints/history
     */
    static async getProcessingHistory(req, res) {
        try {
            const { days = 7 } = req.query;
            const history = await etlPipeline.getProcessingHistory(parseInt(days));

            res.json({
                status: 'success',
                data: {
                    history,
                    days: parseInt(days),
                    timestamp: new Date().toISOString()
                }
            });
        } catch (error) {
            logging.error(`Processing history error: ${error.message}`);
            res.status(500).json({
                status: 'error',
                message: 'Failed to get processing history',
                error: error.message
            });
        }
    }
}

module.exports = CheckpointController;
