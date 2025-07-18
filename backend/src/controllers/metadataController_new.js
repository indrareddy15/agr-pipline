/**
 * Metadata Controller
 * Handles metadata operations and schema information
 */

const ETLPipelineService = require('../services/etlPipeline');
const { logging } = require('../utils');

// Initialize ETL Pipeline Service
const etlPipeline = new ETLPipelineService();

class MetadataController {
    /**
     * Get unique sensor IDs
     * GET /api/metadata/sensor-ids
     */
    static async getSensorIds(req, res) {
        try {
            const sensorIds = await etlPipeline.getUniqueSensorIds();

            res.json({
                status: 'success',
                data: sensorIds,
                timestamp: new Date().toISOString()
            });
        } catch (error) {
            logging.error(`Sensor IDs retrieval error: ${error.message}`);
            res.status(500).json({
                status: 'error',
                message: 'Failed to retrieve sensor IDs',
                error: error.message
            });
        }
    }

    /**
     * Get unique reading types
     * GET /api/metadata/reading-types
     */
    static async getReadingTypes(req, res) {
        try {
            const readingTypes = await etlPipeline.getUniqueReadingTypes();

            res.json({
                status: 'success',
                data: readingTypes,
                timestamp: new Date().toISOString()
            });
        } catch (error) {
            logging.error(`Reading types retrieval error: ${error.message}`);
            res.status(500).json({
                status: 'error',
                message: 'Failed to retrieve reading types',
                error: error.message
            });
        }
    }
}

module.exports = MetadataController;
