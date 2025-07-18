/**
 * Pipeline Controller
 * Handles pipeline processing operations and step-by-step execution
 */

const multer = require('multer');
const path = require('path');
const fs = require('fs').promises;
const ETLPipelineService = require('../services/etlPipeline');
const dataIngestion = require('../services/dataIngestion');
const { logging } = require('../utils');

// Initialize ETL Pipeline Service
const etlPipeline = new ETLPipelineService();

// Configure multer for validation uploads (temporary files)
const tempDir = path.join(__dirname, '../../data/temp');

// Ensure temp directory exists
const ensureTempDir = async () => {
    try {
        await fs.mkdir(tempDir, { recursive: true });
    } catch (error) {
        // Directory might already exist, which is fine
    }
};

const upload = multer({
    dest: tempDir,
    limits: {
        fileSize: 100 * 1024 * 1024 // 100MB limit
    },
    fileFilter: (req, file, cb) => {
        const allowedExtensions = ['.parquet', '.csv', '.json'];
        const fileExtension = path.extname(file.originalname).toLowerCase();

        if (allowedExtensions.includes(fileExtension)) {
            cb(null, true);
        } else {
            cb(new Error(`Invalid file type. Only ${allowedExtensions.join(', ')} files are allowed.`));
        }
    }
});

// Initialize temp directory
ensureTempDir();

class PipelineController {
    /**
     * Get multer middleware for file validation uploads
     */
    static get validationUpload() {
        return upload.array('files', 20); // Max 20 files for validation
    }

    /**
     * Validate files before processing
     * POST /api/pipeline/validate
     */
    static async validateFiles(req, res) {
        try {
            // Handle file uploads for validation
            if (req.files && req.files.length > 0) {
                const validationResults = [];

                for (const file of req.files) {
                    try {
                        // Validate schema using dataIngestion service
                        await dataIngestion.validateSchemaWithDuckDB(file.path);

                        validationResults.push({
                            filename: file.originalname,
                            status: 'valid',
                            message: 'Schema validation passed',
                            size: file.size
                        });

                        // Clean up temporary file
                        await fs.unlink(file.path);
                    } catch (error) {
                        validationResults.push({
                            filename: file.originalname,
                            status: 'invalid',
                            error: error.message,
                            size: file.size
                        });

                        // Clean up temporary file
                        try {
                            await fs.unlink(file.path);
                        } catch (cleanupError) {
                            // Ignore cleanup errors
                        }
                    }
                }

                res.json({
                    status: 'success',
                    data: {
                        validationResults,
                        totalFiles: validationResults.length,
                        validFiles: validationResults.filter(r => r.status === 'valid').length,
                        invalidFiles: validationResults.filter(r => r.status === 'invalid').length,
                        timestamp: new Date().toISOString()
                    }
                });
                return;
            }

            // Handle filename-based validation (for already uploaded files)
            const { filenames } = req.body;

            if (!filenames || !Array.isArray(filenames) || filenames.length === 0) {
                return res.status(400).json({
                    status: 'error',
                    message: 'No files provided for validation'
                });
            }

            const validationResults = await etlPipeline.validateFiles(filenames);

            res.json({
                status: 'success',
                data: {
                    validationResults,
                    timestamp: new Date().toISOString()
                }
            });
        } catch (error) {
            logging.error(`File validation error: ${error.message}`);
            res.status(500).json({
                status: 'error',
                message: 'Failed to validate files',
                error: error.message
            });
        }
    }

    /**
     * Process specific pipeline step
     * POST /api/pipeline/process-step
     */
    static async processStep(req, res) {
        try {
            const { step, filenames, options = {} } = req.body;

            if (!step) {
                return res.status(400).json({
                    status: 'error',
                    message: 'Pipeline step is required'
                });
            }

            if (!filenames || !Array.isArray(filenames) || filenames.length === 0) {
                return res.status(400).json({
                    status: 'error',
                    message: 'No filenames provided for processing'
                });
            }

            let result;

            switch (step) {
                case 'ingestion':
                    result = await etlPipeline.performIngestion(filenames, options);
                    break;
                case 'transformation':
                    result = await etlPipeline.performTransformation(filenames, options);
                    break;
                case 'validation':
                    result = await etlPipeline.performValidation(filenames, options);
                    break;
                case 'loading':
                    result = await etlPipeline.performLoading(filenames, options);
                    break;
                default:
                    return res.status(400).json({
                        status: 'error',
                        message: `Invalid pipeline step: ${step}. Valid steps are: ingestion, transformation, validation, loading`
                    });
            }

            res.json({
                status: 'success',
                data: {
                    step,
                    result,
                    timestamp: new Date().toISOString()
                }
            });
        } catch (error) {
            logging.error(`Pipeline step ${req.body.step} error: ${error.message}`);
            res.status(500).json({
                status: 'error',
                message: `Failed to process pipeline step: ${req.body.step}`,
                error: error.message
            });
        }
    }

    /**
     * Run complete pipeline for files
     * POST /api/pipeline/run
     */
    static async runPipeline(req, res) {
        try {
            const { filenames, options = {} } = req.body;

            if (!filenames || !Array.isArray(filenames) || filenames.length === 0) {
                return res.status(400).json({
                    status: 'error',
                    message: 'No filenames provided for pipeline execution'
                });
            }

            logging.logSystem('INFO', `Starting complete pipeline for ${filenames.length} files`);

            const results = await etlPipeline.processFilesWithMonitoring(filenames, options);

            res.json({
                status: 'success',
                message: 'Pipeline executed successfully',
                data: {
                    results,
                    processedFiles: filenames.length,
                    timestamp: new Date().toISOString()
                }
            });
        } catch (error) {
            logging.error(`Pipeline execution error: ${error.message}`);
            res.status(500).json({
                status: 'error',
                message: 'Failed to execute pipeline',
                error: error.message
            });
        }
    }

    /**
     * Get pipeline execution status
     * GET /api/pipeline/status/:executionId
     */
    static async getPipelineStatus(req, res) {
        try {
            const { executionId } = req.params;
            const status = await etlPipeline.getExecutionStatus(executionId);

            res.json({
                status: 'success',
                data: {
                    executionId,
                    status,
                    timestamp: new Date().toISOString()
                }
            });
        } catch (error) {
            logging.error(`Pipeline status error: ${error.message}`);
            res.status(500).json({
                status: 'error',
                message: 'Failed to get pipeline status',
                error: error.message
            });
        }
    }
}

module.exports = PipelineController;
