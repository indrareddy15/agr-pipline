/**
 * Upload Controller
 * Handles file upload operations
 */

const multer = require('multer');
const path = require('path');
const fs = require('fs').promises;
const ETLPipelineService = require('../services/etlPipeline');
const { logging } = require('../utils');

// Initialize ETL Pipeline Service
const etlPipeline = new ETLPipelineService();

// Configure multer for file uploads
const upload = multer({
    dest: path.join(__dirname, '../../data/raw'),
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

class UploadController {
    /**
     * Get multer middleware for single/multiple file uploads
     */
    static get singleUpload() {
        return upload.array('files', 20); // Support multiple files
    }

    /**
     * Handle file upload and processing
     * POST /api/upload
     */
    static async uploadSingle(req, res) {
        try {
            // Handle both single file and multiple files
            const files = req.files || (req.file ? [req.file] : []);

            if (files.length === 0) {
                return res.status(400).json({
                    status: 'error',
                    message: 'No files uploaded'
                });
            }

            const uploadedFiles = [];
            const processingResults = [];

            // Process uploaded files
            for (const file of files) {
                const filename = file.originalname;
                const newPath = path.join(file.destination, filename);

                // Rename file to original name
                await fs.rename(file.path, newPath);

                uploadedFiles.push({
                    filename,
                    size: file.size,
                    path: newPath
                });
            }

            logging.info(`Files uploaded: ${uploadedFiles.length} file(s)`);

            // Process the uploaded files through the 4-step pipeline
            const shouldProcess = req.body.process !== 'false'; // Default to true

            if (shouldProcess) {
                logging.info(`Starting 4-step pipeline processing for ${uploadedFiles.length} uploaded file(s)`);

                for (const file of uploadedFiles) {
                    try {
                        const result = await etlPipeline.processFile(file.filename);
                        processingResults.push({
                            filename: file.filename,
                            ...result
                        });
                    } catch (error) {
                        processingResults.push({
                            filename: file.filename,
                            success: false,
                            error: error.message
                        });
                    }
                }
            }

            // Generate response
            const totalFiles = uploadedFiles.length;
            const successfulProcessing = processingResults.filter(r => r.success).length;
            const failedProcessing = processingResults.filter(r => !r.success).length;

            const response = {
                status: 'success',
                message: shouldProcess ?
                    `${totalFiles} file(s) uploaded and processed` :
                    `${totalFiles} file(s) uploaded successfully`,
                data: {
                    files: uploadedFiles,
                    totalFiles,
                    processed: shouldProcess,
                    timestamp: new Date().toISOString()
                }
            };

            if (shouldProcess) {
                response.data.processing = {
                    totalProcessed: processingResults.length,
                    successful: successfulProcessing,
                    failed: failedProcessing,
                    results: processingResults
                };

                // Update message based on processing results
                if (failedProcessing > 0) {
                    response.message = `${totalFiles} file(s) uploaded, ${successfulProcessing} processed successfully, ${failedProcessing} failed`;
                }
            }

            res.json(response);

        } catch (error) {
            logging.error(`Upload error: ${error.message}`);
            res.status(500).json({
                status: 'error',
                message: 'Failed to upload file',
                error: error.message
            });
        }
    }

    /**
     * Process uploaded files using ETL pipeline
     * POST /api/process-files
     */
    static async processFiles(req, res) {
        try {
            const { filenames } = req.body;

            if (!filenames || !Array.isArray(filenames) || filenames.length === 0) {
                return res.status(400).json({
                    status: 'error',
                    message: 'No filenames provided for processing'
                });
            }

            logging.logSystem('INFO', `Starting processing for ${filenames.length} files`);

            // Process files through ETL pipeline
            const results = await etlPipeline.processFilesWithMonitoring(filenames);

            res.json({
                status: 'success',
                message: 'Files processed successfully',
                data: {
                    results,
                    timestamp: new Date().toISOString()
                }
            });
        } catch (error) {
            logging.error(`File processing error: ${error.message}`);
            res.status(500).json({
                status: 'error',
                message: 'Failed to process files',
                error: error.message
            });
        }
    }
}

module.exports = UploadController;
