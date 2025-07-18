/**
 * API Routes for Agricultural Data Pipeline
 * Defines all RESTful endpoints using modular controllers
 */

const express = require('express');
const {
    StatusController,
    UploadController,
    DataController,
    PipelineController,
    MetadataController,
    LogController,
    CheckpointController
} = require('../controllers');

const router = express.Router();

// ==============================================
// STATUS ROUTES
// ==============================================

/**
 * Get pipeline status and statistics
 */
router.get('/status', StatusController.getStatus);

/**
 * Get data quality report
 */
router.get('/quality-report', StatusController.getQualityReport);

/**
 * Reset the entire pipeline
 */
router.delete('/reset', StatusController.resetPipeline);

// ==============================================
// UPLOAD ROUTES
// ==============================================

/**
 * Upload and process single file
 */
router.post('/upload', UploadController.singleUpload, UploadController.uploadSingle);

/**
 * Process uploaded files using ETL pipeline
 */
router.post('/process-files', UploadController.processFiles);

// ==============================================
// DATA ROUTES
// ==============================================

/**
 * Get sensor data with optional filtering
 */
router.get('/data', DataController.getData);

/**
 * Get recent sensor data
 */
router.get('/data/recent', DataController.getRecentData);

/**
 * Get summary statistics for data
 */
router.get('/data/summary', DataController.getDataSummary);

/**
 * Export data with analytical optimizations
 */
router.post('/data/export', DataController.exportData);

/**
 * Download processed data files
 */
router.get('/data/download/:format', DataController.downloadData);

// ==============================================
// PIPELINE ROUTES
// ==============================================

/**
 * Validate files before processing
 */
router.post('/pipeline/validate', PipelineController.validationUpload, PipelineController.validateFiles);

/**
 * Process specific pipeline step
 */
router.post('/pipeline/process-step', PipelineController.processStep);

// ==============================================
// METADATA ROUTES
// ==============================================

/**
 * Get unique sensor IDs
 */
router.get('/metadata/sensor-ids', MetadataController.getSensorIds);

/**
 * Get unique reading types
 */
router.get('/metadata/reading-types', MetadataController.getReadingTypes);

// ==============================================
// LOG ROUTES
// ==============================================

/**
 * Get list of available log files
 */
router.get('/logs', LogController.getLogFiles);

/**
 * Get content of a specific log file
 */
router.get('/logs/:filename', LogController.getLogContent);

/**
 * Download a specific log file
 */
router.get('/logs/:filename/download', LogController.downloadLog);

/**
 * Clear a specific log file
 */
router.delete('/logs/:filename', LogController.clearLog);

// ==============================================
// CHECKPOINT ROUTES
// ==============================================

/**
 * Get checkpoint status
 */
router.get('/checkpoints', CheckpointController.getCheckpointStatus);

/**
 * Clear all checkpoints
 */
router.delete('/checkpoints', CheckpointController.clearCheckpoints);

module.exports = router;
