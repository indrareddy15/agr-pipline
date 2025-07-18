/**
 * Services index file
 * Exports all service modules
 */

const dataIngestionService = require('./dataIngestion');
const dataTransformationService = require('./dataTransformation');
const dataQualityService = require('./dataQuality');
const dataStorageService = require('./dataStorage');
const ETLPipelineService = require('./etlPipeline');
const timeGapDetectionService = require('./timeGapDetection');
const advancedDataProfilingService = require('./advancedDataProfiling');
const summaryTablesGenerationService = require('./summaryTablesGeneration');

module.exports = {
    dataIngestion: dataIngestionService,
    dataTransformation: dataTransformationService,
    dataQuality: dataQualityService,
    dataStorage: dataStorageService,
    ETLPipelineService: ETLPipelineService,
    timeGapDetection: timeGapDetectionService,
    advancedDataProfiling: advancedDataProfilingService,
    summaryTablesGeneration: summaryTablesGenerationService
};
