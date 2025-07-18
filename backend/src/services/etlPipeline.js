const path = require('path');
const { app: appConfig } = require('../config');
const { fileSystem, logging } = require('../utils');
const dataIngestion = require('./dataIngestion');
const dataTransformation = require('./dataTransformation');
const dataQuality = require('./dataQuality');
const dataStorage = require('./dataStorage');
const summaryTablesGeneration = require('./summaryTablesGeneration');

/**
 * Main Agr Pipeline Service
 * Orchestrates the entire data processing pipeline
 */
class ETLPipelineService {
    constructor () {
        this.stats = {
            filesProcessed: 0,
            filesSkipped: 0,
            recordsIngested: 0,
            recordsFailed: 0,
            processingTime: 0
        };
    }

    /**
     * Convert BigInt values to regular numbers for JSON serialization
     * @param {any} obj - Object to process
     * @returns {any} Object with BigInt values converted to numbers
     */
    convertBigIntToNumber(obj) {
        if (obj === null || obj === undefined) {
            return obj;
        }

        if (typeof obj === 'bigint') {
            return Number(obj);
        }

        if (Array.isArray(obj)) {
            return obj.map(item => this.convertBigIntToNumber(item));
        }

        if (typeof obj === 'object') {
            const converted = {};
            for (const [key, value] of Object.entries(obj)) {
                converted[key] = this.convertBigIntToNumber(value);
            }
            return converted;
        }

        return obj;
    }

    /**
     * Initialize required directories
     */
    async initializeDirectories() {
        await fileSystem.ensureDir(appConfig.paths.RAW_DATA_DIR);
        await fileSystem.ensureDir(appConfig.paths.PROCESSED_DIR);
        await fileSystem.ensureDir(appConfig.paths.CHECKPOINT_DIR);
    }

    /**
     * Get list of new files to process
     * @returns {Array<string>} Array of new file names
     */
    async getNewFilesToProcess() {
        const processedFiles = await fileSystem.getProcessedFiles();
        const allFiles = (await fileSystem.readdir(appConfig.paths.RAW_DATA_DIR)).filter(f => f.endsWith('.parquet'));
        const newFiles = allFiles.filter(f => !processedFiles.has(f));

        logging.info(`Found ${allFiles.length} total files, ${newFiles.length} new files to process`);
        return newFiles;
    }

    /**
     * Process a single file through the complete 4-step pipeline
     * @param {string} file - File name to process
     * @returns {Object} Processing results with statistics
     */
    async processFile(file) {
        const filepath = path.join(appConfig.paths.RAW_DATA_DIR, file);
        const startTime = Date.now();
        let rawData = null; // Declare rawData outside try block for catch block access

        logging.info(`Starting 4-step pipeline processing for file: ${file}`);

        try {
            // ============================================
            // STEP 1: DATA INGESTION
            // ============================================
            logging.info(`Step 1: Data Ingestion - Processing ${file}`);

            // Schema validation using DuckDB
            const schemaValidation = await dataIngestion.validateSchemaWithDuckDB(filepath);
            if (!schemaValidation.isValid) {
                throw new Error(`Schema validation failed: ${schemaValidation.errors.join(', ')}`);
            }
            logging.info(`✓ Schema validation passed for ${file}`);

            // Quality checks on raw data
            const qualityChecks = await dataIngestion.performQualityChecks(filepath);
            logging.info(`✓ Quality checks completed for ${file}`);

            // Read and normalize data
            rawData = await dataIngestion.readParquetFile(filepath);
            if (!rawData.length) {
                logging.warn(`File ${file} is empty, skipping`);
                this.stats.filesSkipped++;
                return {
                    success: false,
                    reason: 'empty_file',
                    file,
                    processingTime: Date.now() - startTime
                };
            }
            logging.info(`✓ Step 1 Complete: Ingested ${rawData.length} records from ${file}`);

            // ============================================
            // STEP 2: DATA TRANSFORMATION
            // ============================================
            logging.info(`Step 2: Data Transformation - Processing ${rawData.length} records`);

            const transformationResult = await dataTransformation.transformData(rawData);
            const transformedData = transformationResult.transformedData;

            // Log transformation statistics
            const transformStats = dataTransformation.getStats();
            logging.info(`✓ Step 2 Complete: Transformed ${transformedData.length} records. ` +
                `Duplicates removed: ${transformStats.duplicatesRemoved}, ` +
                `Anomalies detected: ${transformStats.anomaliesDetected}`);

            // ============================================
            // STEP 3: DATA QUALITY VALIDATION
            // ============================================
            logging.info(`Step 3: Data Quality Validation - Analyzing ${transformedData.length} records`);

            const qualityReport = await dataQuality.generateDataQualityReport(transformedData);

            logging.info(`✓ Step 3 Complete: Quality report generated with overall score: ${qualityReport.summary.overallQualityScore}`);

            // ============================================
            // STEP 4: DATA LOADING & STORAGE
            // ============================================
            logging.info(`Step 4: Data Loading & Storage - Storing ${transformedData.length} records`);

            const storageResult = await dataStorage.storeProcessedData(transformedData);

            logging.info(`✓ Step 4 Complete: Stored ${storageResult.recordsStored} records in ${storageResult.partitionsCreated} partitions`);

            // ============================================
            // FINALIZATION
            // ============================================

            // Generate summary tables
            await summaryTablesGeneration.generateAllSummaryTables(transformedData);
            logging.info(`✓ Generated summary tables for ${file}`);

            // Update processing checkpoint
            await fileSystem.updateCheckpoint(file);

            // Log ingestion statistics
            await dataIngestion.logIngestionStats(file, {
                recordsProcessed: rawData.length,
                recordsFailed: rawData.length - transformedData.length,
                processingTime: Date.now() - startTime,
                status: 'success'
            });

            // Update pipeline statistics
            this.stats.recordsIngested += rawData.length;
            this.stats.filesProcessed++;

            const processingTime = Date.now() - startTime;
            this.stats.processingTime += processingTime;

            const result = {
                success: true,
                file,
                pipeline: {
                    step1_ingestion: {
                        recordsRead: rawData.length,
                        schemaValid: schemaValidation.isValid,
                        qualityChecks: qualityChecks
                    },
                    step2_transformation: {
                        recordsProcessed: transformedData.length,
                        stats: transformStats
                    },
                    step3_quality: {
                        overallScore: qualityReport.summary.overallQualityScore,
                        reportPath: appConfig.paths.QUALITY_REPORT_FILE
                    },
                    step4_storage: {
                        recordsStored: storageResult.recordsStored,
                        partitionsCreated: storageResult.partitionsCreated,
                        storagePath: storageResult.storagePath
                    }
                },
                processingTime,
                timestamp: new Date().toISOString()
            };

            logging.info(`✓ 4-step pipeline completed successfully for ${file} in ${processingTime}ms`);
            return result;

        } catch (error) {
            const processingTime = Date.now() - startTime;

            logging.error(`✗ 4-step pipeline failed for file ${file}: ${error.message}`);

            // Log detailed error information
            await dataIngestion.logIngestionStats(file, {
                recordsProcessed: 0,
                recordsFailed: rawData ? rawData.length : 0,
                processingTime,
                status: 'failed',
                error: error.message
            });

            // Update failure statistics
            this.stats.recordsFailed += rawData ? rawData.length : 0;
            this.stats.filesSkipped++;

            return {
                success: false,
                file,
                error: error.message,
                processingTime,
                timestamp: new Date().toISOString()
            };
        }
    }

    /**
     * Generate and log quality report
     * @param {Array<Object>} allTransformedData - All transformed data
     */
    async generateQualityReport(allTransformedData) {
        if (allTransformedData.length === 0) {
            logging.warn('No data available for quality report generation');
            return;
        }

        logging.info('Generating comprehensive data quality report...');
        const qualityReport = await dataQuality.generateDataQualityReport(allTransformedData);
        logging.info(`✓ Data quality report generated and saved to: ${appConfig.paths.QUALITY_REPORT_FILE}`);

        // Log summary of quality metrics
        logging.info('\n=== DATA QUALITY SUMMARY ===');
        logging.info(`Overall Quality Score: ${qualityReport.summary.overallQualityScore.toFixed(2)}`);
        logging.info(`Total Records: ${qualityReport.summary.totalRecords}`);
        logging.info(`Missing Values: ${qualityReport.summary.missingValues} (${qualityReport.details.missing_values.percentage}%)`);
        logging.info(`Anomalous Readings: ${qualityReport.summary.anomalousReadings} (${qualityReport.details.anomalous_readings.percentage}%)`);
        logging.info(`Outliers Corrected: ${qualityReport.summary.outliersCorreted} (${qualityReport.details.outliers_corrected.percentage}%)`);
    }

    /**
     * Log final ingestion statistics
     */
    async logFinalStats() {
        await logging.logIngestionStats(this.stats);

        logging.info('\n=== INGESTION SUMMARY ===');
        logging.info(`Files processed: ${this.stats.filesProcessed}`);
        logging.info(`Files skipped: ${this.stats.filesSkipped}`);
        logging.info(`Records ingested: ${this.stats.recordsIngested}`);
        logging.info(`Records failed: ${this.stats.recordsFailed}`);
        logging.info(`Processing time: ${(this.stats.processingTime / 1000).toFixed(2)}s`);
    }

    /**
     * Run the complete Agr pipeline
     */
    async runPipeline() {
        const startTime = Date.now();

        try {
            logging.info('Starting Agr pipeline...');

            // Initialize directories
            await this.initializeDirectories();

            // Get new files to process
            const newFiles = await this.getNewFilesToProcess();

            if (newFiles.length === 0) {
                logging.info('No new files to process');
                return;
            }

            // Process all files
            const allTransformedData = [];

            for (const file of newFiles) {
                const transformedData = await this.processFile(file);
                if (transformedData) {
                    allTransformedData.push(...transformedData);
                }
            }

            // Generate quality report
            await this.generateQualityReport(allTransformedData);

            // Generate final summary tables for all data
            if (allTransformedData.length > 0) {
                await summaryTablesGeneration.generateAllSummaryTables(allTransformedData);
                await summaryTablesGeneration.generateSummaryMetadata(allTransformedData);
                logging.info('✓ Generated comprehensive summary tables');
            }

            // Log final statistics
            this.stats.processingTime = Date.now() - startTime;
            await this.logFinalStats();

            logging.info('✓ Agr pipeline completed successfully');

        } catch (err) {
            logging.error(`✗ Agr pipeline failed: ${err.message}`);
            process.exit(1);
        }
    }

    /**
     * Get current pipeline statistics
     * @returns {Object} Current statistics
     */
    getStats() {
        return {
            ...this.stats,
            lastUpdated: new Date().toISOString()
        };
    }

    /**
     * Get list of processed files
     * @returns {Set<string>} Set of processed file names
     */
    async getProcessedFilesList() {
        return await fileSystem.getProcessedFiles();
    }

    /**
     * Generate data quality report for existing processed data
     */
    async generateDataQualityReport() {
        try {
            // Read all processed data
            const processedData = await this.getAllProcessedData();
            if (processedData.length === 0) {
                logging.warn('No processed data available for quality report');
                return null;
            }

            // Generate and cache the quality report
            const qualityReport = await dataQuality.generateDataQualityReport(processedData);

            // Cache the report for future requests
            this.cachedQualityReport = qualityReport;

            // Generate the quality report (this saves it to file)
            await this.generateQualityReport(processedData);

            return qualityReport;
        } catch (error) {
            logging.error(`Failed to generate data quality report: ${error.message}`);
            throw error;
        }
    }

    /**
     * Get existing data quality report
     * @returns {Object|null} Quality report data or null if not exists
     */
    async getDataQualityReport() {
        try {
            // Return cached report if available
            if (this.cachedQualityReport) {
                return this.cachedQualityReport;
            }

            // Try to read from file
            const reportPath = appConfig.paths.QUALITY_REPORT_FILE;
            if (await fileSystem.pathExists(reportPath)) {
                const csv = require('csv-parser');
                const fs = require('fs');

                return new Promise((resolve, reject) => {
                    const results = [];
                    fs.createReadStream(reportPath)
                        .pipe(csv())
                        .on('data', (data) => results.push(data))
                        .on('end', () => resolve(results))
                        .on('error', reject);
                });
            }
            return null;
        } catch (error) {
            logging.error(`Failed to read quality report: ${error.message}`);
            return null;
        }
    }

    /**
     * Get processed data with filtering and pagination using optimized DuckDB queries
     * @param {Object} filters - Filter criteria and pagination
     * @returns {Array<Object>} Filtered and paginated data
     */
    async getProcessedData(filters = {}) {
        try {
            const {
                sensor_id,
                date_from,
                date_to,
                reading_type,
                limit = 1000,
                offset = 0
            } = filters;

            const processedDir = appConfig.paths.PROCESSED_DIR;

            // Use DuckDB for efficient querying with push-down predicates
            const { database: dbConfig } = require('../config');
            const db = dbConfig.createInMemoryDatabase();
            const con = dbConfig.getConnection(db);
            // Only include partitioned sensor data files, not summaries
            const parquetGlob = `${processedDir.replace(/\\/g, '/').replace(/\/$/, '')}/date=*/sensor_id=*/data.parquet`;

            try {
                // Build the WHERE clause based on filters
                const whereConditions = [];
                if (sensor_id) whereConditions.push(`sensor_id = '${sensor_id}'`);
                if (date_from) whereConditions.push(`timestamp >= '${date_from}'`);
                if (date_to) whereConditions.push(`timestamp <= '${date_to}'`);
                if (reading_type) whereConditions.push(`reading_type = '${reading_type}'`);

                const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

                const query = `
                    SELECT * FROM read_parquet('${parquetGlob}')
                    ${whereClause}
                    ORDER BY timestamp DESC
                    LIMIT ${limit} OFFSET ${offset}
                `;

                const result = await new Promise((resolve, reject) => {
                    con.all(query, (err, rows) => {
                        if (err) reject(err);
                        else resolve(rows);
                    });
                });

                await dbConfig.closeConnection(con);
                return result;
            } catch (error) {
                await dbConfig.closeConnection(con);
                throw error;
            }
        } catch (error) {
            logging.error(`Failed to get processed data: ${error.message}`);
            return [];
        }
    }

    /**
     * Get all processed data from parquet files
     * @returns {Array<Object>} All processed data
     */
    async getAllProcessedData() {
        try {
            const processedDir = appConfig.paths.PROCESSED_DIR;
            const allData = [];

            // Recursively read all parquet files in processed directory
            const readProcessedFiles = async (dir) => {
                const items = await fileSystem.readdir(dir);

                for (const item of items) {
                    const itemPath = path.join(dir, item);
                    const stat = await fileSystem.stat(itemPath);

                    if (stat.isDirectory()) {
                        await readProcessedFiles(itemPath);
                    } else if (item.endsWith('.parquet')) {
                        try {
                            const data = await dataIngestion.readParquetFile(itemPath);
                            allData.push(...data);
                        } catch (error) {
                            logging.warn(`Failed to read processed file ${itemPath}: ${error.message}`);
                        }
                    }
                }
            };

            await readProcessedFiles(processedDir);
            return allData;
        } catch (error) {
            logging.error(`Failed to read all processed data: ${error.message}`);
            return [];
        }
    }

    /**
     * Clear checkpoints for reprocessing
     */
    async clearCheckpoints() {
        try {
            const checkpointFile = path.join(appConfig.paths.CHECKPOINT_DIR, 'processed_files.txt');
            if (await fileSystem.pathExists(checkpointFile)) {
                await fileSystem.remove(checkpointFile);
                logging.info('Checkpoints cleared');
            }
        } catch (error) {
            logging.error(`Failed to clear checkpoints: ${error.message}`);
            throw error;
        }
    }

    /**
     * Reset the entire pipeline (clear processed data and checkpoints)
     */
    async resetPipeline() {
        try {
            // Clear processed data
            const processedDir = appConfig.paths.PROCESSED_DIR;
            if (await fileSystem.pathExists(processedDir)) {
                await fileSystem.remove(processedDir);
                await fileSystem.ensureDir(processedDir);
                logging.info('Processed data cleared');
            }

            // Clear checkpoints
            await this.clearCheckpoints();

            // Clear quality report
            const reportPath = appConfig.paths.QUALITY_REPORT_FILE;
            if (await fileSystem.pathExists(reportPath)) {
                await fileSystem.remove(reportPath);
                logging.info('Quality report cleared');
            }

            // Reset stats
            this.stats = {
                filesProcessed: 0,
                filesSkipped: 0,
                recordsIngested: 0,
                recordsFailed: 0,
                processingTime: 0
            };

            logging.info('Pipeline reset completed');
        } catch (error) {
            logging.error(`Failed to reset pipeline: ${error.message}`);
            throw error;
        }
    }

    /**
     * Get unique sensor IDs efficiently using JSON files
     * @returns {Array<string>} Array of unique sensor IDs
     */
    async getSensorIds() {
        try {
            // Read all data from JSON files
            const allData = await this.readDataFromJSONFiles();

            // Extract unique sensor IDs
            const sensorIds = [...new Set(allData.map(record => record.sensor_id))];
            sensorIds.sort();

            return sensorIds;
        } catch (error) {
            logging.error(`Failed to get sensor IDs: ${error.message}`);
            return [];
        }
    }

    /**
     * Get unique reading types efficiently using JSON files
     * @returns {Array<string>} Array of unique reading types
     */
    async getReadingTypes() {
        try {
            // Read all data from JSON files
            const allData = await this.readDataFromJSONFiles();

            // Extract unique reading types
            const readingTypes = [...new Set(allData.map(record => record.reading_type))];
            readingTypes.sort();

            return readingTypes;
        } catch (error) {
            logging.error(`Failed to get reading types: ${error.message}`);
            return [];
        }
    }

    /**
     * Get metadata summary for quick overview
     * @returns {Object} Metadata summary
     */
    async getMetadataSummary() {
        try {
            const processedDir = appConfig.paths.PROCESSED_DIR;

            // Use DuckDB to efficiently query summary statistics
            const { database: dbConfig } = require('../config');
            const db = dbConfig.createInMemoryDatabase();
            const con = dbConfig.getConnection(db);
            // Only include partitioned sensor data files, not summaries
            const parquetGlob = `${processedDir.replace(/\\/g, '/').replace(/\/$/, '')}/date=*/sensor_id=*/data.parquet`;

            try {
                const query = `
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(DISTINCT sensor_id) as unique_sensors,
                        COUNT(DISTINCT reading_type) as unique_reading_types,
                        MIN(timestamp) as earliest_timestamp,
                        MAX(timestamp) as latest_timestamp,
                        COUNT(DISTINCT DATE(timestamp)) as unique_dates
                    FROM read_parquet('${parquetGlob}')
                `;

                const result = await new Promise((resolve, reject) => {
                    con.all(query, (err, rows) => {
                        if (err) reject(err);
                        else resolve(rows[0]);
                    });
                });

                await dbConfig.closeConnection(con);

                // Convert BigInt values to strings for JSON serialization
                const summary = JSON.parse(JSON.stringify(result, (key, value) =>
                    typeof value === 'bigint' ? value.toString() : value
                ));

                return summary;
            } catch (error) {
                await dbConfig.closeConnection(con);
                throw error;
            }
        } catch (error) {
            logging.error(`Failed to get metadata summary: ${error.message}`);
            return {
                total_records: 0,
                unique_sensors: 0,
                unique_reading_types: 0,
                earliest_timestamp: null,
                latest_timestamp: null,
                unique_dates: 0
            };
        }
    }

    /**
     * Individual Pipeline Step Methods for Step-by-Step Processing
     */

    /**
     * Perform data ingestion step
     * @param {Array<string>} filenames - Array of filenames to process
     * @param {Object} options - Processing options
     * @returns {Object} Ingestion results
     */
    async performIngestion(filenames, options = {}) {
        const results = [];

        for (const filename of filenames) {
            try {
                const filepath = path.join(appConfig.paths.RAW_DATA_DIR, filename);

                // Validate schema
                await dataIngestion.validateSchemaWithDuckDB(filepath);

                // Read file data
                const fileData = await dataIngestion.readParquetFile(filepath);

                results.push({
                    filename,
                    status: 'success',
                    recordsRead: fileData.length,
                    message: `Successfully read ${fileData.length} records`
                });

                logging.info(`✓ Ingestion completed for ${filename}: ${fileData.length} records`);
            } catch (error) {
                results.push({
                    filename,
                    status: 'error',
                    error: error.message
                });
                logging.error(`✗ Ingestion failed for ${filename}: ${error.message}`);
            }
        }

        return { files: results };
    }

    /**
     * Perform data transformation step
     * @param {Array<string>} filenames - Array of filenames to process
     * @param {Object} options - Processing options
     * @returns {Object} Transformation results
     */
    async performTransformation(filenames, options = {}) {
        const results = [];

        for (const filename of filenames) {
            try {
                const filepath = path.join(appConfig.paths.RAW_DATA_DIR, filename);

                // Read file data
                const fileData = await dataIngestion.readParquetFile(filepath);

                // Transform data
                const transformationResult = await dataTransformation.transformData(fileData);
                const transformed = transformationResult.transformedData || transformationResult; // Handle both formats

                results.push({
                    filename,
                    status: 'success',
                    recordsTransformed: Array.isArray(transformed) ? transformed.length : 0,
                    originalRecords: fileData.length,
                    message: `Successfully transformed ${Array.isArray(transformed) ? transformed.length : 0} records`
                });

                logging.info(`✓ Transformation completed for ${filename}: ${transformed.length} records`);
            } catch (error) {
                results.push({
                    filename,
                    status: 'error',
                    error: error.message
                });
                logging.error(`✗ Transformation failed for ${filename}: ${error.message}`);
            }
        }

        return { files: results };
    }

    /**
     * Perform data validation step
     * @param {Array<string>} filenames - Array of filenames to process
     * @param {Object} options - Processing options
     * @returns {Object} Validation results
     */
    async performValidation(filenames, options = {}) {
        const results = [];

        for (const filename of filenames) {
            try {
                const filepath = path.join(appConfig.paths.RAW_DATA_DIR, filename);

                // Read file data
                const fileData = await dataIngestion.readParquetFile(filepath);

                // Perform validation
                const validation = await dataIngestion.validateDataWithDuckDB(fileData);

                // Convert BigInt values to strings for JSON serialization
                const validationForLogging = JSON.parse(JSON.stringify(validation, (key, value) =>
                    typeof value === 'bigint' ? value.toString() : value
                ));

                results.push({
                    filename,
                    status: 'success',
                    validation: validationForLogging,
                    recordsValidated: fileData.length,
                    message: 'Quality validation completed'
                });

                logging.info(`✓ Validation completed for ${filename}`);
            } catch (error) {
                results.push({
                    filename,
                    status: 'error',
                    error: error.message
                });
                logging.error(`✗ Validation failed for ${filename}: ${error.message}`);
            }
        }

        return { files: results };
    }

    /**
     * Perform data loading/storage step
     * @param {Array<string>} filenames - Array of filenames to process
     * @param {Object} options - Processing options
     * @returns {Object} Loading results
     */
    async performLoading(filenames, options = {}) {
        const results = [];

        for (const filename of filenames) {
            try {
                const filepath = path.join(appConfig.paths.RAW_DATA_DIR, filename);

                // Read and transform data
                const fileData = await dataIngestion.readParquetFile(filepath);
                const transformationResult = await dataTransformation.transformData(fileData);
                const transformed = transformationResult.transformedData || transformationResult; // Handle both formats

                // Store transformed data
                await dataStorage.writeParquetPartitioned(transformed);

                // Generate summary tables
                await summaryTablesGeneration.generateAllSummaryTables(transformed);

                // Update statistics
                this.stats.filesProcessed++;
                this.stats.recordsProcessed += transformed.length;

                results.push({
                    filename,
                    status: 'success',
                    recordsStored: transformed.length,
                    message: `Successfully stored ${transformed.length} records`
                });

                logging.info(`✓ Loading completed for ${filename}: ${transformed.length} records stored`);
            } catch (error) {
                results.push({
                    filename,
                    status: 'error',
                    error: error.message
                });
                logging.error(`✗ Loading failed for ${filename}: ${error.message}`);
            }
        }

        return { files: results };
    }

    /**
     * Get filtered data with pagination
     * @param {Object} filters - Filter criteria
     * @returns {Object} Filtered data results
     */
    async getFilteredData(filters = {}) {
        try {
            const {
                sensor_id,
                date_from,
                date_to,
                reading_type,
                limit = 1000,
                offset = 0
            } = filters;

            // Use JSON files approach to bypass DuckDB parquet issues
            const data = await this.readDataFromJSONFiles(filters);

            // Apply pagination
            const total = data.length;
            const paginatedData = data.slice(offset, offset + limit);

            return {
                data: paginatedData,
                pagination: {
                    limit,
                    offset,
                    total,
                    pages: Math.ceil(total / limit)
                }
            };
        } catch (error) {
            logging.error(`Filtered data retrieval error: ${error.message}`);
            throw error;
        }
    }

    /**
     * Read data from JSON files with filtering
     * @param {Object} filters - Filter criteria
     * @returns {Array} Filtered data
     */
    async readDataFromJSONFiles(filters = {}) {
        try {
            const processedDir = appConfig.paths.PROCESSED_DIR;
            const allData = [];

            // Read all JSON files from processed directory
            const readJSONFiles = async (dir) => {
                try {
                    const items = await fileSystem.readdir(dir);

                    for (const item of items) {
                        const itemPath = path.join(dir, item);
                        const stat = await fileSystem.stat(itemPath);

                        if (stat.isDirectory()) {
                            await readJSONFiles(itemPath);
                        } else if (item === 'data.json') {
                            try {
                                const fileContent = await fileSystem.readFile(itemPath, 'utf8');
                                const data = JSON.parse(fileContent);
                                allData.push(...data);
                            } catch (error) {
                                logging.warn(`Failed to read JSON file ${itemPath}: ${error.message}`);
                            }
                        }
                    }
                } catch (error) {
                    // Directory might not exist yet, that's OK
                    logging.warn(`Directory not accessible: ${dir}`);
                }
            };

            await readJSONFiles(processedDir);

            // Apply filters
            let filteredData = allData;

            if (filters.sensor_id) {
                filteredData = filteredData.filter(record => record.sensor_id === filters.sensor_id);
            }

            if (filters.reading_type) {
                filteredData = filteredData.filter(record => record.reading_type === filters.reading_type);
            }

            if (filters.date_from) {
                filteredData = filteredData.filter(record =>
                    new Date(record.timestamp) >= new Date(filters.date_from)
                );
            }

            if (filters.date_to) {
                filteredData = filteredData.filter(record =>
                    new Date(record.timestamp) <= new Date(filters.date_to)
                );
            }

            // Sort by timestamp descending
            filteredData.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));

            return filteredData;

        } catch (error) {
            logging.error(`JSON data reading error: ${error.message}`);
            return [];
        }
    }

    /**
     * Get unique sensor IDs from the database
     * @returns {Array<string>} Array of unique sensor IDs
     */
    async getUniqueSensorIds() {
        try {
            // Use the same method as getSensorIds
            return await this.getSensorIds();
        } catch (error) {
            logging.error(`Unique sensor IDs retrieval error: ${error.message}`);
            throw error;
        }
    }

    /**
     * Get unique reading types from the database
     * @returns {Array<string>} Array of unique reading types
     */
    async getUniqueReadingTypes() {
        try {
            // Use the same method as getReadingTypes
            return await this.getReadingTypes();
        } catch (error) {
            logging.error(`Unique reading types retrieval error: ${error.message}`);
            throw error;
        }
    }

    /**
     * Get recent sensor data within specified hours
     * @param {number} hours - Number of hours to look back (default: 24)
     * @param {number} limit - Maximum number of records to return (default: 100)
     * @returns {Array} Recent sensor data
     */
    async getRecentData(hours = 24, limit = 100) {
        try {
            const hoursAgo = new Date();
            hoursAgo.setHours(hoursAgo.getHours() - hours);

            const filters = {
                date_from: hoursAgo.toISOString(),
                limit: limit,
                offset: 0
            };

            const result = await this.getFilteredData(filters);
            return result.data;
        } catch (error) {
            logging.error(`Recent data retrieval error: ${error.message}`);
            throw error;
        }
    }

    /**
     * Get data summary statistics for a date range
     * @param {string} dateStart - Start date (ISO string)
     * @param {string} dateEnd - End date (ISO string)
     * @returns {Object} Data summary statistics
     */
    async getDataSummary(dateStart, dateEnd) {
        try {
            const filters = {};

            if (dateStart) {
                filters.date_from = dateStart;
            }

            if (dateEnd) {
                filters.date_to = dateEnd;
            }

            // Get all data within the date range
            const data = await this.readDataFromJSONFiles(filters);

            // Calculate summary statistics
            const summary = {
                total_records: data.length,
                unique_sensors: [...new Set(data.map(record => record.sensor_id))].length,
                unique_reading_types: [...new Set(data.map(record => record.reading_type))].length,
                date_range: {
                    start: dateStart,
                    end: dateEnd
                },
                sensor_breakdown: {},
                reading_type_breakdown: {},
                timestamps: {
                    earliest: null,
                    latest: null
                }
            };

            if (data.length > 0) {
                // Get timestamp range
                const timestamps = data.map(record => new Date(record.timestamp)).sort();
                summary.timestamps.earliest = timestamps[0].toISOString();
                summary.timestamps.latest = timestamps[timestamps.length - 1].toISOString();

                // Calculate sensor breakdown
                data.forEach(record => {
                    if (!summary.sensor_breakdown[record.sensor_id]) {
                        summary.sensor_breakdown[record.sensor_id] = 0;
                    }
                    summary.sensor_breakdown[record.sensor_id]++;
                });

                // Calculate reading type breakdown
                data.forEach(record => {
                    if (!summary.reading_type_breakdown[record.reading_type]) {
                        summary.reading_type_breakdown[record.reading_type] = 0;
                    }
                    summary.reading_type_breakdown[record.reading_type]++;
                });
            }

            return summary;
        } catch (error) {
            logging.error(`Data summary error: ${error.message}`);
            throw error;
        }
    }

    /**
     * Delete data within a specified date range
     * @param {string} dateStart - Start date (ISO string)
     * @param {string} dateEnd - End date (ISO string)
     * @returns {Object} Result with deleted count
     */
    async deleteDataInDateRange(dateStart, dateEnd) {
        try {
            if (!dateStart || !dateEnd) {
                throw new Error('Both dateStart and dateEnd are required');
            }

            // Get all data to find what matches the criteria
            const filters = {
                date_from: dateStart,
                date_to: dateEnd
            };

            const dataToDelete = await this.readDataFromJSONFiles(filters);
            const deletedCount = dataToDelete.length;

            // Since we're using JSON files stored in date partitions, 
            // we need to delete the files/directories that fall within the date range
            const processedDir = appConfig.paths.PROCESSED_DIR;

            // Parse date range
            const startDate = new Date(dateStart);
            const endDate = new Date(dateEnd);

            let actualDeletedCount = 0;

            // Check each date directory
            const items = await fileSystem.readdir(processedDir);

            for (const item of items) {
                if (item.startsWith('date=')) {
                    const dateStr = item.replace('date=', '');
                    const itemDate = new Date(dateStr);

                    // If the date falls within the range, delete the directory
                    if (itemDate >= startDate && itemDate <= endDate) {
                        const itemPath = path.join(processedDir, item);
                        try {
                            await fileSystem.rmdir(itemPath, { recursive: true });
                            logging.info(`Deleted date directory: ${item}`);
                            actualDeletedCount += await this.countRecordsInDateDir(itemPath);
                        } catch (error) {
                            logging.error(`Failed to delete directory ${item}: ${error.message}`);
                        }
                    }
                }
            }

            return {
                deletedCount: actualDeletedCount > 0 ? actualDeletedCount : deletedCount,
                dateRange: { dateStart, dateEnd },
                message: `Deleted data from ${dateStart} to ${dateEnd}`
            };

        } catch (error) {
            logging.error(`Data deletion error: ${error.message}`);
            throw error;
        }
    }

    /**
     * Helper method to count records in a date directory (for deletion reporting)
     * @param {string} dateDirPath - Path to the date directory
     * @returns {number} Number of records that would be deleted
     */
    async countRecordsInDateDir(dateDirPath) {
        try {
            let count = 0;
            const readJSONFiles = async (dir) => {
                try {
                    const items = await fileSystem.readdir(dir);

                    for (const item of items) {
                        const itemPath = path.join(dir, item);
                        const stat = await fileSystem.stat(itemPath);

                        if (stat.isDirectory()) {
                            await readJSONFiles(itemPath);
                        } else if (item === 'data.json') {
                            try {
                                const fileContent = await fileSystem.readFile(itemPath, 'utf8');
                                const data = JSON.parse(fileContent);
                                count += data.length;
                            } catch (error) {
                                logging.warn(`Could not read ${itemPath} for counting: ${error.message}`);
                            }
                        }
                    }
                } catch (error) {
                    // Directory might not exist, that's ok
                    logging.warn(`Could not read directory ${dir} for counting: ${error.message}`);
                }
            };

            await readJSONFiles(dateDirPath);
            return count;
        } catch (error) {
            logging.warn(`Error counting records in ${dateDirPath}: ${error.message}`);
            return 0;
        }
    }

    /**
     * Export data with analytical optimizations
     * @param {Object} options - Export options
     * @returns {Object} Export result with download info
     */
    async exportOptimizedData(options = {}) {
        try {
            const {
                format = 'json',
                compression = 'none',
                partition_by = 'date',
                date_from,
                date_to,
                sensor_id,
                reading_type,
                columnar = false
            } = options;

            // Get filtered data
            const filters = {};
            if (date_from) filters.date_from = date_from;
            if (date_to) filters.date_to = date_to;
            if (sensor_id) filters.sensor_id = sensor_id;
            if (reading_type) filters.reading_type = reading_type;

            const data = await this.readDataFromJSONFiles(filters);

            // Generate filename
            const timestamp = new Date().toISOString().split('T')[0];
            const compressionSuffix = compression !== 'none' ? `.${compression}` : '';
            const filename = `sensor_data_${timestamp}_${format}${compressionSuffix}`;

            // Create optimized export based on format
            let exportData;
            let contentType;

            if (format === 'parquet') {
                exportData = await this.generateParquetExport(data, options);
                contentType = 'application/octet-stream';
            } else if (format === 'csv') {
                exportData = await this.generateCSVExport(data, options);
                contentType = 'text/csv';
            } else {
                exportData = await this.generateJSONExport(data, options);
                contentType = 'application/json';
            }

            // Apply compression if requested
            if (compression !== 'none') {
                exportData = await this.compressData(exportData, compression);
            }

            // Save to temporary file
            const tempDir = path.join(appConfig.paths.RAW_DATA_DIR, '../temp');
            await fileSystem.ensureDir(tempDir);
            const filePath = path.join(tempDir, filename);

            if (format === 'parquet' || compression !== 'none') {
                await fileSystem.writeFile(filePath, exportData);
            } else {
                await fileSystem.writeFile(filePath, exportData, 'utf8');
            }

            const stats = await fileSystem.stat(filePath);

            return {
                downloadUrl: `/api/data/download/${format}?filename=${filename}`,
                filename,
                filePath,
                size: stats.size,
                recordCount: data.length,
                format,
                compression,
                partitioning: partition_by
            };

        } catch (error) {
            logging.error(`Export optimization error: ${error.message}`);
            throw error;
        }
    }

    /**
     * Generate download file with specified options
     * @param {Object} options - Download options
     * @returns {Object} File info for download
     */
    async generateDownloadFile(options = {}) {
        try {
            return await this.exportOptimizedData(options);
        } catch (error) {
            logging.error(`Download file generation error: ${error.message}`);
            throw error;
        }
    }

    /**
     * Generate Parquet export with columnar optimization
     * @param {Array} data - Data to export
     * @param {Object} options - Export options
     * @returns {Buffer} Parquet data
     */
    async generateParquetExport(data, options = {}) {
        try {
            const { database: dbConfig } = require('../config');
            const db = dbConfig.createInMemoryDatabase();
            const con = dbConfig.getConnection(db);

            // Create temporary table with data
            const createTableQuery = `
                CREATE TEMPORARY TABLE temp_export AS 
                SELECT * FROM (VALUES ${data.map(row =>
                `('${row.sensor_id}', '${row.timestamp}', '${row.reading_type}', ${row.value}, ${row.battery_level || 'NULL'})`
            ).join(', ')}) AS t(sensor_id, timestamp, reading_type, value, battery_level)
            `;

            await new Promise((resolve, reject) => {
                con.run(createTableQuery, (err) => {
                    if (err) reject(err);
                    else resolve();
                });
            });

            // Export as Parquet with compression
            const tempFile = path.join(appConfig.paths.RAW_DATA_DIR, '../temp', 'temp_export.parquet');
            const compressionType = options.compression === 'snappy' ? 'SNAPPY' : 'GZIP';

            const exportQuery = `COPY temp_export TO '${tempFile}' (FORMAT PARQUET, COMPRESSION '${compressionType}')`;

            await new Promise((resolve, reject) => {
                con.run(exportQuery, (err) => {
                    if (err) reject(err);
                    else resolve();
                });
            });

            await dbConfig.closeConnection(con);

            // Read the generated file
            return await fileSystem.readFile(tempFile);

        } catch (error) {
            logging.error(`Parquet export error: ${error.message}`);
            throw error;
        }
    }

    /**
     * Generate CSV export with optional columnar optimization
     * @param {Array} data - Data to export
     * @param {Object} options - Export options
     * @returns {string} CSV data
     */
    async generateCSVExport(data, options = {}) {
        try {
            if (data.length === 0) return '';

            const { columnar = false, partition_by = 'date' } = options;

            if (columnar && partition_by !== 'none') {
                // Group data by partition key
                const partitioned = {};

                data.forEach(row => {
                    let key;
                    if (partition_by === 'date') {
                        key = new Date(row.timestamp).toISOString().split('T')[0];
                    } else if (partition_by === 'sensor_id') {
                        key = row.sensor_id;
                    } else if (partition_by === 'both') {
                        const date = new Date(row.timestamp).toISOString().split('T')[0];
                        key = `${date}_${row.sensor_id}`;
                    } else {
                        key = 'all';
                    }

                    if (!partitioned[key]) partitioned[key] = [];
                    partitioned[key].push(row);
                });

                // Generate CSV with partition headers
                let csvContent = '';
                Object.entries(partitioned).forEach(([partition, rows]) => {
                    csvContent += `# Partition: ${partition}\n`;
                    const headers = Object.keys(rows[0]);
                    csvContent += headers.join(',') + '\n';

                    rows.forEach(row => {
                        csvContent += headers.map(field => {
                            const value = row[field];
                            if (typeof value === 'string' && (value.includes(',') || value.includes('"'))) {
                                return `"${value.replace(/"/g, '""')}"`;
                            }
                            return value;
                        }).join(',') + '\n';
                    });
                    csvContent += '\n';
                });

                return csvContent;
            } else {
                // Standard CSV export
                const headers = Object.keys(data[0]);
                return [
                    headers.join(','),
                    ...data.map(row =>
                        headers.map(field => {
                            const value = row[field];
                            if (typeof value === 'string' && (value.includes(',') || value.includes('"'))) {
                                return `"${value.replace(/"/g, '""')}"`;
                            }
                            return value;
                        }).join(',')
                    )
                ].join('\n');
            }

        } catch (error) {
            logging.error(`CSV export error: ${error.message}`);
            throw error;
        }
    }

    /**
     * Generate JSON export with optional partitioning
     * @param {Array} data - Data to export
     * @param {Object} options - Export options
     * @returns {string} JSON data
     */
    async generateJSONExport(data, options = {}) {
        try {
            const { partition_by = 'none', columnar = false } = options;

            if (partition_by !== 'none' && columnar) {
                // Partition data and create columnar structure
                const partitioned = {};

                data.forEach(row => {
                    let key;
                    if (partition_by === 'date') {
                        key = new Date(row.timestamp).toISOString().split('T')[0];
                    } else if (partition_by === 'sensor_id') {
                        key = row.sensor_id;
                    } else if (partition_by === 'both') {
                        const date = new Date(row.timestamp).toISOString().split('T')[0];
                        key = `${date}_${row.sensor_id}`;
                    }

                    if (!partitioned[key]) {
                        partitioned[key] = {
                            sensor_id: [],
                            timestamp: [],
                            reading_type: [],
                            value: [],
                            battery_level: []
                        };
                    }

                    Object.keys(partitioned[key]).forEach(field => {
                        partitioned[key][field].push(row[field]);
                    });
                });

                return JSON.stringify({
                    format: 'columnar',
                    partitioning: partition_by,
                    partitions: partitioned
                }, null, 2);
            } else {
                // Standard JSON export
                return JSON.stringify({
                    format: 'row-based',
                    data: data,
                    metadata: {
                        recordCount: data.length,
                        exportedAt: new Date().toISOString(),
                        partitioning: partition_by
                    }
                }, null, 2);
            }

        } catch (error) {
            logging.error(`JSON export error: ${error.message}`);
            throw error;
        }
    }

    /**
     * Compress data using specified compression algorithm
     * @param {string|Buffer} data - Data to compress
     * @param {string} compression - Compression type (gzip, snappy)
     * @returns {Buffer} Compressed data
     */
    async compressData(data, compression) {
        try {
            const zlib = require('zlib');

            if (compression === 'gzip') {
                return zlib.gzipSync(Buffer.isBuffer(data) ? data : Buffer.from(data));
            } else if (compression === 'snappy') {
                // Note: For snappy compression in Node.js, you'd need a library like 'snappy'
                // For now, fall back to gzip
                logging.warn('Snappy compression not available, using gzip instead');
                return zlib.gzipSync(Buffer.isBuffer(data) ? data : Buffer.from(data));
            } else {
                return Buffer.isBuffer(data) ? data : Buffer.from(data);
            }
        } catch (error) {
            logging.error(`Compression error: ${error.message}`);
            throw error;
        }
    }
}

module.exports = ETLPipelineService;
