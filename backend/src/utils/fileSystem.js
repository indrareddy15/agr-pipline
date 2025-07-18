const fs = require('fs-extra');
const path = require('path');
const { app: appConfig } = require('../config');

/**
 * File system utilities for managing files and directories
 */
class FileSystemUtils {
    /**
     * Ensure directory exists, create if not
     * @param {string} dirPath - Directory path
     */
    async ensureDir(dirPath) {
        await fs.ensureDir(dirPath);
    }

    /**
     * Read file content
     * @param {string} filePath - File path
     * @param {string} encoding - File encoding (default: 'utf8')
     * @returns {string} File content
     */
    async readFile(filePath, encoding = 'utf8') {
        return await fs.readFile(filePath, encoding);
    }

    /**
     * Write content to file
     * @param {string} filePath - File path
     * @param {string} content - Content to write
     */
    async writeFile(filePath, content) {
        await fs.writeFile(filePath, content);
    }

    /**
     * Append content to file
     * @param {string} filePath - File path
     * @param {string} content - Content to append
     */
    async appendFile(filePath, content) {
        await fs.appendFile(filePath, content);
    }

    /**
     * Check if file exists
     * @param {string} filePath - File path
     * @returns {boolean} True if file exists
     */
    async pathExists(filePath) {
        return await fs.pathExists(filePath);
    }

    /**
     * Remove file or directory
     * @param {string} path - Path to remove
     */
    async remove(path) {
        await fs.remove(path);
    }

    /**
     * Read directory contents
     * @param {string} dirPath - Directory path
     * @returns {Array<string>} Array of file/directory names
     */
    async readdir(dirPath) {
        return await fs.readdir(dirPath);
    }

    /**
     * Get processed files from checkpoint
     * @returns {Set<string>} Set of processed file names
     */
    async getProcessedFiles() {
        try {
            await this.ensureDir(appConfig.paths.CHECKPOINT_DIR);
            const data = await this.readFile(appConfig.paths.CHECKPOINT_FILE);
            return new Set(data.split('\n').filter(Boolean));
        } catch {
            return new Set();
        }
    }

    /**
     * Update checkpoint with processed file
     * @param {string} filename - Processed file name
     */
    async updateCheckpoint(filename) {
        await this.ensureDir(appConfig.paths.CHECKPOINT_DIR);
        await this.appendFile(appConfig.paths.CHECKPOINT_FILE, filename + '\n');
    }

    /**
     * Get file/directory stats
     * @param {string} filePath - File or directory path
     * @returns {Promise<fs.Stats>} File stats
     */
    async stat(filePath) {
        return await fs.stat(filePath);
    }
}

module.exports = new FileSystemUtils();
