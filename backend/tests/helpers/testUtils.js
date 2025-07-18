/**
 * Test Utilities
 * Common utilities for testing
 */

const fs = require('fs-extra');
const path = require('path');

class TestUtils {
    /**
     * Create temporary test file
     * @param {string} filename - File name
     * @param {any} content - File content
     * @returns {string} Full file path
     */
    static async createTempFile(filename, content) {
        const tempDir = path.join(__dirname, '../temp');
        await fs.ensureDir(tempDir);
        const filePath = path.join(tempDir, filename);

        if (typeof content === 'object') {
            await fs.writeJson(filePath, content, { spaces: 2 });
        } else {
            await fs.writeFile(filePath, content);
        }

        return filePath;
    }

    /**
     * Clean up temporary files
     * @param {Array} filePaths - Array of file paths to clean up
     */
    static async cleanup(filePaths) {
        for (const filePath of filePaths) {
            try {
                await fs.remove(filePath);
            } catch (error) {
                console.warn(`Failed to clean up ${filePath}:`, error.message);
            }
        }
    }

    /**
     * Wait for a specified amount of time
     * @param {number} ms - Milliseconds to wait
     */
    static async delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    /**
     * Assert array equality with tolerance for floating point numbers
     * @param {Array} actual - Actual array
     * @param {Array} expected - Expected array
     * @param {number} tolerance - Tolerance for floating point comparison
     */
    static assertArraysEqualWithTolerance(actual, expected, tolerance = 0.001) {
        expect(actual).toHaveLength(expected.length);

        for (let i = 0; i < actual.length; i++) {
            if (typeof actual[i] === 'number' && typeof expected[i] === 'number') {
                expect(Math.abs(actual[i] - expected[i])).toBeLessThanOrEqual(tolerance);
            } else {
                expect(actual[i]).toEqual(expected[i]);
            }
        }
    }

    /**
     * Generate random string
     * @param {number} length - Length of string
     * @returns {string} Random string
     */
    static generateRandomString(length = 10) {
        const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
        let result = '';
        for (let i = 0; i < length; i++) {
            result += chars.charAt(Math.floor(Math.random() * chars.length));
        }
        return result;
    }

    /**
     * Generate random number in range
     * @param {number} min - Minimum value
     * @param {number} max - Maximum value
     * @returns {number} Random number
     */
    static generateRandomNumber(min = 0, max = 100) {
        return Math.random() * (max - min) + min;
    }

    /**
     * Mock console methods for testing
     * @returns {Object} Mock functions and restore function
     */
    static mockConsole() {
        const originalLog = console.log;
        const originalError = console.error;
        const originalWarn = console.warn;

        const logs = [];
        const errors = [];
        const warnings = [];

        console.log = jest.fn((...args) => logs.push(args.join(' ')));
        console.error = jest.fn((...args) => errors.push(args.join(' ')));
        console.warn = jest.fn((...args) => warnings.push(args.join(' ')));

        return {
            logs,
            errors,
            warnings,
            restore: () => {
                console.log = originalLog;
                console.error = originalError;
                console.warn = originalWarn;
            }
        };
    }

    /**
     * Create performance test wrapper
     * @param {Function} testFunction - Function to test
     * @param {number} maxDuration - Maximum allowed duration in ms
     * @returns {Function} Wrapped test function
     */
    static performanceTest(testFunction, maxDuration = 1000) {
        return async (...args) => {
            const startTime = Date.now();
            const result = await testFunction(...args);
            const duration = Date.now() - startTime;

            expect(duration).toBeLessThanOrEqual(maxDuration);
            return result;
        };
    }

    /**
     * Create memory usage test wrapper
     * @param {Function} testFunction - Function to test
     * @param {number} maxMemoryMB - Maximum allowed memory usage in MB
     * @returns {Function} Wrapped test function
     */
    static memoryTest(testFunction, maxMemoryMB = 100) {
        return async (...args) => {
            const initialMemory = process.memoryUsage().heapUsed;
            const result = await testFunction(...args);

            // Force garbage collection if available
            if (global.gc) {
                global.gc();
            }

            const finalMemory = process.memoryUsage().heapUsed;
            const memoryUsedMB = (finalMemory - initialMemory) / 1024 / 1024;

            expect(memoryUsedMB).toBeLessThanOrEqual(maxMemoryMB);
            return result;
        };
    }

    /**
     * Validate object structure
     * @param {Object} obj - Object to validate
     * @param {Object} schema - Expected schema
     * @returns {boolean} True if valid
     */
    static validateObjectSchema(obj, schema) {
        const validateProperty = (value, schemaType) => {
            if (schemaType === 'string') return typeof value === 'string';
            if (schemaType === 'number') return typeof value === 'number';
            if (schemaType === 'boolean') return typeof value === 'boolean';
            if (schemaType === 'array') return Array.isArray(value);
            if (schemaType === 'object') return typeof value === 'object' && value !== null;
            if (schemaType === 'date') return value instanceof Date || !isNaN(Date.parse(value));
            return true;
        };

        for (const [key, expectedType] of Object.entries(schema)) {
            if (!(key in obj)) return false;
            if (!validateProperty(obj[key], expectedType)) return false;
        }

        return true;
    }
}

module.exports = TestUtils;
