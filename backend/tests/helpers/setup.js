const fs = require('fs-extra');
const path = require('path');

// Global test setup
beforeAll(async () => {
    // Ensure test directories exist
    const testDirs = [
        path.join(__dirname, '../temp'),
        path.join(__dirname, '../temp/data'),
        path.join(__dirname, '../temp/uploads'),
        path.join(__dirname, '../temp/processed'),
        path.join(__dirname, '../temp/raw'),
        path.join(__dirname, '../temp/logs')
    ];

    for (const dir of testDirs) {
        await fs.ensureDir(dir);
    }

    // Set test environment
    process.env.NODE_ENV = 'test';
    process.env.TEST_DATA_DIR = path.join(__dirname, '../temp/data');
    process.env.LOG_LEVEL = 'error'; // Reduce log noise during tests
});

// Global test teardown
afterAll(async () => {
    // Clean up test files
    try {
        await fs.remove(path.join(__dirname, '../temp'));
    } catch (error) {
        console.warn('Failed to clean up test files:', error.message);
    }
});

// Add custom matchers
expect.extend({
    toBeWithinRange(received, floor, ceiling) {
        const pass = received >= floor && received <= ceiling;
        if (pass) {
            return {
                message: () => `expected ${received} not to be within range ${floor} - ${ceiling}`,
                pass: true,
            };
        } else {
            return {
                message: () => `expected ${received} to be within range ${floor} - ${ceiling}`,
                pass: false,
            };
        }
    },

    toBeValidISO8601(received) {
        const iso8601Regex = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z$/;
        const pass = iso8601Regex.test(received) && !isNaN(Date.parse(received));

        if (pass) {
            return {
                message: () => `expected ${received} not to be a valid ISO 8601 date`,
                pass: true,
            };
        } else {
            return {
                message: () => `expected ${received} to be a valid ISO 8601 date`,
                pass: false,
            };
        }
    }
});
