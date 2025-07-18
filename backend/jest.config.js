module.exports = {
    testEnvironment: 'node',
    roots: ['<rootDir>/tests'],
    testMatch: [
        '**/__tests__/**/*.js',
        '**/?(*.)+(spec|test).js'
    ],
    collectCoverageFrom: [
        'src/**/*.js',
        '!src/server.js',
        '!**/node_modules/**',
        '!**/vendor/**'
    ],
    coverageDirectory: 'coverage',
    coverageReporters: ['text', 'lcov', 'html', 'json'],
    setupFilesAfterEnv: ['<rootDir>/tests/helpers/setup.js'],
    testTimeout: 30000,
    verbose: true,
    collectCoverage: true, // Enable coverage by default
    coverageThreshold: {
        global: {
            branches: 80,
            functions: 80,
            lines: 80,
            statements: 80
        }
    },
    moduleNameMapper: {
        '^@/(.*)$': '<rootDir>/src/$1',
        '^@tests/(.*)$': '<rootDir>/tests/$1'
    },
    reporters: [
        'default',
        ['jest-html-reporter', {
            pageTitle: 'Agricultural Pipeline Test Report',
            outputPath: 'test-results/test-report.html',
            includeFailureMsg: true,
            includeSuiteFailure: true,
            includeConsoleLog: true
        }],
        ['jest-junit', {
            outputDirectory: 'test-results',
            outputName: 'junit.xml',
            classNameTemplate: '{classname}',
            titleTemplate: '{title}',
            ancestorSeparator: ' › ',
            usePathForSuiteName: true
        }]
    ],
    testPathIgnorePatterns: [
        '/node_modules/',
        '/coverage/',
        '/test-results/'
    ],
    transform: {},
    transformIgnorePatterns: [
        'node_modules/(?!(module-that-needs-to-be-transformed)/)'
    ]
};
