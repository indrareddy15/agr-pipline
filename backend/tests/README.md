# 🧪 Comprehensive Test Suite Documentation

## Agricultural Data Pipeline Testing Framework

This documentation provides complete guidance for running, understanding, and maintaining the comprehensive test suite for the Agricultural Data Pipeline application.

## 📋 Table of Contents

- [Overview](#overview)
- [Test Architecture](#test-architecture)
- [Quick Start](#quick-start)
- [Test Commands](#test-commands)
- [Test Categories](#test-categories)
- [Coverage Requirements](#coverage-requirements)
- [Performance Testing](#performance-testing)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)

## 🎯 Overview

The test suite provides **comprehensive coverage** for the agricultural data pipeline with over **200+ test cases** covering:

- ✅ **Transformation Logic**: Data processing, normalization, cleaning
- ✅ **Anomaly Detection**: Z-score calculations, outlier detection, statistical accuracy
- ✅ **Validation Rules**: Schema validation, timestamp checking, business rules
- ✅ **Data Quality**: Quality reports, metrics calculation, scoring algorithms
- ✅ **Integration Testing**: Full ETL pipeline, API endpoints, file operations
- ✅ **End-to-End Testing**: Complete system workflows with realistic data
- ✅ **Performance Testing**: Memory usage, execution time, throughput benchmarks

## 🏗️ Test Architecture

### Directory Structure

```
tests/
├── config/
│   └── testConfig.js           # Centralized test configuration
├── helpers/
│   ├── setup.js                # Global test setup and custom matchers
│   ├── testDataFactory.js      # Mock data generation for agricultural scenarios
│   └── testUtils.js            # Common utilities and helper functions
├── unit/
│   ├── dataTransformation.test.js    # Transformation logic testing
│   ├── anomalyDetection.test.js      # Anomaly detection algorithms
│   ├── dataQuality.test.js           # Data quality calculations
│   └── validationRules.test.js       # Validation rules and schema checks
├── integration/
│   ├── etlPipeline.test.js           # ETL pipeline integration tests
│   └── controllers.test.js           # API controllers integration tests
├── e2e/
│   └── fullPipeline.test.js          # End-to-end system tests
└── runner/
    └── testRunner.js                 # Advanced test runner with reporting
```

### Test Framework Stack

- **Jest 29.7.0**: Primary testing framework
- **Supertest 6.3.4**: API testing
- **Sinon 17.0.1**: Mocking and stubbing
- **Custom Test Runner**: Enhanced reporting and execution control

## 🚀 Quick Start

### 1. Install Dependencies

```bash
npm install
```

### 2. Run All Tests (Recommended)

```bash
npm run test
```

### 3. Run Specific Test Types

```bash
# Unit tests only
npm run test:unit

# Integration tests only
npm run test:integration

# End-to-end tests only
npm run test:e2e

# Coverage analysis
npm run test:coverage
```

### 4. Watch Mode for Development

```bash
# Watch all tests
npm run test:watch

# Watch unit tests only
npm run test:watch:unit
```

## 🛠️ Test Commands

### Core Test Commands

```bash
# Complete test suite with advanced runner
npm run test                    # Run all tests with comprehensive reporting
npm run test:all               # Same as above

# Individual test types
npm run test:unit              # Unit tests (5s timeout)
npm run test:integration       # Integration tests (15s timeout)
npm run test:e2e               # End-to-end tests (60s timeout)
npm run test:coverage          # Coverage analysis with reporting
npm run test:performance       # Performance benchmarking
```

### Development Commands

```bash
# Development and debugging
npm run test:watch             # Watch mode for all tests
npm run test:watch:unit        # Watch mode for unit tests only
npm run test:debug             # Debug mode with inspector
npm run test:quick             # Fast unit test execution
npm run test:smoke             # Quick smoke tests
```

### Utility Commands

```bash
# Test environment management
npm run setup:test             # Setup test environment
npm run clean:test             # Clean test artifacts
npm run pretest                # Pre-test setup
npm run posttest               # Post-test cleanup

# Reporting and analysis
npm run report:coverage        # View coverage report in browser
npm run report:test            # View test results in browser
npm run generate:test-report   # Generate detailed test report
```

### Advanced Commands

```bash
# Specific test execution
npm run test:specific -- "pattern"     # Run tests matching pattern
npm run test:file -- "path/to/test"    # Run specific test file

# CI/CD commands
npm run test:ci                # CI-optimized test execution
npm run pretest:ci             # CI pre-test setup
npm run posttest:ci            # CI post-test cleanup

# Quality assurance
npm run lint:tests             # Lint test files
npm run validate:tests         # Validate test file discovery
npm run audit:tests            # Security audit of test dependencies
```

## 📊 Test Categories

### 1. Unit Tests (`tests/unit/`)

#### Data Transformation Tests (`dataTransformation.test.js`)

- **Coverage**: 50+ test cases
- **Focus**: Data processing, normalization, timestamp handling
- **Key Tests**:
  - Data normalization algorithms
  - Timestamp format conversion
  - Missing value handling
  - Duplicate removal
  - Data type conversion
  - Edge case handling

```bash
# Run transformation tests only
npm run test:file -- "tests/unit/dataTransformation.test.js"
```

#### Anomaly Detection Tests (`anomalyDetection.test.js`)

- **Coverage**: 40+ test cases
- **Focus**: Statistical algorithms, outlier detection
- **Key Tests**:
  - Z-score calculation accuracy
  - Outlier detection algorithms
  - Statistical threshold validation
  - Agricultural data specific anomalies
  - Performance with large datasets

```bash
# Run anomaly detection tests only
npm run test:file -- "tests/unit/anomalyDetection.test.js"
```

#### Data Quality Tests (`dataQuality.test.js`)

- **Coverage**: 45+ test cases
- **Focus**: Quality metrics, scoring algorithms
- **Key Tests**:
  - Quality report generation
  - Completeness calculations
  - Accuracy assessments
  - Consistency checks
  - Timeliness validation

#### Validation Rules Tests (`validationRules.test.js`)

- **Coverage**: 60+ test cases
- **Focus**: Schema validation, business rules
- **Key Tests**:
  - Schema validation against DuckDB
  - Timestamp format validation
  - Agricultural sensor value ranges
  - Business rule enforcement
  - Error handling scenarios

### 2. Integration Tests (`tests/integration/`)

#### ETL Pipeline Tests (`etlPipeline.test.js`)

- **Coverage**: Complete pipeline workflows
- **Focus**: End-to-end data processing
- **Key Tests**:
  - Full ETL execution
  - Error recovery mechanisms
  - Performance benchmarks
  - Memory usage validation
  - File system operations

#### Controller Tests (`controllers.test.js`)

- **Coverage**: API endpoint testing
- **Focus**: HTTP API functionality
- **Key Tests**:
  - Data retrieval endpoints
  - File upload functionality
  - Pipeline control endpoints
  - Status monitoring
  - Error response handling

### 3. End-to-End Tests (`tests/e2e/`)

#### Full Pipeline Tests (`fullPipeline.test.js`)

- **Coverage**: Complete system workflows
- **Focus**: Realistic user scenarios
- **Key Tests**:
  - Complete data upload → processing → retrieval flow
  - Multi-sensor data scenarios
  - Large dataset processing
  - System integration validation
  - Performance under load

## 📈 Coverage Requirements

### Coverage Thresholds

- **Statements**: ≥ 80%
- **Branches**: ≥ 80%
- **Functions**: ≥ 80%
- **Lines**: ≥ 80%

### Coverage Reports

```bash
# Generate and view coverage
npm run test:coverage
npm run report:coverage    # Opens browser with HTML report
```

### Coverage Files Generated

- `coverage/lcov-report/index.html` - Interactive HTML report
- `coverage/lcov.info` - LCOV format for CI tools
- `coverage/coverage-final.json` - JSON format
- `coverage/clover.xml` - Clover format

## ⚡ Performance Testing

### Performance Benchmarks

| Operation           | Records/Second | Max Memory (MB) | Max Latency (ms) |
| ------------------- | -------------- | --------------- | ---------------- |
| Data Ingestion      | 1,000          | 100             | 100              |
| Data Transformation | 500            | 200             | 200              |
| Anomaly Detection   | 200            | 150             | 500              |
| Quality Validation  | 300            | 100             | 300              |
| Full Pipeline       | 100            | 500             | 5,000            |

### Running Performance Tests

```bash
npm run test:performance
```

### Performance Test Features

- Memory usage monitoring
- Execution time measurement
- Throughput calculation
- Memory leak detection
- Performance regression detection

## 🔧 Troubleshooting

### Common Issues

#### 1. Test Timeout Errors

```bash
# Increase timeout for specific test type
npm run test:unit -- --testTimeout=10000
npm run test:integration -- --testTimeout=30000
```

#### 2. Memory Issues with Large Datasets

```bash
# Run with increased memory
node --max-old-space-size=4096 node_modules/.bin/jest
```

#### 3. Database Connection Issues

```bash
# Clean and rebuild test environment
npm run clean:test
npm run setup:test
```

#### 4. File System Permission Issues

```bash
# Ensure test directories are writable
npm run setup:test-dirs
```

### Debug Mode

```bash
# Run in debug mode
npm run test:debug

# Attach debugger at chrome://inspect
```

### Verbose Output

```bash
# Get detailed test output
npm run test:unit -- --verbose --no-silent
```

## 📋 Best Practices

### 1. Test Organization

- Group related tests in `describe` blocks
- Use descriptive test names
- Follow AAA pattern (Arrange, Act, Assert)
- Clean up resources in `afterEach`/`afterAll`

### 2. Mock Data Usage

```javascript
// Use TestDataFactory for consistent mock data
const { TestDataFactory } = require("../helpers/testDataFactory");
const mockData = TestDataFactory.generateValidSensorData(100);
```

### 3. Async Testing

```javascript
// Proper async/await usage
test("should process data asynchronously", async () => {
  const result = await dataService.processData(mockData);
  expect(result).toBeDefined();
});
```

### 4. Error Testing

```javascript
// Test both success and error scenarios
test("should handle invalid data gracefully", async () => {
  await expect(dataService.processData(null)).rejects.toThrow(
    "Invalid data provided"
  );
});
```

### 5. Performance Testing

```javascript
// Use performance utilities
const { performanceTest } = require("../helpers/testUtils");

test("should meet performance requirements", async () => {
  const benchmark = { maxExecutionTime: 1000, maxMemoryMB: 100 };
  await performanceTest(() => dataService.process(largeDataset), benchmark);
});
```

## 📊 Test Reports

### Generated Reports

- **HTML Coverage Report**: `coverage/lcov-report/index.html`
- **JSON Test Results**: `test-results/test-report-{timestamp}.json`
- **JUnit XML**: `junit.xml` (for CI integration)
- **Performance Report**: `test-results/performance-report.json`

### Viewing Reports

```bash
# Open coverage report in browser
npm run report:coverage

# Open test results in browser
npm run report:test
```

## 🔄 Continuous Integration

### CI Configuration

The test suite is optimized for CI environments:

```bash
# CI-optimized test execution
npm run test:ci
```

### CI Features

- **Parallel execution**: `--maxWorkers=2`
- **No watch mode**: `--watchAll=false`
- **Extended timeout**: `--testTimeout=30000`
- **Coverage reporting**: Automatic coverage upload
- **Artifact generation**: Test reports and coverage data

## 📞 Support

For issues or questions about the test suite:

1. **Check the troubleshooting section** above
2. **Review test logs** in `test-results/`
3. **Run tests in debug mode** for detailed investigation
4. **Validate test environment** with `npm run validate:tests`

## 🎯 Summary

This comprehensive test suite provides:

✅ **200+ test cases** covering all critical functionality  
✅ **Multiple test types**: Unit, Integration, E2E, Performance  
✅ **80% coverage requirement** with detailed reporting  
✅ **Agricultural-specific scenarios** with realistic data  
✅ **Advanced test runner** with comprehensive reporting  
✅ **CI/CD ready** with optimized execution  
✅ **Development-friendly** with watch mode and debugging

**Ready for production deployment!** 🚀
