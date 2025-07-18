#!/usr/bin/env node

/**
 * Advanced Test Runner for Agricultural Data Pipeline
 * Provides comprehensive testing with detailed reporting and performance metrics
 */

const { spawn, execSync } = require('child_process');
const path = require('path');
const fs = require('fs-extra');
const chalk = require('chalk');

class TestRunner {
    constructor () {
        this.projectRoot = path.join(__dirname, '..');
        this.testResults = {
            unit: null,
            integration: null,
            e2e: null,
            coverage: null,
            performance: {}
        };
        this.startTime = Date.now();
    }

    /**
     * Display header with test runner information
     */
    displayHeader() {
        console.log(chalk.cyan('='.repeat(80)));
        console.log(chalk.cyan.bold('🧪 AGRICULTURAL DATA PIPELINE - COMPREHENSIVE TEST RUNNER'));
        console.log(chalk.cyan('='.repeat(80)));
        console.log(chalk.yellow(`📅 Started: ${new Date().toISOString()}`));
        console.log(chalk.yellow(`📂 Project: ${this.projectRoot}`));
        console.log(chalk.cyan('-'.repeat(80)));
    }

    /**
     * Run unit tests
     */
    async runUnitTests() {
        console.log(chalk.blue.bold('\n🔬 RUNNING UNIT TESTS'));
        console.log(chalk.blue('-'.repeat(40)));

        const startTime = Date.now();

        try {
            const result = await this.executeTest('npm run test:unit', 'Unit Tests');
            this.testResults.unit = {
                passed: result.success,
                duration: Date.now() - startTime,
                details: result.output
            };

            if (result.success) {
                console.log(chalk.green('✅ Unit tests passed successfully'));
            } else {
                console.log(chalk.red('❌ Unit tests failed'));
            }
        } catch (error) {
            console.error(chalk.red(`❌ Unit tests error: ${error.message}`));
            this.testResults.unit = {
                passed: false,
                duration: Date.now() - startTime,
                error: error.message
            };
        }
    }

    /**
     * Run integration tests
     */
    async runIntegrationTests() {
        console.log(chalk.blue.bold('\n🔗 RUNNING INTEGRATION TESTS'));
        console.log(chalk.blue('-'.repeat(40)));

        const startTime = Date.now();

        try {
            const result = await this.executeTest('npm run test:integration', 'Integration Tests');
            this.testResults.integration = {
                passed: result.success,
                duration: Date.now() - startTime,
                details: result.output
            };

            if (result.success) {
                console.log(chalk.green('✅ Integration tests passed successfully'));
            } else {
                console.log(chalk.red('❌ Integration tests failed'));
            }
        } catch (error) {
            console.error(chalk.red(`❌ Integration tests error: ${error.message}`));
            this.testResults.integration = {
                passed: false,
                duration: Date.now() - startTime,
                error: error.message
            };
        }
    }

    /**
     * Run end-to-end tests
     */
    async runE2ETests() {
        console.log(chalk.blue.bold('\n🌐 RUNNING END-TO-END TESTS'));
        console.log(chalk.blue('-'.repeat(40)));

        const startTime = Date.now();

        try {
            const result = await this.executeTest('npm run test:e2e', 'E2E Tests');
            this.testResults.e2e = {
                passed: result.success,
                duration: Date.now() - startTime,
                details: result.output
            };

            if (result.success) {
                console.log(chalk.green('✅ E2E tests passed successfully'));
            } else {
                console.log(chalk.red('❌ E2E tests failed'));
            }
        } catch (error) {
            console.error(chalk.red(`❌ E2E tests error: ${error.message}`));
            this.testResults.e2e = {
                passed: false,
                duration: Date.now() - startTime,
                error: error.message
            };
        }
    }

    /**
     * Run coverage analysis
     */
    async runCoverageAnalysis() {
        console.log(chalk.blue.bold('\n📊 RUNNING COVERAGE ANALYSIS'));
        console.log(chalk.blue('-'.repeat(40)));

        const startTime = Date.now();

        try {
            const result = await this.executeTest('npm run test:coverage', 'Coverage Analysis');
            this.testResults.coverage = {
                passed: result.success,
                duration: Date.now() - startTime,
                details: result.output
            };

            if (result.success) {
                console.log(chalk.green('✅ Coverage analysis completed successfully'));
                await this.parseCoverageResults();
            } else {
                console.log(chalk.red('❌ Coverage analysis failed'));
            }
        } catch (error) {
            console.error(chalk.red(`❌ Coverage analysis error: ${error.message}`));
            this.testResults.coverage = {
                passed: false,
                duration: Date.now() - startTime,
                error: error.message
            };
        }
    }

    /**
     * Run performance tests
     */
    async runPerformanceTests() {
        console.log(chalk.blue.bold('\n⚡ RUNNING PERFORMANCE TESTS'));
        console.log(chalk.blue('-'.repeat(40)));

        const performanceTests = [
            { name: 'Data Ingestion', pattern: '**/dataIngestion.test.js' },
            { name: 'Data Transformation', pattern: '**/dataTransformation.test.js' },
            { name: 'ETL Pipeline', pattern: '**/etlPipeline.test.js' }
        ];

        for (const test of performanceTests) {
            const startTime = Date.now();
            try {
                console.log(chalk.yellow(`  🏃‍♂️ Running ${test.name} performance tests...`));
                const result = await this.executeTest(
                    `npx jest --testPathPattern="${test.pattern}" --verbose`,
                    test.name
                );

                this.testResults.performance[test.name] = {
                    passed: result.success,
                    duration: Date.now() - startTime,
                    details: result.output
                };

                if (result.success) {
                    console.log(chalk.green(`    ✅ ${test.name} performance tests passed`));
                } else {
                    console.log(chalk.red(`    ❌ ${test.name} performance tests failed`));
                }
            } catch (error) {
                console.error(chalk.red(`    ❌ ${test.name} performance error: ${error.message}`));
                this.testResults.performance[test.name] = {
                    passed: false,
                    duration: Date.now() - startTime,
                    error: error.message
                };
            }
        }
    }

    /**
     * Execute a test command
     * @param {string} command - Command to execute
     * @param {string} testName - Name of the test for logging
     * @returns {Promise<Object>} Test result
     */
    async executeTest(command, testName) {
        return new Promise((resolve, reject) => {
            console.log(chalk.gray(`  🔄 Executing: ${command}`));

            const child = spawn(command, {
                shell: true,
                cwd: this.projectRoot,
                stdio: 'pipe'
            });

            let output = '';
            let errorOutput = '';

            child.stdout.on('data', (data) => {
                const text = data.toString();
                output += text;
                process.stdout.write(chalk.gray(text));
            });

            child.stderr.on('data', (data) => {
                const text = data.toString();
                errorOutput += text;
                process.stderr.write(chalk.yellow(text));
            });

            child.on('close', (code) => {
                const success = code === 0;
                resolve({
                    success,
                    code,
                    output: output.trim(),
                    error: errorOutput.trim()
                });
            });

            child.on('error', (error) => {
                reject(error);
            });

            // Set timeout for long-running tests
            setTimeout(() => {
                child.kill();
                reject(new Error(`Test timeout: ${testName} exceeded 5 minutes`));
            }, 5 * 60 * 1000); // 5 minutes
        });
    }

    /**
     * Parse coverage results from coverage reports
     */
    async parseCoverageResults() {
        try {
            const coverageDir = path.join(this.projectRoot, 'coverage');
            const summaryFile = path.join(coverageDir, 'coverage-summary.json');

            if (await fs.pathExists(summaryFile)) {
                const summary = await fs.readJson(summaryFile);
                console.log(chalk.green('\n📈 Coverage Summary:'));

                if (summary.total) {
                    const { lines, statements, functions, branches } = summary.total;
                    console.log(chalk.white(`  Lines: ${lines.pct}%`));
                    console.log(chalk.white(`  Statements: ${statements.pct}%`));
                    console.log(chalk.white(`  Functions: ${functions.pct}%`));
                    console.log(chalk.white(`  Branches: ${branches.pct}%`));

                    // Check coverage thresholds
                    const threshold = 80;
                    const allPassed = [lines.pct, statements.pct, functions.pct, branches.pct]
                        .every(pct => pct >= threshold);

                    if (allPassed) {
                        console.log(chalk.green(`  ✅ All coverage thresholds met (>= ${threshold}%)`));
                    } else {
                        console.log(chalk.red(`  ❌ Some coverage thresholds not met (< ${threshold}%)`));
                    }
                }
            }
        } catch (error) {
            console.warn(chalk.yellow(`  ⚠️  Could not parse coverage results: ${error.message}`));
        }
    }

    /**
     * Generate comprehensive test report
     */
    async generateTestReport() {
        console.log(chalk.blue.bold('\n📋 GENERATING TEST REPORT'));
        console.log(chalk.blue('-'.repeat(40)));

        const totalDuration = Date.now() - this.startTime;
        const report = {
            timestamp: new Date().toISOString(),
            duration: totalDuration,
            results: this.testResults,
            summary: this.generateSummary()
        };

        // Save detailed report
        const reportsDir = path.join(this.projectRoot, 'test-reports');
        await fs.ensureDir(reportsDir);

        const reportFile = path.join(reportsDir, `test-report-${Date.now()}.json`);
        await fs.writeJson(reportFile, report, { spaces: 2 });

        console.log(chalk.green(`  📄 Detailed report saved: ${reportFile}`));

        // Display summary
        this.displaySummary(report.summary);

        return report;
    }

    /**
     * Generate test summary
     */
    generateSummary() {
        const summary = {
            total: 0,
            passed: 0,
            failed: 0,
            duration: Date.now() - this.startTime
        };

        const testTypes = ['unit', 'integration', 'e2e', 'coverage'];

        testTypes.forEach(type => {
            if (this.testResults[type]) {
                summary.total++;
                if (this.testResults[type].passed) {
                    summary.passed++;
                } else {
                    summary.failed++;
                }
            }
        });

        // Add performance tests
        Object.keys(this.testResults.performance).forEach(() => {
            summary.total++;
            // Performance tests counted based on success
        });

        summary.successRate = summary.total > 0 ? (summary.passed / summary.total * 100).toFixed(1) : 0;

        return summary;
    }

    /**
     * Display test summary
     */
    displaySummary(summary) {
        console.log(chalk.cyan('\n' + '='.repeat(80)));
        console.log(chalk.cyan.bold('📊 TEST EXECUTION SUMMARY'));
        console.log(chalk.cyan('='.repeat(80)));

        console.log(chalk.white(`⏱️  Total Duration: ${this.formatDuration(summary.duration)}`));
        console.log(chalk.white(`📝 Total Tests: ${summary.total}`));
        console.log(chalk.green(`✅ Passed: ${summary.passed}`));
        console.log(chalk.red(`❌ Failed: ${summary.failed}`));
        console.log(chalk.blue(`📈 Success Rate: ${summary.successRate}%`));

        // Overall status
        if (summary.failed === 0) {
            console.log(chalk.green.bold('\n🎉 ALL TESTS PASSED! READY FOR PRODUCTION'));
        } else {
            console.log(chalk.red.bold('\n⚠️  SOME TESTS FAILED - REVIEW REQUIRED'));
        }

        console.log(chalk.cyan('='.repeat(80)));
    }

    /**
     * Format duration in human readable format
     */
    formatDuration(ms) {
        const seconds = Math.floor(ms / 1000);
        const minutes = Math.floor(seconds / 60);
        const hours = Math.floor(minutes / 60);

        if (hours > 0) {
            return `${hours}h ${minutes % 60}m ${seconds % 60}s`;
        } else if (minutes > 0) {
            return `${minutes}m ${seconds % 60}s`;
        } else {
            return `${seconds}s`;
        }
    }

    /**
     * Run all tests
     */
    async runAll() {
        this.displayHeader();

        // Run tests in sequence for better reporting
        await this.runUnitTests();
        await this.runIntegrationTests();
        await this.runE2ETests();
        await this.runCoverageAnalysis();
        await this.runPerformanceTests();

        // Generate final report
        const report = await this.generateTestReport();

        // Exit with appropriate code
        const hasFailures = report.summary.failed > 0;
        process.exit(hasFailures ? 1 : 0);
    }

    /**
     * Run specific test type
     */
    async runSpecific(testType) {
        this.displayHeader();

        switch (testType) {
            case 'unit':
                await this.runUnitTests();
                break;
            case 'integration':
                await this.runIntegrationTests();
                break;
            case 'e2e':
                await this.runE2ETests();
                break;
            case 'coverage':
                await this.runCoverageAnalysis();
                break;
            case 'performance':
                await this.runPerformanceTests();
                break;
            default:
                console.error(chalk.red(`Unknown test type: ${testType}`));
                process.exit(1);
        }

        await this.generateTestReport();
    }
}

// CLI execution
if (require.main === module) {
    const testRunner = new TestRunner();
    const args = process.argv.slice(2);

    if (args.length === 0) {
        testRunner.runAll();
    } else {
        testRunner.runSpecific(args[0]);
    }
}

module.exports = TestRunner;
