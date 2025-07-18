const { app: appConfig } = require('../config');

/**
 * Statistical utilities for data analysis
 */
class StatisticsUtils {
    /**
     * Calculate z-score for outlier detection
     * @param {Array<number>} values - Array of numeric values
     * @returns {Array<number>} Array of z-scores
     */
    calculateZScore(values) {
        const mean = values.reduce((a, b) => a + b, 0) / values.length;
        const variance = values.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / values.length;
        const stdDev = Math.sqrt(variance);
        return values.map(v => Math.abs(v - mean) / stdDev);
    }

    /**
     * Calculate median value from array
     * @param {Array<number>} values - Array of numeric values
     * @returns {number} Median value
     */
    calculateMedian(values) {
        const sorted = [...values].sort((a, b) => a - b);
        return sorted[Math.floor(sorted.length / 2)];
    }

    /**
     * Calculate mean value from array
     * @param {Array<number>} values - Array of numeric values
     * @returns {number} Mean value
     */
    calculateMean(values) {
        return values.reduce((a, b) => a + b, 0) / values.length;
    }

    /**
     * Detect outliers using z-score method
     * @param {Array<Object>} data - Array of data records
     * @param {number} threshold - Z-score threshold for outlier detection
     * @returns {Object} Object containing cleaned data and outliers
     */
    detectOutliers(data, threshold = appConfig.processing.outlierThreshold) {
        const outliers = [];
        const cleaned = [];

        // Group by reading_type for outlier detection
        const groupedData = {};
        data.forEach(row => {
            if (!groupedData[row.reading_type]) {
                groupedData[row.reading_type] = [];
            }
            groupedData[row.reading_type].push(row);
        });

        Object.keys(groupedData).forEach(readingType => {
            const typeData = groupedData[readingType];
            const values = typeData.map(row => row.value);
            const zScores = this.calculateZScore(values);

            typeData.forEach((row, index) => {
                if (zScores[index] > threshold) {
                    outliers.push({ ...row, z_score: zScores[index] });
                    // Replace outlier with median value
                    const median = this.calculateMedian(values);
                    cleaned.push({ ...row, value: median, outlier_corrected: true });
                } else {
                    cleaned.push({ ...row, outlier_corrected: false });
                }
            });
        });

        return { cleaned, outliers };
    }

    /**
     * Calculate standard deviation
     * @param {Array<number>} values - Array of numeric values
     * @returns {number} Standard deviation
     */
    calculateStandardDeviation(values) {
        const mean = this.calculateMean(values);
        const variance = values.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / values.length;
        return Math.sqrt(variance);
    }

    /**
     * Calculate min and max values
     * @param {Array<number>} values - Array of numeric values
     * @returns {Object} Object with min and max values
     */
    calculateMinMax(values) {
        return {
            min: Math.min(...values),
            max: Math.max(...values)
        };
    }

    /**
     * Calculate percentiles
     * @param {Array<number>} values - Array of numeric values
     * @param {Array<number>} percentiles - Array of percentile values (0-100)
     * @returns {Object} Object with percentile values
     */
    calculatePercentiles(values, percentiles = [25, 50, 75, 90, 95]) {
        const sorted = [...values].sort((a, b) => a - b);
        const result = {};

        percentiles.forEach(p => {
            const index = Math.ceil((p / 100) * sorted.length) - 1;
            result[`p${p}`] = sorted[Math.max(0, index)];
        });

        return result;
    }

    /**
     * Calculate comprehensive statistics for a dataset
     * @param {Array<number>} values - Array of numeric values
     * @returns {Object} Comprehensive statistics object
     */
    calculateComprehensiveStats(values) {
        if (!values || values.length === 0) {
            return null;
        }

        const { min, max } = this.calculateMinMax(values);
        const mean = this.calculateMean(values);
        const median = this.calculateMedian(values);
        const stdDev = this.calculateStandardDeviation(values);
        const percentiles = this.calculatePercentiles(values);

        return {
            count: values.length,
            mean,
            median,
            min,
            max,
            stdDev,
            ...percentiles
        };
    }

    /**
     * Group data by specified keys
     * @param {Array<Object>} data - Array of data records
     * @param {Function} keyFunction - Function to generate grouping key
     * @returns {Object} Grouped data object
     */
    groupBy(data, keyFunction) {
        const groups = {};
        data.forEach(item => {
            const key = keyFunction(item);
            if (!groups[key]) {
                groups[key] = [];
            }
            groups[key].push(item);
        });
        return groups;
    }

    /**
     * Calculate time-based statistics for data gaps
     * @param {Array<string>} timestamps - Array of timestamp strings
     * @returns {Object} Time-based statistics
     */
    calculateTimeStats(timestamps) {
        if (!timestamps || timestamps.length < 2) {
            return null;
        }

        const dates = timestamps.map(ts => new Date(ts)).sort((a, b) => a - b);
        const gaps = [];

        for (let i = 1; i < dates.length; i++) {
            const gapHours = (dates[i] - dates[i - 1]) / (1000 * 60 * 60);
            gaps.push(gapHours);
        }

        return {
            minTime: dates[0],
            maxTime: dates[dates.length - 1],
            totalDurationHours: (dates[dates.length - 1] - dates[0]) / (1000 * 60 * 60),
            averageGapHours: this.calculateMean(gaps),
            maxGapHours: Math.max(...gaps),
            minGapHours: Math.min(...gaps)
        };
    }

    /**
     * Generate expected hourly timestamps between two dates
     * @param {Date} startDate - Start date
     * @param {Date} endDate - End date
     * @returns {Array<Date>} Array of expected hourly timestamps
     */
    generateHourlyTimestamps(startDate, endDate) {
        const timestamps = [];
        const current = new Date(startDate);
        current.setMinutes(0, 0, 0); // Round to hour

        while (current <= endDate) {
            timestamps.push(new Date(current));
            current.setHours(current.getHours() + 1);
        }

        return timestamps;
    }
}

module.exports = new StatisticsUtils();
