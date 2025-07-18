const moment = require('moment-timezone');
const { app: appConfig } = require('../config');

/**
 * Timestamp processing utilities for consistent time handling
 */
class TimestampUtils {
    constructor () {
        this.timezone = appConfig.timezone.format;
        this.utcOffset = appConfig.timezone.utcOffset; // UTC+5:30 in minutes
        this.iso8601Offset = appConfig.timezone.iso8601_format; // +05:30
        this.displayName = appConfig.timezone.display_name;
    }

    /**
     * Convert timestamp to ISO 8601 format with UTC+5:30 timezone
     * @param {string|Date} timestamp - Input timestamp
     * @returns {string} ISO 8601 formatted timestamp with timezone
     */
    toISO8601WithTimezone(timestamp) {
        try {
            let momentObj;

            // Handle different input types
            if (typeof timestamp === 'string') {
                // Try parsing as is first
                momentObj = moment.tz(timestamp, this.timezone);

                // If invalid, try without timezone info and assume UTC
                if (!momentObj.isValid()) {
                    momentObj = moment.utc(timestamp);
                }
            } else if (timestamp instanceof Date) {
                momentObj = moment.tz(timestamp, this.timezone);
            } else {
                // Fallback to current time
                momentObj = moment.tz(this.timezone);
            }

            // Ensure it's valid
            if (!momentObj.isValid()) {
                momentObj = moment.tz(this.timezone);
            }

            // Convert to UTC+5:30 and format as ISO 8601
            return momentObj.utcOffset(this.utcOffset).format('YYYY-MM-DDTHH:mm:ss.SSSZ');

        } catch (error) {
            // Fallback to current time with correct timezone
            return moment.tz(this.timezone).utcOffset(this.utcOffset).format('YYYY-MM-DDTHH:mm:ss.SSSZ');
        }
    }

    /**
     * Convert timestamp to UTC format
     * @param {string|Date} timestamp - Input timestamp
     * @returns {string} UTC ISO 8601 formatted timestamp
     */
    toUTC(timestamp) {
        try {
            let momentObj;

            if (typeof timestamp === 'string') {
                momentObj = moment.tz(timestamp, this.timezone);
                if (!momentObj.isValid()) {
                    momentObj = moment.utc(timestamp);
                }
            } else if (timestamp instanceof Date) {
                momentObj = moment.utc(timestamp);
            } else {
                momentObj = moment.utc();
            }

            if (!momentObj.isValid()) {
                momentObj = moment.utc();
            }

            return momentObj.format('YYYY-MM-DDTHH:mm:ss.SSS[Z]');

        } catch (error) {
            return moment.utc().format('YYYY-MM-DDTHH:mm:ss.SSS[Z]');
        }
    }

    /**
     * Convert timestamp from any format to Asia/Kolkata timezone
     * @param {string|Date} timestamp - Input timestamp
     * @returns {string} Asia/Kolkata timezone formatted timestamp
     */
    toKolkataTimezone(timestamp) {
        try {
            let momentObj;

            if (typeof timestamp === 'string') {
                // First try parsing with timezone info
                momentObj = moment.tz(timestamp, this.timezone);

                // If invalid, assume UTC and convert
                if (!momentObj.isValid()) {
                    momentObj = moment.utc(timestamp).tz(this.timezone);
                }
            } else if (timestamp instanceof Date) {
                momentObj = moment.tz(timestamp, this.timezone);
            } else {
                momentObj = moment.tz(this.timezone);
            }

            if (!momentObj.isValid()) {
                momentObj = moment.tz(this.timezone);
            }

            return momentObj.format('YYYY-MM-DDTHH:mm:ss.SSSZ');

        } catch (error) {
            return moment.tz(this.timezone).format('YYYY-MM-DDTHH:mm:ss.SSSZ');
        }
    }

    /**
     * Validate if timestamp is in correct ISO 8601 format
     * @param {string} timestamp - Timestamp to validate
     * @returns {boolean} True if valid ISO 8601 format
     */
    isValidISO8601(timestamp) {
        if (typeof timestamp !== 'string') return false;

        // ISO 8601 regex pattern
        const iso8601Regex = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?([+-]\d{2}:\d{2}|Z)$/;

        if (!iso8601Regex.test(timestamp)) return false;

        // Validate with moment
        const momentObj = moment(timestamp, moment.ISO_8601, true);
        return momentObj.isValid();
    }

    /**
     * Normalize timestamp to standard format (ISO 8601 with UTC+5:30)
     * @param {string|Date} timestamp - Input timestamp
     * @returns {Object} Normalized timestamp information
     */
    normalizeTimestamp(timestamp) {
        const iso8601WithTz = this.toISO8601WithTimezone(timestamp);
        const utcFormat = this.toUTC(timestamp);
        const kolkataFormat = this.toKolkataTimezone(timestamp);

        return {
            iso8601_with_timezone: iso8601WithTz,
            utc_format: utcFormat,
            kolkata_format: kolkataFormat,
            is_valid: this.isValidISO8601(iso8601WithTz),
            timezone_offset: this.iso8601Offset,
            timezone_name: this.timezone,
            timezone_display: this.displayName
        };
    }

    /**
     * Process timestamps for data transformation pipeline
     * @param {Array<Object>} data - Array of data records with timestamps
     * @returns {Array<Object>} Data with processed timestamps
     */
    processTimestampsForPipeline(data) {
        return data.map(row => {
            if (!row.timestamp) {
                return {
                    ...row,
                    timestamp: this.toISO8601WithTimezone(new Date()),
                    timestamp_processed: true,
                    timestamp_original: null
                };
            }

            const originalTimestamp = row.timestamp;
            const processedTimestamp = this.toISO8601WithTimezone(originalTimestamp);

            return {
                ...row,
                timestamp: processedTimestamp,
                timestamp_processed: true,
                timestamp_original: originalTimestamp
            };
        });
    }

    /**
     * Extract date components from timestamp
     * @param {string|Date} timestamp - Input timestamp
     * @returns {Object} Date components
     */
    extractDateComponents(timestamp) {
        const momentObj = moment.tz(timestamp, this.timezone);

        if (!momentObj.isValid()) {
            const now = moment.tz(this.timezone);
            return {
                year: now.year(),
                month: now.month() + 1, // moment months are 0-indexed
                day: now.date(),
                hour: now.hour(),
                minute: now.minute(),
                second: now.second(),
                dayOfWeek: now.day(),
                dayOfYear: now.dayOfYear(),
                weekOfYear: now.week(),
                quarterOfYear: now.quarter()
            };
        }

        return {
            year: momentObj.year(),
            month: momentObj.month() + 1,
            day: momentObj.date(),
            hour: momentObj.hour(),
            minute: momentObj.minute(),
            second: momentObj.second(),
            dayOfWeek: momentObj.day(),
            dayOfYear: momentObj.dayOfYear(),
            weekOfYear: momentObj.week(),
            quarterOfYear: momentObj.quarter()
        };
    }

    /**
     * Calculate time difference in various units
     * @param {string|Date} startTime - Start timestamp
     * @param {string|Date} endTime - End timestamp
     * @returns {Object} Time differences in various units
     */
    calculateTimeDifference(startTime, endTime) {
        const start = moment.tz(startTime, this.timezone);
        const end = moment.tz(endTime, this.timezone);

        if (!start.isValid() || !end.isValid()) {
            return {
                milliseconds: 0,
                seconds: 0,
                minutes: 0,
                hours: 0,
                days: 0,
                weeks: 0,
                months: 0,
                years: 0
            };
        }

        const duration = moment.duration(end.diff(start));

        return {
            milliseconds: duration.asMilliseconds(),
            seconds: duration.asSeconds(),
            minutes: duration.asMinutes(),
            hours: duration.asHours(),
            days: duration.asDays(),
            weeks: duration.asWeeks(),
            months: duration.asMonths(),
            years: duration.asYears()
        };
    }

    /**
     * Get current timestamp in various formats
     * @returns {Object} Current timestamp in multiple formats
     */
    getCurrentTimestamp() {
        const now = moment.tz(this.timezone);

        return {
            iso8601_with_timezone: now.format('YYYY-MM-DDTHH:mm:ss.SSSZ'),
            utc: now.utc().format('YYYY-MM-DDTHH:mm:ss.SSS[Z]'),
            kolkata: now.format('YYYY-MM-DDTHH:mm:ss.SSSZ'),
            unix_timestamp: now.unix(),
            unix_milliseconds: now.valueOf(),
            human_readable: now.format('YYYY-MM-DD HH:mm:ss [UTC+05:30]')
        };
    }
}

module.exports = new TimestampUtils();
