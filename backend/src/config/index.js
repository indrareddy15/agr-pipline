/**
 * Configuration index file
 * Exports all configuration modules
 */

const appConfig = require('./app');
const databaseConfig = require('./database');

module.exports = {
    app: appConfig,
    database: databaseConfig
};
