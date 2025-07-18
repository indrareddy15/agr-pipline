const duckdb = require('duckdb');
const path = require('path');

/**
 * Database configuration and connection utilities
 */
class DatabaseConfig {
    constructor () {
        this.connectionOptions = {
            // DuckDB connection options can be added here
        };
        this.persistentDb = null;
        this.persistentConnection = null;
    }

    /**
     * Create a new in-memory DuckDB database instance
     * @returns {Database} DuckDB database instance
     */
    createInMemoryDatabase() {
        return new duckdb.Database(':memory:');
    }

    /**
     * Create a persistent DuckDB database instance
     * @param {string} filepath - Path to the database file
     * @returns {Database} DuckDB database instance
     */
    createPersistentDatabase(filepath) {
        return new duckdb.Database(filepath);
    }

    /**
     * Get or create persistent database connection for data storage
     * @returns {Object} { db, connection }
     */
    async getPersistentConnection() {
        if (!this.persistentDb || !this.persistentConnection) {
            const dbPath = path.join(process.cwd(), 'data', 'pipeline.duckdb');
            this.persistentDb = new duckdb.Database(dbPath);
            this.persistentConnection = this.persistentDb.connect();

            // Initialize tables if they don't exist
            await this.initializeTables();
        }

        return {
            db: this.persistentDb,
            connection: this.persistentConnection
        };
    }

    /**
     * Initialize required tables in the persistent database
     */
    async initializeTables() {
        if (!this.persistentConnection) return;

        const createTableQuery = `
            CREATE TABLE IF NOT EXISTS sensor_data (
                sensor_id VARCHAR,
                timestamp TIMESTAMP,
                reading_type VARCHAR,
                value DOUBLE,
                battery_level DOUBLE,
                anomalous_reading BOOLEAN DEFAULT false,
                daily_avg DOUBLE,
                rolling_avg_7d DOUBLE,
                processed_date DATE DEFAULT CURRENT_DATE,
                partition_date VARCHAR
            )
        `;

        return new Promise((resolve, reject) => {
            this.persistentConnection.run(createTableQuery, (err) => {
                if (err) reject(err);
                else resolve();
            });
        });
    }

    /**
     * Get database connection
     * @param {Database} db - DuckDB database instance
     * @returns {Connection} Database connection
     */
    getConnection(db) {
        return db.connect();
    }

    /**
     * Execute query with promise wrapper
     * @param {Connection} connection - Database connection
     * @param {string} query - SQL query to execute
     * @param {Array} params - Query parameters
     * @returns {Promise} Query results
     */
    async executeQuery(connection, query, params = []) {
        return new Promise((resolve, reject) => {
            connection.all(query, params, (err, result) => {
                if (err) reject(err);
                else resolve(result);
            });
        });
    }

    /**
     * Close database connection safely
     * @param {Connection} connection - Database connection to close
     */
    async closeConnection(connection) {
        try {
            if (connection && typeof connection.close === 'function') {
                await new Promise((resolve) => {
                    connection.close((err) => {
                        if (err) console.error('Error closing connection:', err);
                        resolve();
                    });
                });
            }
        } catch (error) {
            console.error('Error closing database connection:', error);
        }
    }
}

module.exports = new DatabaseConfig();
