/**
 * Log Controller
 * Handles log retrieval and management operations
 */

const fs = require('fs').promises;
const path = require('path');
const { logging } = require('../utils');

class LogController {
    /**
     * Get list of available log files
     * GET /api/logs
     */
    static async getLogFiles(req, res) {
        try {
            const logsDir = path.join(__dirname, '../../data/logs');

            try {
                await fs.access(logsDir);
            } catch {
                // Create logs directory if it doesn't exist
                await fs.mkdir(logsDir, { recursive: true });
            }

            const files = await fs.readdir(logsDir);
            const logFiles = files.filter(file => file.endsWith('.log'));

            const fileDetails = await Promise.all(
                logFiles.map(async (file) => {
                    const filePath = path.join(logsDir, file);
                    const stats = await fs.stat(filePath);
                    return {
                        filename: file,
                        size: stats.size,
                        modified: stats.mtime.toISOString(),
                        created: stats.birthtime.toISOString()
                    };
                })
            );

            res.json({
                status: 'success',
                data: {
                    files: fileDetails,
                    count: fileDetails.length,
                    timestamp: new Date().toISOString()
                }
            });
        } catch (error) {
            logging.error(`Log files listing error: ${error.message}`);
            res.status(500).json({
                status: 'error',
                message: 'Failed to list log files',
                error: error.message
            });
        }
    }

    /**
     * Get content of a specific log file
     * GET /api/logs/:filename
     */
    static async getLogContent(req, res) {
        try {
            const { filename } = req.params;
            const { lines = 1000 } = req.query;
            const logsDir = path.join(__dirname, '../../data/logs');
            const logPath = path.join(logsDir, filename);

            // Validate filename to prevent directory traversal
            if (!filename.endsWith('.log') || filename.includes('..')) {
                return res.status(400).json({
                    status: 'error',
                    message: 'Invalid log filename'
                });
            }

            const content = await fs.readFile(logPath, 'utf8');
            const logLines = content.split('\n').filter(line => line.trim());
            const recentLines = logLines.slice(-parseInt(lines));

            res.json({
                status: 'success',
                data: {
                    filename,
                    content: recentLines.join('\n'),
                    lines: recentLines.length,
                    totalLines: logLines.length,
                    timestamp: new Date().toISOString()
                }
            });
        } catch (error) {
            if (error.code === 'ENOENT') {
                res.status(404).json({
                    status: 'error',
                    message: 'Log file not found'
                });
            } else {
                logging.error(`Log content error: ${error.message}`);
                res.status(500).json({
                    status: 'error',
                    message: 'Failed to read log file',
                    error: error.message
                });
            }
        }
    }

    /**
     * Download a specific log file
     * GET /api/logs/:filename/download
     */
    static async downloadLog(req, res) {
        try {
            const { filename } = req.params;
            const logsDir = path.join(__dirname, '../../data/logs');
            const logPath = path.join(logsDir, filename);

            // Validate filename
            if (!filename.endsWith('.log') || filename.includes('..')) {
                return res.status(400).json({
                    status: 'error',
                    message: 'Invalid log filename'
                });
            }

            // Check if file exists
            await fs.access(logPath);

            res.download(logPath, filename, (err) => {
                if (err) {
                    logging.error(`Log download error: ${err.message}`);
                    res.status(500).json({
                        status: 'error',
                        message: 'Failed to download log file',
                        error: err.message
                    });
                }
            });
        } catch (error) {
            if (error.code === 'ENOENT') {
                res.status(404).json({
                    status: 'error',
                    message: 'Log file not found'
                });
            } else {
                logging.error(`Log download error: ${error.message}`);
                res.status(500).json({
                    status: 'error',
                    message: 'Failed to download log file',
                    error: error.message
                });
            }
        }
    }

    /**
     * Clear a specific log file
     * DELETE /api/logs/:filename
     */
    static async clearLog(req, res) {
        try {
            const { filename } = req.params;
            const logsDir = path.join(__dirname, '../../data/logs');
            const logPath = path.join(logsDir, filename);

            // Validate filename
            if (!filename.endsWith('.log') || filename.includes('..')) {
                return res.status(400).json({
                    status: 'error',
                    message: 'Invalid log filename'
                });
            }

            // Clear the log file by writing empty content
            await fs.writeFile(logPath, '', 'utf8');

            logging.logSystem('INFO', `Log file cleared: ${filename}`);

            res.json({
                status: 'success',
                message: `Log file ${filename} cleared successfully`,
                data: {
                    filename,
                    timestamp: new Date().toISOString()
                }
            });
        } catch (error) {
            logging.error(`Log clearing error: ${error.message}`);
            res.status(500).json({
                status: 'error',
                message: 'Failed to clear log file',
                error: error.message
            });
        }
    }
}

module.exports = LogController;
