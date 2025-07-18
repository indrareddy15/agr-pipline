import axios from 'axios';

// Base API configuration
// In development and production, use relative URLs since backend proxies frontend
const BASE_URL = 'http://localhost:1508';
const API_BASE = `${BASE_URL}/api`;

// Create axios instance with default config
const apiClient = axios.create({
    baseURL: BASE_URL,
    timeout: 300000, // Increased to 5 minutes for large operations
    headers: {
        'Content-Type': 'application/json',
    },
});

// Request interceptor for logging
apiClient.interceptors.request.use(
    (config) => {
        return config;
    },
    (error) => {
        return Promise.reject(error);
    }
);

// Response interceptor for error handling
apiClient.interceptors.response.use(
    (response) => {
        return response;
    },
    (error) => {

        // Handle specific error cases
        if (error.code === 'ECONNREFUSED') {
            throw new Error('Unable to connect to server. Please ensure the backend is running.');
        }

        if (error.code === 'ECONNABORTED') {
            throw new Error('Request timeout. The file might be too large or the server is busy. Please try again.');
        }

        if (error.response?.status === 404) {
            throw new Error('API endpoint not found.');
        }

        if (error.response?.status >= 500) {
            throw new Error('Server error. Please try again later.');
        }

        throw error;
    }
);

export const apiService = {
    // Health check
    async checkHealth() {
        const response = await apiClient.get('/health');
        return response.data;
    },

    // Get pipeline status
    async getPipelineStatus() {
        const response = await apiClient.get('/api/status');
        return response.data;
    },

    // Upload files with 4-step pipeline processing
    async uploadFiles(formDataOrFiles, options = {}) {
        let formData;

        // Check if it's already FormData or needs to be created
        if (formDataOrFiles instanceof FormData) {
            formData = formDataOrFiles;
        } else {
            formData = new FormData();
            // Add files to form data
            formDataOrFiles.forEach((file) => {
                formData.append('files', file);
            });
        }

        // Add processing option (default to true for automatic processing)
        const shouldProcess = options.process !== false;
        formData.append('process', shouldProcess.toString());

        // Add optional parameters
        if (options.generateReport !== undefined) {
            formData.append('generateReport', options.generateReport.toString());
        }

        if (options.timestamp) {
            formData.append('timestamp', options.timestamp);
        }

        if (options.filename) {
            formData.append('filename', options.filename);
        }

        const response = await apiClient.post('/api/upload', formData, {
            headers: {
                'Content-Type': 'multipart/form-data',
            },
            timeout: 600000, // 10 minutes for file uploads and processing
            onUploadProgress: options.onProgress,
            validateStatus: function (status) {
                return status < 500; // Resolve only if the status code is less than 500
            }
        });

        return response.data;
    },

    // Pipeline step processing
    async validateFiles(files) {
        const formData = new FormData();
        files.forEach((file) => {
            formData.append('files', file);
        });

        const response = await apiClient.post('/api/pipeline/validate', formData, {
            headers: {
                'Content-Type': 'multipart/form-data',
            },
            timeout: 300000, // 5 minutes
        });

        return response.data;
    },

    async processStep(step, fileList) {
        const response = await apiClient.post('/api/pipeline/process-step', {
            step,
            filenames: fileList  // Backend expects 'filenames', not 'fileList'
        }, {
            timeout: 600000, // 10 minutes for processing steps
        });

        return response.data;
    },

    // Process existing files
    async processFiles(options = {}) {
        const response = await apiClient.post('/api/process-files', {
            forceReprocess: options.forceReprocess || false,
            generateReport: options.generateReport !== undefined ? options.generateReport : true,
        });

        return response.data;
    },

    // Get processed data
    async getProcessedData(filters = {}) {
        const params = new URLSearchParams();

        Object.entries(filters).forEach(([key, value]) => {
            if (value !== undefined && value !== null && value !== '') {
                params.append(key, value.toString());
            }
        });

        const response = await apiClient.get(`/api/data?${params.toString()}`);
        return response.data;
    },

    // Get data quality report
    async getQualityReport() {
        const response = await apiClient.get('/api/quality-report');
        return response.data;
    },

    // Get checkpoints
    async getCheckpoints() {
        const response = await apiClient.get('/api/checkpoints');
        return response.data;
    },

    // Clear checkpoints
    async clearCheckpoints() {
        const response = await apiClient.delete('/api/checkpoints');
        return response.data;
    },

    // Reset pipeline
    async resetPipeline() {
        const response = await apiClient.delete('/api/reset');
        return response.data;
    },

    // Get data with advanced filtering
    async getFilteredData(filters) {
        const {
            sensor_id,
            date_from,
            date_to,
            reading_type,
            limit = 1000,
            offset = 0,
        } = filters;

        const params = new URLSearchParams();

        if (sensor_id) params.append('sensor_id', sensor_id);
        if (date_from) params.append('date_from', date_from);
        if (date_to) params.append('date_to', date_to);
        if (reading_type) params.append('reading_type', reading_type);
        params.append('limit', limit.toString());
        params.append('offset', offset.toString());

        const response = await apiClient.get(`/api/data?${params.toString()}`);
        return response.data;
    },

    // Get data for analytics (summary statistics)
    async getAnalyticsData(timeRange = 'week') {
        const endDate = new Date();
        let startDate = new Date();

        switch (timeRange) {
            case 'day':
                startDate.setDate(endDate.getDate() - 1);
                break;
            case 'week':
                startDate.setDate(endDate.getDate() - 7);
                break;
            case 'month':
                startDate.setMonth(endDate.getMonth() - 1);
                break;
            case 'year':
                startDate.setFullYear(endDate.getFullYear() - 1);
                break;
            default:
                startDate.setDate(endDate.getDate() - 7);
        }

        const response = await this.getFilteredData({
            date_from: startDate.toISOString().split('T')[0],
            date_to: endDate.toISOString().split('T')[0],
            limit: 10000,
        });

        return response;
    },

    // Get unique sensor IDs
    async getSensorIds() {
        try {
            const response = await apiClient.get('/api/metadata/sensor-ids');
            return response.data.data || [];
        } catch {
            return [];
        }
    },

    // Get unique reading types
    async getReadingTypes() {
        try {
            const response = await apiClient.get('/api/metadata/reading-types');
            return response.data.data || [];
        } catch {
            return [];
        }
    },

    // Get recent sensor data for analytics
    async getRecentData(hours = 24, limit = 1000) {
        const response = await apiClient.get(`/api/data/recent?hours=${hours}&limit=${limit}`);
        return response.data;
    },

    // Get data summary for analytics
    async getDataSummary(dateStart = null, dateEnd = null) {
        let url = '/api/data/summary';
        const params = new URLSearchParams();
        if (dateStart) params.append('date_start', dateStart);
        if (dateEnd) params.append('date_end', dateEnd);
        if (params.toString()) url += `?${params.toString()}`;

        const response = await apiClient.get(url);
        return response.data;
    },

    // Get raw sensor data with filtering
    async getSensorData(filters = {}) {
        const params = new URLSearchParams();
        Object.entries(filters).forEach(([key, value]) => {
            if (value !== null && value !== undefined && value !== '') {
                params.append(key, value);
            }
        });

        const url = `/api/data${params.toString() ? '?' + params.toString() : ''}`;
        const response = await apiClient.get(url);
        return response.data;
    },

    // Log Management
    getLogs: async () => {
        const response = await apiClient.get('/api/logs');
        return response.data;
    },

    getLogContent: async (filename, lines = 1000) => {
        const response = await apiClient.get(`/api/logs/${filename}?lines=${lines}`);
        return response.data;
    },

    downloadLog: (filename) => {
        const url = `${BASE_URL}/api/logs/${filename}/download`;
        window.open(url, '_blank');
    },

    clearLog: async (filename) => {
        const response = await apiClient.delete(`/api/logs/${filename}`);
        return response.data;
    },

    // Export data with analytical optimizations
    exportData: async (options = {}) => {
        const response = await apiClient.post('/api/data/export', options);
        return response.data;
    },

    // Download data with specified format and compression
    downloadData: async (format, options = {}) => {
        const params = new URLSearchParams(options).toString();
        const url = `/api/data/download/${format}${params ? '?' + params : ''}`;

        // Create download link
        const link = document.createElement('a');
        link.href = url;
        link.download = '';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    },

    // Get available export formats and compression options
    getExportOptions: async () => {
        return {
            formats: [
                { value: 'json', label: 'JSON', description: 'JavaScript Object Notation' },
                { value: 'csv', label: 'CSV', description: 'Comma Separated Values' },
                { value: 'parquet', label: 'Parquet', description: 'Columnar storage format' }
            ],
            compressions: [
                { value: 'none', label: 'None', description: 'No compression' },
                { value: 'gzip', label: 'GZIP', description: 'GNU Zip compression' },
                { value: 'snappy', label: 'Snappy', description: 'Fast compression (Parquet only)' }
            ],
            partitions: [
                { value: 'none', label: 'None', description: 'No partitioning' },
                { value: 'date', label: 'By Date', description: 'Partition by date' },
                { value: 'sensor_id', label: 'By Sensor ID', description: 'Partition by sensor' },
                { value: 'both', label: 'Date + Sensor', description: 'Partition by both date and sensor' }
            ]
        };
    },
};

// Export individual functions for specific use cases
export const {
    checkHealth,
    getPipelineStatus,
    uploadFiles,
    processFiles,
    getProcessedData,
    getQualityReport,
    getCheckpoints,
    clearCheckpoints,
    resetPipeline,
    getFilteredData,
    getAnalyticsData,
    getSensorIds,
    getReadingTypes,
    getRecentData,
    getDataSummary,
    getSensorData,
    getLogs,
    getLogContent,
    downloadLog,
    clearLog,
    exportData,
    downloadData,
    getExportOptions,
} = apiService;

export default apiService;
