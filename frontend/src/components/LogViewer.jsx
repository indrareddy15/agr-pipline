import React, { useState, useEffect } from 'react';
import {
    Card,
    Table,
    Button,
    Select,
    InputNumber,
    Space,
    Typography,
    Alert,
    Spin,
    Modal,
    message,
    Tooltip,
    Tag,
    Row,
    Col,
    Statistic,
} from 'antd';
import {
    ReloadOutlined,
    DownloadOutlined,
    DeleteOutlined,
    EyeOutlined,
    FileTextOutlined,
} from '@ant-design/icons';
import { format } from 'date-fns';
import { apiService } from '../services/apiService';

const { Title, Text, Paragraph } = Typography;
const { Option } = Select;

const LogViewer = () => {
    const [logFiles, setLogFiles] = useState([]);
    const [selectedLog, setSelectedLog] = useState(null);
    const [logContent, setLogContent] = useState('');
    const [loading, setLoading] = useState(false);
    const [contentLoading, setContentLoading] = useState(false);
    const [lines, setLines] = useState(1000);
    const [logModalVisible, setLogModalVisible] = useState(false);
    const [error, setError] = useState(null);

    useEffect(() => {
        fetchLogFiles();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    const fetchLogFiles = async () => {
        setLoading(true);
        setError(null);
        try {
            const response = await apiService.getLogs();
            if (response.status === 'success' && response.data && Array.isArray(response.data.files)) {
                setLogFiles(response.data.files);
                if (response.data.files.length > 0 && !selectedLog) {
                    setSelectedLog(response.data.files[0].filename);
                }
            } else {
                // If response is not successful or data is not an array, set empty array
                setLogFiles([]);
                setError('Invalid log files data received');
            }
        } catch {
            setError('Failed to fetch log files');
            setLogFiles([]); // Ensure logFiles is always an array
            message.error('Failed to fetch log files');
        } finally {
            setLoading(false);
        }
    };

    const fetchLogContent = async (filename = selectedLog) => {
        if (!filename) return;

        setContentLoading(true);
        try {
            const response = await apiService.getLogContent(filename, lines);
            if (response.status === 'success') {
                setLogContent(response.data.content);
            }
        } catch {
            message.error('Failed to fetch log content');
            setLogContent('');
        } finally {
            setContentLoading(false);
        }
    };

    const handleDownloadLog = (filename) => {
        try {
            apiService.downloadLog(filename);
            message.success(`Downloading ${filename}...`);
        } catch {
            message.error('Failed to download log file');
        }
    };

    const handleClearLog = (filename) => {
        Modal.confirm({
            title: 'Clear Log File',
            content: `Are you sure you want to clear the log file "${filename}"? This action cannot be undone.`,
            okText: 'Clear',
            okType: 'danger',
            cancelText: 'Cancel',
            onOk: async () => {
                try {
                    await apiService.clearLog(filename);
                    message.success(`Log file ${filename} cleared successfully`);
                    fetchLogFiles();
                    if (filename === selectedLog) {
                        setLogContent('');
                    }
                } catch {
                    message.error('Failed to clear log file');
                }
            },
        });
    };

    const handleViewLog = (filename) => {
        setSelectedLog(filename);
        setLogModalVisible(true);
        fetchLogContent(filename);
    };

    const formatFileSize = (bytes) => {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    };

    const getLogLevel = (line) => {
        if (line.includes('[ERROR]')) return 'error';
        if (line.includes('[WARN]')) return 'warning';
        if (line.includes('[INFO]')) return 'info';
        return 'default';
    };

    const renderLogContent = () => {
        if (!logContent) return <Text type="secondary">No log content available</Text>;

        const lines = logContent.split('\n').filter(line => line.trim());

        return (
            <div style={{
                maxHeight: '400px',
                overflow: 'auto',
                backgroundColor: '#f5f5f5',
                padding: '12px',
                borderRadius: '6px',
                fontFamily: 'monospace',
                fontSize: '12px',
                lineHeight: '1.4'
            }}>
                {lines.map((line, index) => (
                    <div key={index} style={{ marginBottom: '2px' }}>
                        <Tag color={
                            getLogLevel(line) === 'error' ? 'red' :
                                getLogLevel(line) === 'warning' ? 'orange' :
                                    getLogLevel(line) === 'info' ? 'blue' : 'default'
                        } size="small" style={{ marginRight: '8px', minWidth: '50px', textAlign: 'center' }}>
                            {getLogLevel(line).toUpperCase()}
                        </Tag>
                        <Text style={{ color: '#333' }}>{line}</Text>
                    </div>
                ))}
            </div>
        );
    };

    const columns = [
        {
            title: 'Log File',
            dataIndex: 'filename',
            key: 'filename',
            render: (filename) => (
                <Space>
                    <FileTextOutlined />
                    <Text strong>{filename}</Text>
                </Space>
            ),
        },
        {
            title: 'Size',
            dataIndex: 'size',
            key: 'size',
            render: (size) => formatFileSize(size),
            sorter: (a, b) => a.size - b.size,
        },
        {
            title: 'Modified',
            dataIndex: 'modified',
            key: 'modified',
            render: (date) => format(new Date(date), 'MMM dd, yyyy HH:mm:ss'),
            sorter: (a, b) => new Date(a.modified) - new Date(b.modified),
        },
        {
            title: 'Actions',
            key: 'actions',
            render: (_, record) => (
                <Space>
                    <Tooltip title="View Log">
                        <Button
                            type="primary"
                            icon={<EyeOutlined />}
                            size="small"
                            onClick={() => handleViewLog(record.filename)}
                        />
                    </Tooltip>
                    <Tooltip title="Download Log">
                        <Button
                            icon={<DownloadOutlined />}
                            size="small"
                            onClick={() => handleDownloadLog(record.filename)}
                        />
                    </Tooltip>
                    <Tooltip title="Clear Log">
                        <Button
                            danger
                            icon={<DeleteOutlined />}
                            size="small"
                            onClick={() => handleClearLog(record.filename)}
                        />
                    </Tooltip>
                </Space>
            ),
        },
    ];

    const totalSize = Array.isArray(logFiles) ? logFiles.reduce((acc, file) => acc + file.size, 0) : 0;
    const totalFiles = Array.isArray(logFiles) ? logFiles.length : 0;

    return (
        <div className="space-y-6">
            <div>
                <Title level={2}>Log Viewer</Title>
                <Text type="secondary">
                    View, download, and manage Agr Pipeline log files
                </Text>
            </div>

            {error && (
                <Alert
                    message="Error"
                    description={error}
                    type="error"
                    showIcon
                    closable
                    onClose={() => setError(null)}
                />
            )}

            {/* Log Statistics */}
            <Row gutter={16}>
                <Col span={8}>
                    <Card>
                        <Statistic
                            title="Total Log Files"
                            value={totalFiles}
                            prefix={<FileTextOutlined />}
                        />
                    </Card>
                </Col>
                <Col span={8}>
                    <Card>
                        <Statistic
                            title="Total Size"
                            value={formatFileSize(totalSize)}
                            prefix={<FileTextOutlined />}
                        />
                    </Card>
                </Col>
                <Col span={8}>
                    <Card>
                        <Statistic
                            title="Latest Log"
                            value={Array.isArray(logFiles) && logFiles.length > 0 ? logFiles[0].filename : 'None'}
                            prefix={<FileTextOutlined />}
                        />
                    </Card>
                </Col>
            </Row>

            {/* Log Files Table */}
            <Card
                title="Log Files"
                extra={
                    <Button
                        type="primary"
                        icon={<ReloadOutlined />}
                        onClick={fetchLogFiles}
                        loading={loading}
                    >
                        Refresh
                    </Button>
                }
            >
                <Table
                    columns={columns}
                    dataSource={Array.isArray(logFiles) ? logFiles : []}
                    rowKey="filename"
                    loading={loading}
                    pagination={{
                        pageSize: 10,
                        showSizeChanger: true,
                        showQuickJumper: true,
                        showTotal: (total) => `Total ${total} log files`,
                    }}
                />
            </Card>

            {/* Log Content Modal */}
            <Modal
                title={
                    <Space>
                        <FileTextOutlined />
                        <Text>{selectedLog}</Text>
                    </Space>
                }
                open={logModalVisible}
                onCancel={() => setLogModalVisible(false)}
                width={1000}
                footer={[
                    <Space key="actions">
                        <Text>Lines to show:</Text>
                        <InputNumber
                            min={100}
                            max={10000}
                            value={lines}
                            onChange={setLines}
                            style={{ width: 100 }}
                        />
                        <Button
                            icon={<ReloadOutlined />}
                            onClick={() => fetchLogContent(selectedLog)}
                            loading={contentLoading}
                        >
                            Refresh
                        </Button>
                        <Button
                            type="primary"
                            icon={<DownloadOutlined />}
                            onClick={() => handleDownloadLog(selectedLog)}
                        >
                            Download
                        </Button>
                        <Button onClick={() => setLogModalVisible(false)}>
                            Close
                        </Button>
                    </Space>
                ]}
            >
                <div style={{ marginBottom: '16px' }}>
                    <Text type="secondary">
                        Showing last {lines} lines of {selectedLog}
                    </Text>
                </div>

                <Spin spinning={contentLoading}>
                    {renderLogContent()}
                </Spin>
            </Modal>
        </div>
    );
};

export default LogViewer;
