import React, { useState, useEffect } from 'react';
import {
    Card,
    Row,
    Col,
    Button,
    Switch,
    Input,
    Select,
    Typography,
    Space,
    Divider,
    Alert,
    message,
    Modal,
    Form,
    InputNumber,
    Tag,
    Statistic,
    Progress,
    List,
    Tooltip,
    Descriptions,
} from 'antd';
import {
    SettingOutlined,
    ReloadOutlined,
    DeleteOutlined,
    ExclamationCircleOutlined,
    CheckCircleOutlined,
    InfoCircleOutlined,
    DatabaseOutlined,
    CloudServerOutlined,
    ClockCircleOutlined,
    FileTextOutlined,
    WarningOutlined,
} from '@ant-design/icons';
import { apiService } from '../services/apiService';

const { Title, Text } = Typography;
const { Option } = Select;
const { confirm } = Modal;

const Settings = ({ onHealthCheck }) => {
    const [loading, setLoading] = useState(false);
    const [systemStatus, setSystemStatus] = useState(null);
    const [pipelineStatus, setPipelineStatus] = useState(null);
    const [dataStats, setDataStats] = useState(null);
    const [settings, setSettings] = useState({
        autoRefresh: true,
        refreshInterval: 30,
        maxRecords: 1000,
        defaultTimeRange: 'week',
        enableNotifications: true,
        theme: 'light',
    });

    useEffect(() => {
        loadAllSystemData();
        loadSettings();

        // Auto-refresh system data every 30 seconds
        const interval = setInterval(loadAllSystemData, 30000);
        return () => clearInterval(interval);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    const loadAllSystemData = async () => {
        await Promise.all([
            checkSystemStatus(),
            loadPipelineStatus(),
            loadDataStatistics(),
        ]);
    };

    const checkSystemStatus = async () => {
        try {
            setLoading(true);
            const [health, status, checkpoints] = await Promise.all([
                apiService.checkHealth(),
                apiService.getPipelineStatus(),
                apiService.getCheckpoints(),
            ]);

            setSystemStatus({
                health,
                pipeline: status,
                checkpoints,
            });
        } catch {
            // Failed to load system status
        } finally {
            setLoading(false);
        }
    };

    const loadPipelineStatus = async () => {
        try {
            const status = await apiService.getPipelineStatus();
            setPipelineStatus(status);
        } catch {
            // Error is handled by UI state
        }
    };

    const loadDataStatistics = async () => {
        try {
            const [summary, recentData] = await Promise.all([
                apiService.getDataSummary(),
                apiService.getRecentData(24, 10), // Last 24 hours, 10 records
            ]);

            setDataStats({
                summary: summary.data?.summary || {},
                recentCount: recentData.data?.count || 0,
                lastUpdate: recentData.data?.timestamp || null,
            });
        } catch {
            setDataStats({
                summary: {},
                recentCount: 0,
                lastUpdate: null,
            });
        }
    };

    const loadSettings = () => {
        // Load settings from localStorage
        const savedSettings = localStorage.getItem('etl-frontend-settings');
        if (savedSettings) {
            try {
                const parsed = JSON.parse(savedSettings);
                setSettings({ ...settings, ...parsed });
            } catch {
                // Failed to load settings
            }
        }
    };

    const saveSettings = (newSettings) => {
        const updatedSettings = { ...settings, ...newSettings };
        setSettings(updatedSettings);
        localStorage.setItem('etl-frontend-settings', JSON.stringify(updatedSettings));
        message.success('Settings saved successfully');
    };

    const handleClearCheckpoints = () => {
        confirm({
            title: 'Clear Checkpoints',
            icon: <ExclamationCircleOutlined />,
            content: 'This will clear all processing checkpoints and allow full reprocessing of files. Are you sure?',
            okText: 'Clear',
            okType: 'danger',
            cancelText: 'Cancel',
            onOk: async () => {
                try {
                    setLoading(true);
                    await apiService.clearCheckpoints();
                    message.success('Checkpoints cleared successfully');
                    await checkSystemStatus();
                } catch (error) {
                    message.error(`Failed to clear checkpoints: ${error.message}`);
                } finally {
                    setLoading(false);
                }
            },
        });
    };

    const handleResetPipeline = () => {
        confirm({
            title: 'Reset Pipeline',
            icon: <ExclamationCircleOutlined />,
            content: (
                <div>
                    <Alert
                        message="Warning: This action is irreversible!"
                        description="This will delete all processed data and reset the entire pipeline. Use with extreme caution."
                        type="error"
                        className="mb-4"
                    />
                    <Text>Are you absolutely sure you want to reset the pipeline?</Text>
                </div>
            ),
            okText: 'Reset Pipeline',
            okType: 'danger',
            cancelText: 'Cancel',
            onOk: async () => {
                try {
                    setLoading(true);
                    await apiService.resetPipeline();
                    message.success('Pipeline reset successfully');
                    await checkSystemStatus();
                } catch (error) {
                    message.error(`Failed to reset pipeline: ${error.message}`);
                } finally {
                    setLoading(false);
                }
            },
        });
    };

    const handleProcessFiles = async () => {
        try {
            setLoading(true);
            await apiService.processFiles({
                forceReprocess: false,
                generateReport: true
            });
            message.success('File processing started successfully');
            await checkSystemStatus();
        } catch (error) {
            message.error(`Failed to start processing: ${error.message}`);
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="space-y-6">
            <div className="flex justify-between items-center">
                <div>
                    <Title level={2}>Agricultural Pipeline Settings & Monitoring</Title>
                    <Text type="secondary">
                        Configure system settings, monitor pipeline health, and view agricultural sensor data processing statistics
                    </Text>
                </div>
                <Space>
                    <Button
                        icon={<ReloadOutlined />}
                        onClick={loadAllSystemData}
                        loading={loading}
                    >
                        Refresh All
                    </Button>
                    <Button
                        type="primary"
                        icon={<SettingOutlined />}
                        onClick={checkSystemStatus}
                        loading={loading}
                    >
                        System Check
                    </Button>
                </Space>
            </div>

            {/* System Status */}
            <Card title="System Status" className="mb-6">
                <Row gutter={[16, 16]}>
                    <Col xs={24} md={8}>
                        <div className="text-center p-4 bg-gray-50 rounded">
                            <div className={`w-12 h-12 rounded-full mx-auto mb-2 flex items-center justify-center ${systemStatus?.health?.status === 'healthy' ? 'bg-green-100' : 'bg-red-100'
                                }`}>
                                {systemStatus?.health?.status === 'healthy' ?
                                    <CheckCircleOutlined className="text-2xl text-green-600" /> :
                                    <ExclamationCircleOutlined className="text-2xl text-red-600" />
                                }
                            </div>
                            <Text strong>Data Processing Service</Text>
                            <div className="mt-1">
                                <Tag color={systemStatus?.health?.status === 'healthy' ? 'green' : 'red'}>
                                    {systemStatus?.health?.status === 'healthy' ? 'Online' : 'Offline'}
                                </Tag>
                            </div>
                        </div>
                    </Col>

                    <Col xs={24} md={8}>
                        <div className="text-center p-4 bg-gray-50 rounded">
                            <div className="w-12 h-12 rounded-full mx-auto mb-2 flex items-center justify-center bg-blue-100">
                                <InfoCircleOutlined className="text-2xl text-blue-600" />
                            </div>
                            <Text strong>Files Processed</Text>
                            <div className="mt-1">
                                <Text className="text-lg">
                                    {systemStatus?.pipeline?.data?.stats?.filesProcessed || 0}
                                </Text>
                            </div>
                        </div>
                    </Col>

                    <Col xs={24} md={8}>
                        <div className="text-center p-4 bg-gray-50 rounded">
                            <div className="w-12 h-12 rounded-full mx-auto mb-2 flex items-center justify-center bg-green-100">
                                <CheckCircleOutlined className="text-2xl text-green-600" />
                            </div>
                            <Text strong>Records Ingested</Text>
                            <div className="mt-1">
                                <Text className="text-lg">
                                    {systemStatus?.pipeline?.data?.stats?.recordsIngested || 0}
                                </Text>
                            </div>
                        </div>
                    </Col>
                </Row>
            </Card>

            {/* Real-time Data Statistics */}
            <Row gutter={[16, 16]}>
                <Col xs={24} lg={12}>
                    <Card title="Data Statistics" extra={
                        <Tooltip title="Real-time data statistics">
                            <DatabaseOutlined />
                        </Tooltip>
                    }>
                        {dataStats ? (
                            <Row gutter={[16, 16]}>
                                <Col xs={12}>
                                    <Statistic
                                        title="Total Records"
                                        value={dataStats.summary?.total_records || 0}
                                        prefix={<FileTextOutlined />}
                                    />
                                </Col>
                                <Col xs={12}>
                                    <Statistic
                                        title="Recent (24h)"
                                        value={dataStats.recentCount}
                                        prefix={<ClockCircleOutlined />}
                                        valueStyle={{ color: '#52c41a' }}
                                    />
                                </Col>
                                <Col xs={12}>
                                    <Statistic
                                        title="Quality Score"
                                        value={dataStats.summary?.quality_score || 0}
                                        suffix="%"
                                        prefix={<CheckCircleOutlined />}
                                        valueStyle={{
                                            color: (dataStats.summary?.quality_score || 0) >= 90 ? '#52c41a' :
                                                (dataStats.summary?.quality_score || 0) >= 70 ? '#faad14' : '#ff4d4f'
                                        }}
                                    />
                                </Col>
                                <Col xs={12}>
                                    <Statistic
                                        title="Issues"
                                        value={dataStats.summary?.total_issues || 0}
                                        prefix={<WarningOutlined />}
                                        valueStyle={{
                                            color: (dataStats.summary?.total_issues || 0) > 0 ? '#ff4d4f' : '#52c41a'
                                        }}
                                    />
                                </Col>
                            </Row>
                        ) : (
                            <Text type="secondary">Loading data statistics...</Text>
                        )}
                    </Card>
                </Col>
                <Col xs={24} lg={12}>
                    <Card title="Pipeline Health" extra={
                        <Tooltip title="Real-time pipeline status">
                            <CloudServerOutlined />
                        </Tooltip>
                    }>
                        {pipelineStatus ? (
                            <div className="space-y-4">
                                <div className="flex justify-between items-center">
                                    <Text>Pipeline Status:</Text>
                                    <Tag color={pipelineStatus.status === 'success' ? 'green' : 'red'}>
                                        {pipelineStatus.status === 'success' ? 'Operational' : 'Error'}
                                    </Tag>
                                </div>
                                <div className="flex justify-between items-center">
                                    <Text>Processing Speed:</Text>
                                    <Text strong>
                                        {pipelineStatus.data?.stats?.processingTime || 0}ms
                                    </Text>
                                </div>
                                <div className="flex justify-between items-center">
                                    <Text>Success Rate:</Text>
                                    <Progress
                                        percent={
                                            pipelineStatus.data?.stats?.successRate || 100
                                        }
                                        size="small"
                                        style={{ width: '60px' }}
                                    />
                                </div>
                                <div className="flex justify-between items-center">
                                    <Text>Last Activity:</Text>
                                    <Text type="secondary">
                                        {pipelineStatus.data?.stats?.lastUpdated
                                            ? new Date(pipelineStatus.data.stats.lastUpdated).toLocaleTimeString()
                                            : 'Never'
                                        }
                                    </Text>
                                </div>
                            </div>
                        ) : (
                            <Text type="secondary">Loading pipeline status...</Text>
                        )}
                    </Card>
                </Col>
            </Row>

            {/* Data Directory Information */}
            <Card title="Data Directory Paths & Analytical Features">
                <Descriptions bordered size="small" column={2}>
                    <Descriptions.Item label="Expected Data Schema">
                        <Text code>sensor_id, timestamp, reading_type, value, battery_level</Text>
                    </Descriptions.Item>
                    <Descriptions.Item label="Sample Data Types">
                        <Text code>Agricultural, Environmental, Equipment monitoring</Text>
                    </Descriptions.Item>
                    <Descriptions.Item label="Raw Data Directory">
                        <Text code>data/raw/</Text>
                    </Descriptions.Item>
                    <Descriptions.Item label="Processed Data Directory">
                        <Text code>data/processed/</Text>
                    </Descriptions.Item>
                    <Descriptions.Item label="Checkpoints Directory">
                        <Text code>data/checkpoints/</Text>
                    </Descriptions.Item>
                    <Descriptions.Item label="Quality Reports">
                        <Text code>data_quality_report.csv</Text>
                    </Descriptions.Item>
                    <Descriptions.Item label="Pipeline Features" span={2}>
                        <Space wrap>
                            <Tag color="blue">DuckDB Validation</Tag>
                            <Tag color="green">Outlier Detection (z-score &gt; 3)</Tag>
                            <Tag color="purple">7-day Rolling Averages</Tag>
                            <Tag color="orange">Calibration & Normalization</Tag>
                        </Space>
                    </Descriptions.Item>
                    <Descriptions.Item label="Storage Optimization" span={2}>
                        <Space wrap>
                            <Tag color="cyan">Partitioned by Date</Tag>
                            <Tag color="geekblue">Columnar Format</Tag>
                            <Tag color="volcano">Snappy Compression</Tag>
                        </Space>
                    </Descriptions.Item>
                </Descriptions>
            </Card>

            {/* Application Settings */}
            <Card title="Application Settings">
                <Row gutter={[16, 24]}>
                    <Col xs={24} md={12}>
                        <Space direction="vertical" className="w-full">
                            <div className="flex justify-between items-center">
                                <Text strong>Auto-refresh Dashboard</Text>
                                <Switch
                                    checked={settings.autoRefresh}
                                    onChange={(checked) => saveSettings({ autoRefresh: checked })}
                                />
                            </div>
                            <Text type="secondary" className="text-sm">
                                Automatically refresh dashboard data every few seconds
                            </Text>
                        </Space>
                    </Col>

                    <Col xs={24} md={12}>
                        <Space direction="vertical" className="w-full">
                            <Text strong>Refresh Interval (seconds)</Text>
                            <InputNumber
                                value={settings.refreshInterval}
                                onChange={(value) => saveSettings({ refreshInterval: value })}
                                min={10}
                                max={300}
                                className="w-full"
                                disabled={!settings.autoRefresh}
                            />
                            <Text type="secondary" className="text-sm">
                                How often to refresh data when auto-refresh is enabled
                            </Text>
                        </Space>
                    </Col>

                    <Col xs={24} md={12}>
                        <Space direction="vertical" className="w-full">
                            <Text strong>Default Time Range</Text>
                            <Select
                                value={settings.defaultTimeRange}
                                onChange={(value) => saveSettings({ defaultTimeRange: value })}
                                className="w-full"
                            >
                                <Option value="day">Last 24 Hours</Option>
                                <Option value="week">Last 7 Days</Option>
                                <Option value="month">Last 30 Days</Option>
                                <Option value="year">Last Year</Option>
                            </Select>
                            <Text type="secondary" className="text-sm">
                                Default time range for analytics and data views
                            </Text>
                        </Space>
                    </Col>

                    <Col xs={24} md={12}>
                        <Space direction="vertical" className="w-full">
                            <Text strong>Max Records per Page</Text>
                            <Select
                                value={settings.maxRecords}
                                onChange={(value) => saveSettings({ maxRecords: value })}
                                className="w-full"
                            >
                                <Option value={50}>50</Option>
                                <Option value={100}>100</Option>
                                <Option value={200}>200</Option>
                                <Option value={500}>500</Option>
                                <Option value={1000}>1000</Option>
                            </Select>
                            <Text type="secondary" className="text-sm">
                                Maximum number of records to display per page
                            </Text>
                        </Space>
                    </Col>
                </Row>
            </Card>

            {/* Pipeline Operations */}
            <Card title="Pipeline Operations">
                <Alert
                    message="Pipeline Management"
                    description="Use these controls to manage your Agr pipeline operations. Be careful with destructive operations."
                    type="info"
                    showIcon
                    className="mb-4"
                />

                <Row gutter={[16, 16]}>
                    <Col xs={24} sm={8}>
                        <Button
                            type="primary"
                            icon={<SettingOutlined />}
                            onClick={handleProcessFiles}
                            loading={loading}
                            block
                            size="large"
                        >
                            Process Files
                        </Button>
                        <Text type="secondary" className="text-xs mt-1 block">
                            Process new files in the raw data directory
                        </Text>
                    </Col>

                    <Col xs={24} sm={8}>
                        <Button
                            icon={<ReloadOutlined />}
                            onClick={onHealthCheck}
                            loading={loading}
                            block
                            size="large"
                        >
                            Health Check
                        </Button>
                        <Text type="secondary" className="text-xs mt-1 block">
                            Check data processing system status
                        </Text>
                    </Col>

                    <Col xs={24} sm={8}>
                        <Button
                            icon={<DeleteOutlined />}
                            onClick={handleClearCheckpoints}
                            loading={loading}
                            block
                            size="large"
                        >
                            Clear Checkpoints
                        </Button>
                        <Text type="secondary" className="text-xs mt-1 block">
                            Clear processing checkpoints for full reprocessing
                        </Text>
                    </Col>
                </Row>

                <Divider />

                <Row>
                    <Col span={24}>
                        <Alert
                            message="Danger Zone"
                            description="The following operations are irreversible and will destroy data."
                            type="error"
                            className="mb-4"
                        />
                        <Button
                            danger
                            icon={<DeleteOutlined />}
                            onClick={handleResetPipeline}
                            loading={loading}
                            size="large"
                        >
                            Reset Entire Pipeline
                        </Button>
                        <Text type="secondary" className="text-xs mt-1 block">
                            ⚠️ This will delete all processed data and reset the pipeline
                        </Text>
                    </Col>
                </Row>
            </Card>

            {/* Checkpoint Information */}
            {systemStatus?.checkpoints && (
                <Card title="Checkpoint Information">
                    <Row gutter={[16, 16]}>
                        <Col xs={24} md={12}>
                            <Space direction="vertical" className="w-full">
                                <Text strong>Processed Files</Text>
                                <Text>{systemStatus.checkpoints.data?.processedFiles?.length || 0}</Text>
                            </Space>
                        </Col>
                        <Col xs={24} md={12}>
                            <Space direction="vertical" className="w-full">
                                <Text strong>Last Processing Time</Text>
                                <Text>
                                    {systemStatus.checkpoints.data?.stats?.processingTime || 0}ms
                                </Text>
                            </Space>
                        </Col>
                    </Row>

                    {systemStatus.checkpoints.data?.processedFiles?.length > 0 && (
                        <div className="mt-4">
                            <Text strong>Recently Processed Files:</Text>
                            <div className="mt-2 space-y-1">
                                {systemStatus.checkpoints.data.processedFiles.slice(0, 5).map((file, index) => (
                                    <Tag key={index} color="green">{file}</Tag>
                                ))}
                                {systemStatus.checkpoints.data.processedFiles.length > 5 && (
                                    <Tag>+{systemStatus.checkpoints.data.processedFiles.length - 5} more</Tag>
                                )}
                            </div>
                        </div>
                    )}
                </Card>
            )}

            {/* Data Processing Information */}
            <Card title="Data Processing Status">
                <Row gutter={[16, 16]}>
                    <Col xs={24} md={8}>
                        <Text strong>Total Files Processed:</Text>
                        <div>{systemStatus?.checkpoints?.data?.totalProcessed || 0}</div>
                    </Col>
                    <Col xs={24} md={8}>
                        <Text strong>Processing Status:</Text>
                        <div>{pipelineStatus?.data?.isRunning ? 'Active' : 'Idle'}</div>
                    </Col>
                    <Col xs={24} md={8}>
                        <Text strong>Last Processing Time:</Text>
                        <div>{systemStatus?.checkpoints?.data?.timestamp ? new Date(systemStatus.checkpoints.data.timestamp).toLocaleString() : 'Never'}</div>
                    </Col>
                </Row>
            </Card>
        </div>
    );
};

export default Settings;
