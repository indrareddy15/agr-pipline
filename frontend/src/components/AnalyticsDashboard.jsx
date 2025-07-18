import React, { useState, useEffect } from 'react';
import {
    Row,
    Col,
    Card,
    Statistic,
    Progress,
    List,
    Tag,
    Button,
    Alert,
    Spin,
    Typography,
    Space,
    Divider,
    Select,
} from 'antd';
import {
    FileTextOutlined,
    DatabaseOutlined,
    CheckCircleOutlined,
    ClockCircleOutlined,
    ExclamationCircleOutlined,
    ReloadOutlined,
    PlayCircleOutlined,
    BarChartOutlined,
    LineChartOutlined,
    PieChartOutlined,
} from '@ant-design/icons';
import {
    Chart as ChartJS,
    CategoryScale,
    LinearScale,
    PointElement,
    LineElement,
    BarElement,
    ArcElement,
    Title as ChartTitle,
    Tooltip,
    Legend,
    Filler,
} from 'chart.js';
import { Line, Bar, Doughnut } from 'react-chartjs-2';
import { format, parseISO } from 'date-fns';
import { apiService } from '../services/apiService';

// Register Chart.js components
ChartJS.register(
    CategoryScale,
    LinearScale,
    PointElement,
    LineElement,
    BarElement,
    ArcElement,
    ChartTitle,
    Tooltip,
    Legend,
    Filler
);

const { Option } = Select;
const { Title, Text } = Typography;

const AnalyticsDashboard = ({ systemHealth }) => {
    // Dashboard state
    const [pipelineStatus, setPipelineStatus] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [lastUpdated, setLastUpdated] = useState(null);

    // Analytics state
    const [analyticsData, setAnalyticsData] = useState([]);
    const [chartType, setChartType] = useState('line');
    const [analyticsLoading, setAnalyticsLoading] = useState(false);
    const [totalRecordsInSystem, setTotalRecordsInSystem] = useState(0);

    useEffect(() => {
        const loadData = async () => {
            await fetchDashboardData();
            await generateSampleAnalytics();
            await fetchTotalRecordsCount();
        };
        loadData();

        // Auto-refresh every 30 seconds
        const interval = setInterval(async () => {
            await fetchDashboardData();
            await generateSampleAnalytics();
            await fetchTotalRecordsCount();
        }, 30000);
        return () => clearInterval(interval);
    }, []);

    const fetchDashboardData = async () => {
        try {
            setLoading(true);
            setError(null);

            const status = await apiService.getPipelineStatus();
            setPipelineStatus(status);
            setLastUpdated(new Date());
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    const generateSampleAnalytics = async () => {
        setAnalyticsLoading(true);
        try {
            // Get real sensor data from the last 7 days for analytics
            const recentDataResponse = await apiService.getRecentData(168, 2000); // 7 days, max 2000 records

            if (recentDataResponse.status === 'success' && recentDataResponse.data.records.length > 0) {
                const records = recentDataResponse.data.records;

                // Group data by date and calculate averages
                const dataByDate = {};
                records.forEach(record => {
                    const date = new Date(record.timestamp).toLocaleDateString('en-US', {
                        month: 'short',
                        day: '2-digit'
                    });

                    if (!dataByDate[date]) {
                        dataByDate[date] = {
                            date,
                            temperature: [],
                            humidity: [],
                            soilMoisture: [],
                            lightIntensity: [],
                        };
                    }

                    // Map reading types to chart categories
                    switch (record.reading_type?.toLowerCase()) {
                        case 'temperature':
                            dataByDate[date].temperature.push(parseFloat(record.value) || 0);
                            break;
                        case 'humidity':
                            dataByDate[date].humidity.push(parseFloat(record.value) || 0);
                            break;
                        case 'soil_moisture':
                        case 'soil moisture':
                            dataByDate[date].soilMoisture.push(parseFloat(record.value) || 0);
                            break;
                        case 'light_intensity':
                        case 'light intensity':
                            dataByDate[date].lightIntensity.push(parseFloat(record.value) || 0);
                            break;
                        default:
                            // For unknown types, add to temperature as fallback
                            dataByDate[date].temperature.push(parseFloat(record.value) || 0);
                    }
                });

                // Calculate averages and prepare chart data
                const chartData = Object.values(dataByDate)
                    .slice(-7) // Take last 7 days
                    .map(dayData => ({
                        date: dayData.date,
                        temperature: dayData.temperature.length > 0
                            ? dayData.temperature.reduce((sum, val) => sum + val, 0) / dayData.temperature.length
                            : 0,
                        humidity: dayData.humidity.length > 0
                            ? dayData.humidity.reduce((sum, val) => sum + val, 0) / dayData.humidity.length
                            : 0,
                        soilMoisture: dayData.soilMoisture.length > 0
                            ? dayData.soilMoisture.reduce((sum, val) => sum + val, 0) / dayData.soilMoisture.length
                            : 0,
                        lightIntensity: dayData.lightIntensity.length > 0
                            ? dayData.lightIntensity.reduce((sum, val) => sum + val, 0) / dayData.lightIntensity.length
                            : 0,
                    }));

                setAnalyticsData(chartData.length > 0 ? chartData : []);
            } else {
                // If no real data available, show empty state
                setAnalyticsData([]);
            }
        } catch {
            // On error, show empty state instead of mock data
            setAnalyticsData([]);
        } finally {
            setAnalyticsLoading(false);
        }
    };

    const fetchTotalRecordsCount = async () => {
        try {
            // Get data summary to get total count from all uploaded files
            const summaryResponse = await apiService.getDataSummary();
            if (summaryResponse.status === 'success' && summaryResponse.data && summaryResponse.data.summary) {
                // Backend returns total_records in the summary object
                setTotalRecordsInSystem(summaryResponse.data.summary.total_records || 0);
            }
        } catch (err) {
            console.log('Failed to fetch total records count:', err.message);
        }
    };

    const handleProcessFiles = async () => {
        try {
            setLoading(true);
            await apiService.processFiles({ generateReport: true });
            await fetchDashboardData();
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    const prepareChartData = () => {
        const labels = analyticsData.map(item => item.date);

        return {
            labels,
            datasets: [
                {
                    label: 'Temperature (°C)',
                    data: analyticsData.map(item => item.temperature),
                    borderColor: 'rgb(24, 144, 255)',
                    backgroundColor: 'rgba(24, 144, 255, 0.2)',
                    tension: 0.1,
                },
                {
                    label: 'Humidity (%)',
                    data: analyticsData.map(item => item.humidity),
                    borderColor: 'rgb(82, 196, 26)',
                    backgroundColor: 'rgba(82, 196, 26, 0.2)',
                    tension: 0.1,
                },
                {
                    label: 'Soil Moisture (%)',
                    data: analyticsData.map(item => item.soilMoisture),
                    borderColor: 'rgb(250, 173, 20)',
                    backgroundColor: 'rgba(250, 173, 20, 0.2)',
                    tension: 0.1,
                },
            ],
        };
    };

    const chartOptions = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                position: 'top',
            },
            title: {
                display: true,
                text: 'Real-time Sensor Data Analytics',
            },
        },
        scales: {
            y: {
                beginAtZero: true,
            },
        },
    };

    const renderChart = () => {
        if (!analyticsData || analyticsData.length === 0) {
            return (
                <div className="text-center py-20">
                    <Text type="secondary">No sensor data available for analytics</Text>
                    <br />
                    <Text type="secondary" style={{ fontSize: '12px' }}>
                        Process some sensor data files to see analytics charts
                    </Text>
                </div>
            );
        }

        const chartData = prepareChartData();

        switch (chartType) {
            case 'bar':
                return (
                    <div style={{ height: '300px' }}>
                        <Bar data={chartData} options={chartOptions} />
                    </div>
                );

            case 'doughnut': {
                const doughnutData = {
                    labels: ['Temperature', 'Humidity', 'Soil Moisture'],
                    datasets: [
                        {
                            data: [
                                analyticsData.reduce((sum, d) => sum + d.temperature, 0) / analyticsData.length,
                                analyticsData.reduce((sum, d) => sum + d.humidity, 0) / analyticsData.length,
                                analyticsData.reduce((sum, d) => sum + d.soilMoisture, 0) / analyticsData.length,
                            ],
                            backgroundColor: [
                                'rgba(24, 144, 255, 0.8)',
                                'rgba(82, 196, 26, 0.8)',
                                'rgba(250, 173, 20, 0.8)',
                            ],
                            borderColor: [
                                'rgb(24, 144, 255)',
                                'rgb(82, 196, 26)',
                                'rgb(250, 173, 20)',
                            ],
                            borderWidth: 2,
                        },
                    ],
                };
                return (
                    <div style={{ height: '300px', display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
                        <Doughnut
                            data={doughnutData}
                            options={{
                                responsive: true,
                                maintainAspectRatio: false,
                                plugins: {
                                    legend: {
                                        position: 'bottom',
                                    },
                                    title: {
                                        display: true,
                                        text: 'Average Sensor Values Distribution',
                                    },
                                },
                            }}
                        />
                    </div>
                );
            }

            default: // line
                return (
                    <div style={{ height: '300px' }}>
                        <Line data={chartData} options={chartOptions} />
                    </div>
                );
        }
    };

    if (loading && !pipelineStatus) {
        return (
            <div className="flex items-center justify-center py-20">
                <Spin size="large">
                    <div className="p-8">
                        <p>Loading dashboard data...</p>
                    </div>
                </Spin>
            </div>
        );
    }

    if (error) {
        return (
            <Alert
                message="Error Loading Dashboard"
                description={error}
                type="error"
                showIcon
                action={
                    <Button size="small" onClick={fetchDashboardData}>
                        Retry
                    </Button>
                }
            />
        );
    }

    const stats = pipelineStatus?.data?.stats || {};
    const processedFiles = Array.isArray(pipelineStatus?.data?.processedFiles)
        ? pipelineStatus.data.processedFiles
        : [];

    return (
        <div className="space-y-6">
            {/* Header with actions */}
            <div className="flex justify-between items-center">
                <div>
                    <Title level={2} className="mb-2">Agricultural Analytics Dashboard</Title>
                    <Text type="secondary">
                        Monitor agricultural sensor data pipeline performance, environmental trends, and system health
                    </Text>
                </div>
                <Space>
                    <Button
                        icon={<ReloadOutlined />}
                        onClick={async () => {
                            await fetchDashboardData();
                            await generateSampleAnalytics();
                            await fetchTotalRecordsCount();
                        }}
                        loading={loading || analyticsLoading}
                    >
                        Refresh All
                    </Button>
                    <Button
                        type="primary"
                        icon={<PlayCircleOutlined />}
                        onClick={handleProcessFiles}
                        loading={loading}
                    >
                        Process Files
                    </Button>
                </Space>
            </div>

            {/* System Health Alert */}
            {systemHealth && (
                <Alert
                    message={`System Status: ${systemHealth.status === 'healthy' ? 'Online' : 'Offline'}`}
                    description={`Pipeline service is ${systemHealth.status === 'healthy' ? 'running normally' : 'experiencing issues'}`}
                    type={systemHealth.status === 'healthy' ? 'success' : 'error'}
                    showIcon
                    className="mb-6"
                />
            )}

            {/* Key Metrics */}
            <Row gutter={[16, 16]}>
                <Col xs={24} sm={12} lg={6}>
                    <Card className="dashboard-card">
                        <Statistic
                            title="Files Processed"
                            value={stats.filesProcessed || 0}
                            prefix={<FileTextOutlined />}
                            valueStyle={{ color: '#1890ff' }}
                        />
                    </Card>
                </Col>
                <Col xs={24} sm={12} lg={6}>
                    <Card className="dashboard-card">
                        <Statistic
                            title="Total Records in System"
                            value={totalRecordsInSystem || stats.recordsIngested || 0}
                            prefix={<DatabaseOutlined />}
                            valueStyle={{ color: '#1890ff' }}
                        />
                        <Text type="secondary" style={{ fontSize: '12px' }}>
                            From uploaded .parquet files
                        </Text>
                    </Card>
                </Col>
                <Col xs={24} sm={12} lg={6}>
                    <Card className="dashboard-card">
                        <Statistic
                            title="Records Ingested"
                            value={stats.recordsIngested || 0}
                            prefix={<DatabaseOutlined />}
                            valueStyle={{ color: '#52c41a' }}
                        />
                        <Text type="secondary" style={{ fontSize: '12px' }}>
                            Successfully processed
                        </Text>
                    </Card>
                </Col>
                <Col xs={24} sm={12} lg={6}>
                    <Card className="dashboard-card">
                        <Statistic
                            title="Processing Time"
                            value={stats.processingTime || 0}
                            suffix="ms"
                            prefix={<ClockCircleOutlined />}
                            valueStyle={{ color: '#faad14' }}
                        />
                    </Card>
                </Col>
                <Col xs={24} sm={12} lg={6}>
                    <Card className="dashboard-card">
                        <Statistic
                            title="Data Points"
                            value={analyticsData.length || 0}
                            prefix={<BarChartOutlined />}
                            valueStyle={{ color: '#722ed1' }}
                        />
                    </Card>
                </Col>
            </Row>

            {/* Analytics Section */}
            <Row gutter={[16, 16]}>
                <Col xs={24} lg={16}>
                    <Card
                        title={`${chartType.charAt(0).toUpperCase() + chartType.slice(1)} Chart - Sensor Data`}
                        extra={
                            <Select
                                value={chartType}
                                onChange={setChartType}
                                style={{ width: 150 }}
                                size="small"
                            >
                                <Option value="line">
                                    <LineChartOutlined className="mr-2" />
                                    Line Chart
                                </Option>
                                <Option value="bar">
                                    <BarChartOutlined className="mr-2" />
                                    Bar Chart
                                </Option>
                                <Option value="doughnut">
                                    <PieChartOutlined className="mr-2" />
                                    Doughnut
                                </Option>
                            </Select>
                        }
                    >
                        {analyticsLoading ? (
                            <div className="flex items-center justify-center py-20">
                                <Spin size="large" tip="Loading analytics..." />
                            </div>
                        ) : (
                            renderChart()
                        )}
                    </Card>
                </Col>
                <Col xs={24} lg={8}>
                    <div className="space-y-4">
                        <Card title="Analytics Summary">
                            <div className="space-y-3">
                                <Statistic
                                    title="Avg Temperature"
                                    value={analyticsData.length > 0 ? (analyticsData.reduce((sum, d) => sum + d.temperature, 0) / analyticsData.length).toFixed(1) : 0}
                                    precision={1}
                                    suffix="°C"
                                    valueStyle={{ color: '#1890ff' }}
                                />
                                <Divider />
                                <Statistic
                                    title="Avg Humidity"
                                    value={analyticsData.length > 0 ? (analyticsData.reduce((sum, d) => sum + d.humidity, 0) / analyticsData.length).toFixed(1) : 0}
                                    precision={1}
                                    suffix="%"
                                    valueStyle={{ color: '#52c41a' }}
                                />
                                <Divider />
                                <Statistic
                                    title="Avg Soil Moisture"
                                    value={analyticsData.length > 0 ? (analyticsData.reduce((sum, d) => sum + d.soilMoisture, 0) / analyticsData.length).toFixed(1) : 0}
                                    precision={1}
                                    suffix="%"
                                    valueStyle={{ color: '#faad14' }}
                                />
                            </div>
                        </Card>
                        <Card title="Processing Status">
                            <Progress
                                percent={
                                    stats.recordsIngested && (stats.recordsIngested + stats.recordsFailed) > 0
                                        ? Math.round((stats.recordsIngested / (stats.recordsIngested + stats.recordsFailed)) * 100)
                                        : 100
                                }
                                strokeColor={{
                                    '0%': '#108ee9',
                                    '100%': '#87d068',
                                }}
                                className="mb-2"
                            />
                            <div className="flex justify-between text-sm text-gray-600">
                                <span>Success: {stats.recordsIngested || 0}</span>
                                <span>Failed: {stats.recordsFailed || 0}</span>
                            </div>
                        </Card>
                    </div>
                </Col>
            </Row>

            {/* System Information and Recent Files */}
            <Row gutter={[16, 16]}>
                <Col xs={24} lg={12}>
                    <Card title="System Information">
                        <div className="space-y-3">
                            <div className="flex justify-between">
                                <Text>Last Updated:</Text>
                                <Text type="secondary">
                                    {stats.lastUpdated
                                        ? format(parseISO(stats.lastUpdated), 'MMM dd, yyyy HH:mm:ss')
                                        : 'Never'
                                    }
                                </Text>
                            </div>
                            <div className="flex justify-between">
                                <Text>Dashboard Refreshed:</Text>
                                <Text type="secondary">
                                    {lastUpdated
                                        ? format(lastUpdated, 'HH:mm:ss')
                                        : 'Never'
                                    }
                                </Text>
                            </div>
                            <div className="flex justify-between">
                                <Text>Chart Type:</Text>
                                <Tag color="blue">{chartType.toUpperCase()}</Tag>
                            </div>
                            <div className="flex justify-between">
                                <Text>Service:</Text>
                                <Tag color={systemHealth?.status === 'healthy' ? 'green' : 'red'}>
                                    {systemHealth?.service || 'Pipeline API'}
                                </Tag>
                            </div>
                        </div>
                    </Card>
                </Col>

                <Col xs={24} lg={12}>
                    <Card
                        title="Recently Processed Files"
                        extra={
                            <Tag color="blue">
                                {processedFiles.length} file{processedFiles.length !== 1 ? 's' : ''}
                            </Tag>
                        }
                    >
                        {processedFiles.length > 0 ? (
                            <List
                                dataSource={processedFiles.slice(0, 5)} // Show only last 5 files
                                renderItem={(fileName) => (
                                    <List.Item>
                                        <div className="flex items-center justify-between w-full">
                                            <div className="flex items-center space-x-3">
                                                <CheckCircleOutlined className="text-green-500" />
                                                <Text className="truncate">{fileName}</Text>
                                            </div>
                                            <Tag color="green" size="small">Processed</Tag>
                                        </div>
                                    </List.Item>
                                )}
                                className="max-h-48 overflow-y-auto"
                            />
                        ) : (
                            <div className="text-center py-8">
                                <FileTextOutlined className="text-4xl text-gray-400 mb-4" />
                                <Text type="secondary">No files processed yet</Text>
                            </div>
                        )}
                    </Card>
                </Col>
            </Row>
        </div>
    );
};

export default AnalyticsDashboard;
