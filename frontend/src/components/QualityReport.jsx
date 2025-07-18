import React, { useState, useEffect } from 'react';
import {
    Card,
    Row,
    Col,
    Statistic,
    Progress,
    Typography,
    Button,
    Alert,
    Spin,
    Tag,
    Descriptions,
    Empty,
    Result,
    Divider,
    Space,
    Tooltip,
} from 'antd';
import {
    FileTextOutlined,
    ReloadOutlined,
    DownloadOutlined,
    CheckCircleOutlined,
    ExclamationCircleOutlined,
    WarningOutlined,
    TrophyOutlined,
    BugOutlined,
    EyeInvisibleOutlined,
    // BoltOutlined,
    LineChartOutlined,
} from '@ant-design/icons';
import { Line, Bar, Doughnut } from 'react-chartjs-2';
import {
    Chart as ChartJS,
    CategoryScale,
    LinearScale,
    PointElement,
    LineElement,
    BarElement,
    ArcElement,
    Title as ChartTitle,
    Tooltip as ChartTooltip,
    Legend,
} from 'chart.js';
import { apiService } from '../services/apiService';

ChartJS.register(
    CategoryScale,
    LinearScale,
    PointElement,
    LineElement,
    BarElement,
    ArcElement,
    ChartTitle,
    ChartTooltip,
    Legend
);

const { Title, Text } = Typography;

const QualityReport = () => {
    const [report, setReport] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    useEffect(() => {
        fetchQualityReport();
    }, []);

    const fetchQualityReport = async () => {
        try {
            setLoading(true);
            setError(null);
            const response = await apiService.getQualityReport();

            // Fix: The backend returns { status: "success", data: { report: {...} } }
            // So we need to access response.data.report (not response.data.data.report)
            if (response.status === 'success' && response.data?.report) {
                setReport(response.data.report);
            } else {
                // If no report data, show appropriate message instead of mock data
                setReport(null);
                setError('No quality report available. Please process some data files first.');
            }
        } catch (err) {
            setError(err.message);
            // Don't set mock data - let user know they need to process files
            setReport(null);
        } finally {
            setLoading(false);
        }
    };

    const getQualityScoreColor = (score) => {
        if (score >= 90) return '#52c41a'; // Green
        if (score >= 70) return '#faad14'; // Orange
        return '#ff4d4f'; // Red
    };

    const getQualityGrade = (score) => {
        if (score >= 95) return 'Excellent';
        if (score >= 85) return 'Good';
        if (score >= 70) return 'Fair';
        return 'Poor';
    };

    const generateChartData = () => {
        if (!report?.summary) return null;

        const { totalRecords, missingValues, anomalousReadings, outliersCorreted } = report.summary;
        const validRecords = totalRecords - missingValues - anomalousReadings;

        return {
            labels: ['Valid Records', 'Missing Values', 'Anomalous', 'Outliers Corrected'],
            datasets: [{
                data: [validRecords, missingValues, anomalousReadings, outliersCorreted],
                backgroundColor: ['#52c41a', '#ff7875', '#faad14', '#1890ff'],
                borderWidth: 2,
                borderColor: '#fff'
            }]
        };
    };

    const generateTrendData = () => {
        // Mock trend data for demonstration
        return {
            labels: ['Week 1', 'Week 2', 'Week 3', 'Week 4'],
            datasets: [{
                label: 'Quality Score',
                data: [88, 92, 95, report?.summary?.overallQualityScore || 95],
                borderColor: '#1890ff',
                backgroundColor: 'rgba(24, 144, 255, 0.1)',
                fill: true,
                tension: 0.4
            }]
        };
    };

    const handleExportReport = () => {
        if (!report) return;

        // Create a detailed report text
        const reportText = `
DATA QUALITY REPORT
==================
Generated: ${new Date().toISOString()}

${JSON.stringify(report, null, 2)}
    `;

        const blob = new Blob([reportText], { type: 'text/plain;charset=utf-8;' });
        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);
        link.setAttribute('href', url);
        link.setAttribute('download', `data_quality_report_${new Date().toISOString().split('T')[0]}.txt`);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    };

    if (loading) {
        return (
            <div style={{ padding: '24px', textAlign: 'center' }}>
                <Spin size="large" />
                <Title level={4} style={{ marginTop: 16 }}>Loading Quality Report...</Title>
            </div>
        );
    }

    if (error && !report) {
        return (
            <div style={{ padding: '24px' }}>
                <Alert
                    message="Error Loading Quality Report"
                    description={error}
                    type="error"
                    showIcon
                    action={
                        <Button size="small" onClick={fetchQualityReport}>
                            Retry
                        </Button>
                    }
                />
            </div>
        );
    }

    if (!report) {
        return (
            <div style={{ padding: '24px' }}>
                <Card>
                    <Result
                        icon={<ExclamationCircleOutlined />}
                        title="No Quality Report Available"
                        subTitle="Please process some data files first to generate a quality report."
                        extra={
                            <Button type="primary" onClick={fetchQualityReport} loading={loading}>
                                Refresh
                            </Button>
                        }
                    />
                </Card>
            </div>
        );
    }

    const qualityScore = report.summary?.overallQualityScore || 0;
    const chartData = generateChartData();
    const trendData = generateTrendData();

    return (
        <div style={{ padding: '24px' }}>
            <div style={{ marginBottom: 24 }}>
                <Space align="center">
                    <TrophyOutlined style={{ fontSize: 24, color: '#1890ff' }} />
                    <Title level={2} style={{ margin: 0 }}>Agricultural Data Quality Report</Title>
                </Space>
                <Text type="secondary">
                    DuckDB-powered validation analysis: schema compliance, value ranges, gap detection, and agricultural sensor data profiling
                </Text>
            </div>

            {error && (
                <Alert
                    message="Quality Report Unavailable"
                    description={error}
                    type="warning"
                    showIcon
                    style={{ marginBottom: 24 }}
                    action={
                        <Button size="small" onClick={fetchQualityReport}>
                            Retry
                        </Button>
                    }
                />
            )}

            {/* Quality Score Overview */}
            <Row gutter={[24, 24]} style={{ marginBottom: 24 }}>
                <Col xs={24} sm={12} lg={6}>
                    <Card>
                        <Statistic
                            title="Overall Quality Score"
                            value={qualityScore}
                            precision={1}
                            suffix="%"
                            valueStyle={{ color: getQualityScoreColor(qualityScore) }}
                            prefix={<TrophyOutlined />}
                        />
                        <Progress
                            percent={qualityScore}
                            strokeColor={getQualityScoreColor(qualityScore)}
                            size="small"
                            style={{ marginTop: 8 }}
                        />
                        <Tag color={getQualityScoreColor(qualityScore)} style={{ marginTop: 8 }}>
                            {getQualityGrade(qualityScore)}
                        </Tag>
                    </Card>
                </Col>
                <Col xs={24} sm={12} lg={6}>
                    <Card>
                        <Statistic
                            title="Total Records"
                            value={report.summary?.totalRecords || 0}
                            prefix={<FileTextOutlined />}
                        />
                    </Card>
                </Col>
                <Col xs={24} sm={12} lg={6}>
                    <Card>
                        <Statistic
                            title="Missing Values"
                            value={report.summary?.missingValues || 0}
                            valueStyle={{ color: '#ff4d4f' }}
                            prefix={<EyeInvisibleOutlined />}
                        />
                    </Card>
                </Col>
                <Col xs={24} sm={12} lg={6}>
                    <Card>
                        <Statistic
                            title="Anomalies Detected"
                            value={report.summary?.anomalousReadings || 0}
                            valueStyle={{ color: '#faad14' }}
                            prefix={<BugOutlined />}
                        />
                    </Card>
                </Col>
            </Row>

            {/* Charts Section */}
            <Row gutter={[24, 24]} style={{ marginBottom: 24 }}>
                <Col xs={24} lg={12}>
                    <Card title="Data Quality Distribution" extra={<Tooltip title="Distribution of data quality issues"><LineChartOutlined /></Tooltip>}>
                        {chartData && (
                            <div style={{ height: 300, display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
                                <Doughnut
                                    data={chartData}
                                    options={{
                                        responsive: true,
                                        maintainAspectRatio: false,
                                        plugins: {
                                            legend: {
                                                position: 'bottom'
                                            }
                                        }
                                    }}
                                />
                            </div>
                        )}
                    </Card>
                </Col>
                <Col xs={24} lg={12}>
                    <Card title="Quality Trend" extra={<Tooltip title="Quality score trend over time"><LineChartOutlined /></Tooltip>}>
                        {trendData && (
                            <div style={{ height: 300 }}>
                                <Line
                                    data={trendData}
                                    options={{
                                        responsive: true,
                                        maintainAspectRatio: false,
                                        scales: {
                                            y: {
                                                beginAtZero: true,
                                                max: 100
                                            }
                                        }
                                    }}
                                />
                            </div>
                        )}
                    </Card>
                </Col>
            </Row>

            {/* Detailed Metrics */}
            <Card
                title="Detailed Quality Metrics"
                extra={
                    <Space>
                        <Button icon={<ReloadOutlined />} onClick={fetchQualityReport} loading={loading}>
                            Refresh
                        </Button>
                        <Button icon={<DownloadOutlined />} onClick={handleExportReport}>
                            Export Report
                        </Button>
                    </Space>
                }
            >
                <Row gutter={[24, 24]}>
                    <Col xs={24} lg={12}>
                        <Descriptions title="Data Completeness" bordered size="small">
                            <Descriptions.Item label="Missing Values" span={3}>
                                <Space>
                                    <Text strong>{report.details?.missing_values?.count || 0}</Text>
                                    <Text type="secondary">({report.details?.missing_values?.percentage || '0.00'}%)</Text>
                                </Space>
                            </Descriptions.Item>
                            <Descriptions.Item label="Complete Records" span={3}>
                                <Text strong style={{ color: '#52c41a' }}>
                                    {(report.summary?.totalRecords || 0) - (report.summary?.missingValues || 0)}
                                </Text>
                            </Descriptions.Item>
                        </Descriptions>
                    </Col>
                    <Col xs={24} lg={12}>
                        <Descriptions title="Data Accuracy" bordered size="small">
                            <Descriptions.Item label="Anomalous Readings" span={3}>
                                <Space>
                                    <Text strong>{report.details?.anomalous_readings?.count || 0}</Text>
                                    <Text type="secondary">({report.details?.anomalous_readings?.percentage || '0.00'}%)</Text>
                                </Space>
                            </Descriptions.Item>
                            <Descriptions.Item label="Outliers Corrected" span={3}>
                                <Space>
                                    <Text strong>{report.details?.outliers_corrected?.count || 0}</Text>
                                    <Text type="secondary">({report.details?.outliers_corrected?.percentage || '0.00'}%)</Text>
                                </Space>
                            </Descriptions.Item>
                        </Descriptions>
                    </Col>
                </Row>

                <Divider />

                <Descriptions title="Data Consistency" bordered size="small">
                    <Descriptions.Item label="Time Gaps" span={2}>
                        {report.details?.time_gap_analysis?.total_gaps || 0}
                    </Descriptions.Item>
                    <Descriptions.Item label="Longest Gap">
                        {report.details?.time_gap_analysis?.longest_gap_hours || 0} hours
                    </Descriptions.Item>
                    <Descriptions.Item label="Report Generated" span={3}>
                        {report.timestamp ? new Date(report.timestamp).toLocaleString() : 'Unknown'}
                    </Descriptions.Item>
                </Descriptions>
            </Card>
        </div>
    );
};

export default QualityReport;
