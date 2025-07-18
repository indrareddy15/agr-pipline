import React, { useState, useEffect } from 'react';
import {
    Table,
    Card,
    Button,
    Row,
    Col,
    Select,
    DatePicker,
    Input,
    Space,
    Tag,
    Typography,
    Pagination,
    Spin,
    Alert,
    Tooltip,
    Switch,
    Modal,
    Form,
    Dropdown,
    Menu,
    Checkbox,
    Statistic,
    Progress,
    message,
} from 'antd';
import {
    SearchOutlined,
    ReloadOutlined,
    DownloadOutlined,
    FilterOutlined,
    ClearOutlined,
    DatabaseOutlined,
    FileTextOutlined,
    BarChartOutlined,
} from '@ant-design/icons';
import { format, parseISO } from 'date-fns';
import { apiService } from '../services/apiService';

const { RangePicker } = DatePicker;
const { Option } = Select;
const { Title, Text } = Typography;

const DataViewer = () => {
    const [data, setData] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [exportModalVisible, setExportModalVisible] = useState(false);
    const [exportLoading, setExportLoading] = useState(false);
    const [totalRecordsInSystem, setTotalRecordsInSystem] = useState(0); // Total from uploaded files
    const [filters, setFilters] = useState({
        sensor_id: '',
        reading_type: '',
        date_from: '',
        date_to: '',
        limit: 500, // Increased from 100 to 500
        offset: 0,
    });
    const [showAllRecords, setShowAllRecords] = useState(false); // New state for showing all records
    const [pagination, setPagination] = useState({
        current: 1,
        pageSize: 500, // Increased from 100 to 500
        total: 0,
        showSizeChanger: true,
        pageSizeOptions: ['100', '500', '1000', '2000', '5000'], // Removed ALL option since we use toggle
    });
    const [sensorIds, setSensorIds] = useState([]);
    const [readingTypes, setReadingTypes] = useState([]);
    const [showAdvancedFilters, setShowAdvancedFilters] = useState(false);
    const [metadataCache, setMetadataCache] = useState({
        sensorIds: null,
        readingTypes: null,
        lastFetch: null
    });

    // Cache duration in milliseconds (5 minutes)
    const CACHE_DURATION = 5 * 60 * 1000;

    useEffect(() => {
        fetchData();
        fetchFilterOptions();
        fetchTotalRecordsCount(); // Fetch total count from uploaded files
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    useEffect(() => {
        // Debounce data fetching when filters change
        const timeoutId = setTimeout(() => {
            fetchData();
        }, 300); // 300ms debounce delay

        return () => clearTimeout(timeoutId);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [filters.limit, filters.offset, filters.sensor_id, filters.reading_type, filters.date_from, filters.date_to]);

    const fetchData = async () => {
        try {
            setLoading(true);
            setError(null);

            // If showing all records, remove pagination limits
            const fetchFilters = showAllRecords ? {
                ...filters,
                limit: 999999, // Very large number to get all records
                offset: 0
            } : filters;

            const response = await apiService.getFilteredData(fetchFilters);
            setData(response.data.records || []);

            setPagination(prev => ({
                ...prev,
                total: response.data.count || 0,
                current: showAllRecords ? 1 : Math.floor(filters.offset / filters.limit) + 1,
            }));
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    const fetchFilterOptions = async () => {
        try {
            const now = Date.now();

            // Check if cache is still valid
            if (metadataCache.lastFetch &&
                (now - metadataCache.lastFetch) < CACHE_DURATION &&
                metadataCache.sensorIds &&
                metadataCache.readingTypes) {

                setSensorIds(metadataCache.sensorIds);
                setReadingTypes(metadataCache.readingTypes);
                return;
            }

            // Fetch fresh data if cache is expired or empty
            const [sensors, readings] = await Promise.all([
                apiService.getSensorIds(),
                apiService.getReadingTypes(),
            ]);

            setSensorIds(sensors);
            setReadingTypes(readings);

            // Update cache
            setMetadataCache({
                sensorIds: sensors,
                readingTypes: readings,
                lastFetch: now
            });
        } catch {
            // Failed to fetch filter options
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
            // Fallback to pagination total if summary fails
            setTotalRecordsInSystem(pagination.total);
        }
    };

    const handleFilterChange = (key, value) => {
        setFilters(prev => ({
            ...prev,
            [key]: value,
            offset: 0, // Reset to first page when filters change
        }));
    };

    const handleDateRangeChange = (dates, dateStrings) => {
        setFilters(prev => ({
            ...prev,
            date_from: dateStrings[0],
            date_to: dateStrings[1],
            offset: 0,
        }));
    };

    const handleTableChange = (paginationConfig) => {
        setShowAllRecords(false); // Always disable show all when changing pagination
        const newOffset = (paginationConfig.current - 1) * paginationConfig.pageSize;
        setFilters(prev => ({
            ...prev,
            limit: paginationConfig.pageSize,
            offset: newOffset,
        }));
    };

    const handleSearch = () => {
        setFilters(prev => ({ ...prev, offset: 0 }));
        fetchData();
    };

    const handleClearFilters = () => {
        setFilters({
            sensor_id: '',
            reading_type: '',
            date_from: '',
            date_to: '',
            limit: 100,
            offset: 0,
        });
    };

    const handleExport = () => {
        // Simple CSV export (legacy)
        if (data.length === 0) return;

        const headers = Object.keys(data[0]);
        const csvContent = [
            headers.join(','),
            ...data.map(row =>
                headers.map(field => {
                    const value = row[field];
                    // Escape values that contain commas or quotes
                    if (typeof value === 'string' && (value.includes(',') || value.includes('"'))) {
                        return `"${value.replace(/"/g, '""')}"`;
                    }
                    return value;
                }).join(',')
            )
        ].join('\n');

        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);
        link.setAttribute('href', url);
        link.setAttribute('download', `sensor_data_${new Date().toISOString().split('T')[0]}.csv`);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    };

    const handleAdvancedExport = () => {
        setExportModalVisible(true);
    };

    const handleExportSubmit = async (values) => {
        try {
            setExportLoading(true);

            const exportOptions = {
                format: values.format,
                compression: values.compression,
                partition_by: values.partition_by,
                columnar: values.columnar,
                date_from: filters.date_from,
                date_to: filters.date_to,
                sensor_id: filters.sensor_id,
                reading_type: filters.reading_type
            };

            if (values.format === 'download') {
                // Direct download
                await apiService.downloadData(values.actualFormat, {
                    compression: values.compression,
                    partition_by: values.partition_by,
                    date_from: filters.date_from,
                    date_to: filters.date_to,
                    sensor_id: filters.sensor_id,
                    reading_type: filters.reading_type
                });
                message.success('Download started successfully');
            } else {
                // Export preparation
                const result = await apiService.exportData(exportOptions);
                message.success(`Export prepared: ${result.data.filename} (${result.data.recordCount} records)`);

                // Optionally trigger download
                if (result.data.downloadUrl) {
                    const link = document.createElement('a');
                    link.href = result.data.downloadUrl;
                    link.download = result.data.filename;
                    document.body.appendChild(link);
                    link.click();
                    document.body.removeChild(link);
                }
            }

            setExportModalVisible(false);
        } catch (error) {
            message.error(`Export failed: ${error.message}`);
        } finally {
            setExportLoading(false);
        }
    };

    const exportMenu = (
        <Menu>
            <Menu.Item key="csv" onClick={handleExport}>
                <Space>
                    <span>Quick CSV Export</span>
                    <Tag size="small">Legacy</Tag>
                </Space>
            </Menu.Item>
            <Menu.Item key="advanced" onClick={handleAdvancedExport}>
                <Space>
                    <span>Advanced Export</span>
                    <Tag color="blue" size="small">Optimized</Tag>
                </Space>
            </Menu.Item>
        </Menu>
    );

    const getTypeTag = (readingType) => {
        const colors = {
            temperature: 'red',
            humidity: 'blue',
            pressure: 'green',
            voltage: 'orange',
            current: 'purple',
        };
        return (
            <Tag color={colors[readingType] || 'default'}>
                {readingType}
            </Tag>
        );
    };

    const getAnomalyTag = (isAnomalous) => {
        return isAnomalous ? (
            <Tag color="red">Anomaly</Tag>
        ) : (
            <Tag color="green">Normal</Tag>
        );
    };

    const columns = [
        {
            title: 'Sensor ID',
            dataIndex: 'sensor_id',
            key: 'sensor_id',
            width: 120,
            fixed: 'left',
        },
        {
            title: 'Timestamp',
            dataIndex: 'timestamp',
            key: 'timestamp',
            width: 180,
            render: (timestamp) => format(parseISO(timestamp), 'MMM dd, yyyy HH:mm:ss'),
            sorter: (a, b) => new Date(a.timestamp) - new Date(b.timestamp),
        },
        {
            title: 'Reading Type',
            dataIndex: 'reading_type',
            key: 'reading_type',
            width: 120,
            render: getTypeTag,
        },
        {
            title: 'Value',
            dataIndex: 'value',
            key: 'value',
            width: 100,
            render: (value) => parseFloat(value).toFixed(2),
            sorter: (a, b) => a.value - b.value,
        },
        {
            title: 'Calibrated Value',
            dataIndex: 'calibrated_value',
            key: 'calibrated_value',
            width: 120,
            render: (value) => parseFloat(value).toFixed(2),
        },
        {
            title: 'Battery Level',
            dataIndex: 'battery_level',
            key: 'battery_level',
            width: 120,
            render: (value) => `${parseFloat(value).toFixed(1)}%`,
            sorter: (a, b) => a.battery_level - b.battery_level,
        },
        {
            title: 'Status',
            key: 'status',
            width: 100,
            render: (_, record) => getAnomalyTag(record.anomalous_reading),
        },
        {
            title: 'Daily Avg',
            dataIndex: 'daily_average',
            key: 'daily_average',
            width: 100,
            render: (value) => value ? parseFloat(value).toFixed(2) : 'N/A',
        },
        {
            title: 'Flags',
            key: 'flags',
            width: 150,
            render: (_, record) => (
                <Space size={[0, 4]} wrap>
                    {record.outlier_corrected && <Tag color="orange" size="small">Outlier</Tag>}
                    {record.missing_value_filled && <Tag color="blue" size="small">Filled</Tag>}
                </Space>
            ),
        },
    ];

    return (
        <div className="space-y-6">
            <div className="flex justify-between items-center">
                <div>
                    <Title level={2}>Agricultural Sensor Data Viewer</Title>
                    <Text type="secondary">
                        View processed agricultural sensor data with cleaning, calibration, and derived fields.
                        Data includes temperature, humidity, and other environmental measurements from farm sensors.
                        <br />
                        <Text strong>Total Records in System:</Text> Shows all records from your uploaded .parquet files.
                        <br />
                        <Text strong>Filtered Results:</Text> Shows records matching your current filter criteria.
                        <br />
                        <Text strong>Currently Displayed:</Text> Shows records visible {showAllRecords ? '(all matching records)' : 'on the current page'}.
                        <br />
                        <Text strong>Tip:</Text> Use the "Show All" toggle to display all your records without pagination limits.
                    </Text>
                </div>
                <Space>
                    <Switch
                        checked={showAllRecords}
                        onChange={(checked) => {
                            setShowAllRecords(checked);
                            if (checked) {
                                setFilters(prev => ({ ...prev, limit: 999999, offset: 0 }));
                            } else {
                                setFilters(prev => ({ ...prev, limit: 500, offset: 0 }));
                            }
                        }}
                        checkedChildren="Show All"
                        unCheckedChildren="Paginated"
                    />
                    <Dropdown overlay={exportMenu} trigger={['click']}>
                        <Button
                            icon={<DownloadOutlined />}
                            disabled={data.length === 0}
                        >
                            Export Data
                        </Button>
                    </Dropdown>
                    <Button
                        icon={<ReloadOutlined />}
                        onClick={fetchData}
                        loading={loading}
                    >
                        Refresh
                    </Button>
                </Space>
            </div>

            {/* Data Statistics */}
            {(totalRecordsInSystem > 0 || pagination.total > 0) && (
                <Row gutter={16} className="mb-6">
                    <Col span={6}>
                        <Card>
                            <Statistic
                                title="Total Records in System"
                                value={totalRecordsInSystem || pagination.total}
                                prefix={<DatabaseOutlined />}
                                valueStyle={{ color: '#1890ff' }}
                            />
                            <Text type="secondary" style={{ fontSize: '12px' }}>
                                From uploaded .parquet files
                            </Text>
                        </Card>
                    </Col>
                    <Col span={6}>
                        <Card>
                            <Statistic
                                title="Filtered Results"
                                value={pagination.total}
                                prefix={<FilterOutlined />}
                                valueStyle={{ color: '#52c41a' }}
                            />
                            <Text type="secondary" style={{ fontSize: '12px' }}>
                                Matching current filters
                            </Text>
                        </Card>
                    </Col>
                    <Col span={6}>
                        <Card>
                            <Statistic
                                title="Currently Displayed"
                                value={data.length}
                                prefix={<FileTextOutlined />}
                                valueStyle={{ color: '#722ed1' }}
                            />
                            <Text type="secondary" style={{ fontSize: '12px' }}>
                                On this page
                            </Text>
                        </Card>
                    </Col>
                    <Col span={6}>
                        <Card>
                            <Statistic
                                title="Page Size"
                                value={pagination.pageSize}
                                prefix={<BarChartOutlined />}
                                valueStyle={{ color: '#fa8c16' }}
                            />
                            <Text type="secondary" style={{ fontSize: '12px' }}>
                                Records per page
                            </Text>
                        </Card>
                    </Col>
                </Row>
            )}

            {/* Display Progress Indicator */}
            {totalRecordsInSystem > 0 && data.length > 0 && (
                <Card className="mb-6">
                    <Row align="middle">
                        <Col span={4}>
                            <Text strong>Data Coverage:</Text>
                        </Col>
                        <Col span={16}>
                            <Progress
                                percent={Math.round((pagination.total / totalRecordsInSystem) * 100)}
                                strokeColor={{
                                    '0%': '#108ee9',
                                    '100%': '#87d068',
                                }}
                                format={(percent) => `${percent}% of total data`}
                            />
                        </Col>
                        <Col span={4} className="text-right">
                            <Text type="secondary">
                                {pagination.total} / {totalRecordsInSystem} records
                            </Text>
                        </Col>
                    </Row>
                </Card>
            )}

            {/* Filters */}
            <Card title="Filters" className="mb-6">
                <Row gutter={[16, 16]} align="middle">
                    <Col xs={24} sm={12} md={6}>
                        <Text strong>Sensor ID:</Text>
                        <Select
                            value={filters.sensor_id}
                            onChange={(value) => handleFilterChange('sensor_id', value)}
                            placeholder="All sensors"
                            allowClear
                            className="w-full mt-1"
                        >
                            {sensorIds.map(id => (
                                <Option key={id} value={id}>{id}</Option>
                            ))}
                        </Select>
                    </Col>

                    <Col xs={24} sm={12} md={6}>
                        <Text strong>Reading Type:</Text>
                        <Select
                            value={filters.reading_type}
                            onChange={(value) => handleFilterChange('reading_type', value)}
                            placeholder="All types"
                            allowClear
                            className="w-full mt-1"
                        >
                            {readingTypes.map(type => (
                                <Option key={type} value={type}>
                                    {getTypeTag(type)}
                                </Option>
                            ))}
                        </Select>
                    </Col>

                    <Col xs={24} sm={12} md={8}>
                        <Text strong>Date Range:</Text>
                        <RangePicker
                            onChange={handleDateRangeChange}
                            className="w-full mt-1"
                            format="YYYY-MM-DD"
                        />
                    </Col>

                    <Col xs={24} sm={12} md={4}>
                        <div className="mt-6">
                            <Space>
                                <Button
                                    type="primary"
                                    icon={<SearchOutlined />}
                                    onClick={handleSearch}
                                    loading={loading}
                                >
                                    Search
                                </Button>
                                <Button
                                    icon={<ClearOutlined />}
                                    onClick={handleClearFilters}
                                >
                                    Clear
                                </Button>
                            </Space>
                        </div>
                    </Col>
                </Row>

                <Row className="mt-4">
                    <Col span={24}>
                        <Space>
                            <Text strong>Advanced Filters:</Text>
                            <Switch
                                checked={showAdvancedFilters}
                                onChange={setShowAdvancedFilters}
                                size="small"
                            />
                        </Space>
                    </Col>
                </Row>

                {showAdvancedFilters && (
                    <Row gutter={[16, 16]} className="mt-4 p-4 bg-gray-50 rounded">
                        <Col xs={24} sm={8}>
                            <Text strong>Records per page:</Text>
                            <Select
                                value={filters.limit}
                                onChange={(value) => handleFilterChange('limit', value)}
                                className="w-full mt-1"
                            >
                                <Option value={100}>100</Option>
                                <Option value={500}>500</Option>
                                <Option value={1000}>1,000</Option>
                                <Option value={2000}>2,000</Option>
                                <Option value={5000}>5,000</Option>
                            </Select>
                        </Col>
                    </Row>
                )}
            </Card>

            {/* Error Alert */}
            {error && (
                <Alert
                    message="Error Loading Data"
                    description={error}
                    type="error"
                    showIcon
                    action={
                        <Button size="small" onClick={fetchData}>
                            Retry
                        </Button>
                    }
                />
            )}

            {/* Data Table */}
            <Card>
                <div className="mb-4 flex justify-between items-center">
                    <Space>
                        <FilterOutlined />
                        <Text strong>
                            {showAllRecords ? `All ${data.length} records` : `${data.length} records`}
                            {filters.sensor_id && ` for ${filters.sensor_id}`}
                            {filters.reading_type && ` (${filters.reading_type})`}
                        </Text>
                        {showAllRecords && (
                            <Tag color="green">Showing All Records</Tag>
                        )}
                    </Space>

                    {!showAllRecords && (
                        <Pagination
                            current={pagination.current}
                            pageSize={pagination.pageSize}
                            total={pagination.total}
                            showSizeChanger
                            showQuickJumper
                            pageSizeOptions={['100', '500', '1000', '2000', '5000']}
                            showTotal={(total, range) =>
                                `${range[0]}-${range[1]} of ${total} items`
                            }
                            onChange={(page, pageSize) => {
                                handleTableChange({ current: page, pageSize });
                            }}
                            onShowSizeChange={(current, size) => {
                                handleTableChange({ current: 1, pageSize: size });
                            }}
                            size="small"
                        />
                    )}
                </div>

                <Table
                    columns={columns}
                    dataSource={data}
                    loading={loading}
                    rowKey={(record) => `${record.sensor_id}-${record.timestamp}`}
                    scroll={{ x: 1200 }}
                    pagination={false} // Always false since we handle pagination manually above
                    size="small"
                    className="sensor-data-table"
                />
            </Card>

            {/* Advanced Export Modal */}
            <Modal
                title="Advanced Data Export"
                visible={exportModalVisible}
                onCancel={() => setExportModalVisible(false)}
                footer={null}
                width={600}
            >
                <Form
                    layout="vertical"
                    onFinish={handleExportSubmit}
                    initialValues={{
                        format: 'json',
                        compression: 'none',
                        partition_by: 'date',
                        columnar: false
                    }}
                >
                    <Row gutter={[16, 16]}>
                        <Col xs={24} md={12}>
                            <Form.Item
                                label="Export Format"
                                name="format"
                                tooltip="Choose the output format for your data"
                            >
                                <Select>
                                    <Option value="json">JSON - JavaScript Object Notation</Option>
                                    <Option value="csv">CSV - Comma Separated Values</Option>
                                    <Option value="parquet">Parquet - Columnar Storage</Option>
                                </Select>
                            </Form.Item>
                        </Col>
                        <Col xs={24} md={12}>
                            <Form.Item
                                label="Compression"
                                name="compression"
                                tooltip="Apply compression to reduce file size"
                            >
                                <Select>
                                    <Option value="none">None - No compression</Option>
                                    <Option value="gzip">GZIP - Good compression ratio</Option>
                                    <Option value="snappy">Snappy - Fast compression</Option>
                                </Select>
                            </Form.Item>
                        </Col>
                        <Col xs={24} md={12}>
                            <Form.Item
                                label="Partitioning"
                                name="partition_by"
                                tooltip="Organize data for analytical queries"
                            >
                                <Select>
                                    <Option value="none">None - Single file</Option>
                                    <Option value="date">By Date - Partition by date</Option>
                                    <Option value="sensor_id">By Sensor - Partition by sensor ID</Option>
                                    <Option value="both">Date + Sensor - Both partitions</Option>
                                </Select>
                            </Form.Item>
                        </Col>
                        <Col xs={24} md={12}>
                            <Form.Item name="columnar" valuePropName="checked">
                                <Checkbox>
                                    Columnar Format
                                </Checkbox>
                            </Form.Item>
                            <Text type="secondary" className="text-sm">
                                Optimize for analytical queries by storing data in columnar format
                            </Text>
                        </Col>
                    </Row>

                    <Alert
                        message="Export Configuration"
                        description={
                            <div>
                                <p><strong>Current filters will be applied:</strong></p>
                                <ul className="mb-0">
                                    {filters.sensor_id && <li>Sensor ID: {filters.sensor_id}</li>}
                                    {filters.reading_type && <li>Reading Type: {filters.reading_type}</li>}
                                    {filters.date_from && <li>Date From: {filters.date_from}</li>}
                                    {filters.date_to && <li>Date To: {filters.date_to}</li>}
                                    {!filters.sensor_id && !filters.reading_type && !filters.date_from && !filters.date_to &&
                                        <li>All data (no filters applied)</li>
                                    }
                                </ul>
                            </div>
                        }
                        type="info"
                        className="mb-4"
                    />

                    <Row gutter={[16, 16]}>
                        <Col span={12}>
                            <Button
                                type="default"
                                onClick={() => setExportModalVisible(false)}
                                block
                            >
                                Cancel
                            </Button>
                        </Col>
                        <Col span={12}>
                            <Button
                                type="primary"
                                htmlType="submit"
                                loading={exportLoading}
                                block
                            >
                                Export Data
                            </Button>
                        </Col>
                    </Row>
                </Form>
            </Modal>
        </div>
    );
};

export default DataViewer;
