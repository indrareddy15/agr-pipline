import React, { useState, useEffect } from 'react';
import {
  Layout,
  Menu,
  Typography,
  notification,
  Spin,
  ConfigProvider,
  theme,
  App as AntApp,
} from 'antd';
import {
  DashboardOutlined,
  UploadOutlined,
  FileTextOutlined,
  BarChartOutlined,
  SettingOutlined,
  DatabaseOutlined,
  UnorderedListOutlined,
} from '@ant-design/icons';
import AnalyticsDashboard from './components/AnalyticsDashboard';
import ProcessingFlow from './components/ProcessingFlow';
import DataViewer from './components/DataViewer';
import QualityReport from './components/QualityReport';
import Settings from './components/Settings';
import LogViewer from './components/LogViewer';
import { apiService } from './services/apiService';
import './App.css';

const { Header, Content, Sider } = Layout;
const { Title } = Typography;

const App = () => {
  const [selectedMenuItem, setSelectedMenuItem] = useState('dashboard');
  const [loading, setLoading] = useState(true);
  const [systemHealth, setSystemHealth] = useState(null);
  const [collapsed, setCollapsed] = useState(false);

  // Initialize notification
  const [api, contextHolder] = notification.useNotification();

  // Check system health on app load
  useEffect(() => {
    const initializeHealth = async () => {
      try {
        setLoading(true);
        const health = await apiService.checkHealth();
        setSystemHealth(health);

        if (health.status === 'healthy') {
          api.success({
            message: 'System Connected',
            description: 'Agricultural Pipeline backend is running successfully.',
            placement: 'topRight',
          });
        }
      } catch {
        api.error({
          message: 'Connection Failed',
          description: 'Unable to connect to Agricultural Pipeline backend.',
          placement: 'topRight',
        });
      } finally {
        setLoading(false);
      }
    };

    initializeHealth();
  }, [api]);

  const checkSystemHealth = async () => {
    try {
      setLoading(true);
      const health = await apiService.checkHealth();
      setSystemHealth(health);

      if (health.status === 'healthy') {
        api.success({
          message: 'System Connected',
          description: 'Agr Pipeline backend is running successfully.',
          placement: 'topRight',
        });
      }
    } catch {
      api.error({
        message: 'Connection Failed',
        description: 'Unable to connect to Agr Pipeline backend.',
        placement: 'topRight',
      });
    } finally {
      setLoading(false);
    }
  };

  const menuItems = [
    {
      key: 'processing',
      icon: <UploadOutlined />,
      label: 'Data Pipeline',
    },
    {
      key: 'data',
      icon: <DatabaseOutlined />,
      label: 'Sensor Data',
    },
    {
      key: 'quality',
      icon: <BarChartOutlined />,
      label: 'Quality Report',
    },
    {
      key: 'dashboard',
      icon: <DashboardOutlined />,
      label: 'Analytics',
    },
    {
      key: 'logs',
      icon: <UnorderedListOutlined />,
      label: 'System Logs',
    },
    {
      key: 'settings',
      icon: <SettingOutlined />,
      label: 'Settings',
    },
  ];

  const renderContent = () => {
    if (loading) {
      return (
        <div className="flex items-center justify-center h-full">
          <Spin size="large">
            <div className="p-8">
              <p>Connecting to Agr Pipeline...</p>
            </div>
          </Spin>
        </div>
      );
    }

    switch (selectedMenuItem) {
      case 'dashboard':
        return <AnalyticsDashboard systemHealth={systemHealth} />;
      case 'processing':
        return <ProcessingFlow />;
      case 'data':
        return <DataViewer />;
      case 'quality':
        return <QualityReport />;
      case 'analytics':
        return <AnalyticsDashboard systemHealth={systemHealth} />;
      case 'logs':
        return <LogViewer />;
      case 'settings':
        return <Settings onHealthCheck={checkSystemHealth} />;
      default:
        return <AnalyticsDashboard systemHealth={systemHealth} />;
    }
  };

  return (
    <AntApp>
      <ConfigProvider
        theme={{
          algorithm: theme.defaultAlgorithm,
          token: {
            colorPrimary: '#1890ff',
            colorSuccess: '#52c41a',
            colorWarning: '#faad14',
            colorError: '#ff4d4f',
            colorInfo: '#1890ff',
          },
        }}
      >
        {contextHolder}
        <Layout className="min-h-screen">
          <Sider
            collapsible
            collapsed={collapsed}
            onCollapse={setCollapsed}
            theme="light"
            className="shadow-md"
          >
            <div className="p-4">
              <Title
                level={collapsed ? 5 : 4}
                className="text-center text-blue-600 mb-0"
              >
                {collapsed ? 'AGR' : 'Agr Pipeline'}
              </Title>
            </div>
            <Menu
              theme="light"
              mode="inline"
              selectedKeys={[selectedMenuItem]}
              items={menuItems}
              onSelect={({ key }) => setSelectedMenuItem(key)}
            />
          </Sider>

          <Layout>
            <Header className="bg-white shadow-sm px-6 flex items-center justify-between">
              <Title level={3} className="mb-0 text-gray-800">
                {menuItems.find(item => item.key === selectedMenuItem)?.label}
              </Title>

              <div className="flex items-center space-x-4">
                <div className="flex items-center space-x-2">
                  <div
                    className={`w-3 h-3 rounded-full ${systemHealth?.status === 'healthy'
                      ? 'bg-green-500'
                      : 'bg-red-500'
                      }`}
                  />
                  <span className="text-sm text-gray-600">
                    {systemHealth?.status === 'healthy' ? 'Online' : 'Offline'}
                  </span>
                </div>
              </div>
            </Header>

            <Content className="m-6">
              <div className="bg-white rounded-lg shadow-sm p-6 min-h-full">
                {renderContent()}
              </div>
            </Content>
          </Layout>
        </Layout>
      </ConfigProvider>
    </AntApp>
  );
};

export default App;
