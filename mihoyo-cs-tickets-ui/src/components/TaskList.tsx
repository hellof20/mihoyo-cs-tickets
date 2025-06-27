import React, { useState } from 'react';
import { Table, Tag, Space, Select, Button, Tooltip } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import { useQuery, Query } from '@tanstack/react-query';
import { listTasks } from '../services/api';
import { TaskStatus } from '../types';

const { Option } = Select;

const TaskList: React.FC = () => {
  const [pageSize, setPageSize] = useState(10);
  const [current, setCurrent] = useState(1);
  const [lang, setLang] = useState<string>();
  const [status, setStatus] = useState<string>();
  const [refreshing, setRefreshing] = useState(false);

  const handleRefresh = async () => {
    setRefreshing(true);
    try {
      await refetch();
    } finally {
      setRefreshing(false);
    }
  };

  // Fetch data when component mounts
  React.useEffect(() => {
    handleRefresh();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const { data, isLoading, refetch } = useQuery<TaskStatus[]>({
    queryKey: ['tasks', current, pageSize, lang, status],
    queryFn: () => listTasks({
      limit: pageSize,
      offset: (current - 1) * pageSize,
      lang,
      status,
    }),
    enabled: false, // Disable automatic fetching
  });

  const columns = [
    {
      title: 'Task ID',
      dataIndex: 'task_id',
      key: 'task_id',
      ellipsis: true,
    },
    {
      title: 'Business',
      dataIndex: 'business',
      key: 'business',
    },
    {
      title: 'Language',
      dataIndex: 'lang',
      key: 'lang',
      render: (lang: string) => (
        <Tag color="blue">{lang.toUpperCase()}</Tag>
      ),
    },
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => {
        let color = status === 'running' ? 'processing' : 
                   status === 'completed' ? 'success' : 
                   status === 'failed' ? 'error' :
                   status === 'canceled' ? 'warning' : 'default';
        return (
          <Tag color={color}>{status.toUpperCase()}</Tag>
        );
      },
    },
    {
      title: 'Start Date',
      dataIndex: 'start_date',
      key: 'start_date',
    },
    {
      title: 'End Date',
      dataIndex: 'end_date',
      key: 'end_date',
    },
    {
      title: 'Created At',
      dataIndex: 'created_at',
      key: 'created_at',
    },
    {
      title: 'Updated At',
      dataIndex: 'updated_at',
      key: 'updated_at',
    },
    {
      title: 'Error Message',
      dataIndex: 'error_message',
      key: 'error_message',
      ellipsis: true,
      render: (text: string) => text || '-',
    },
  ];

  return (
    <div style={{ padding: '24px' }}>
      <Space style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', width: '100%' }}>
        <Space>
        <Select
          placeholder="Select Language"
          allowClear
          onChange={setLang}
          style={{ width: 120 }}
        >
          <Option value="all">All Languages</Option>
          <Option value="en-us">English (en-us)</Option>
          <Option value="zh-cn">简体中文 (zh-cn)</Option>
          <Option value="ru-ru">Русский (ru-ru)</Option>
          <Option value="ja-jp">日本語 (ja-jp)</Option>
          <Option value="fr-fr">Français (fr-fr)</Option>
        </Select>
        <Select
          placeholder="Select Status"
          allowClear
          onChange={setStatus}
          style={{ width: 120 }}
        >
          <Option value="running">Running</Option>
          <Option value="completed">Completed</Option>
          <Option value="failed">Failed</Option>
          <Option value="canceled">Canceled</Option>
        </Select>
        </Space>
        <Tooltip title="Refresh">
          <Button 
            icon={<ReloadOutlined />} 
            onClick={handleRefresh}
            loading={refreshing}
          />
        </Tooltip>
      </Space>

      <Table
        columns={columns}
        dataSource={data}
        rowKey="task_id"
        loading={refreshing}
        pagination={{
          current,
          pageSize,
          onChange: (page, pageSize) => {
            setCurrent(page);
            setPageSize(pageSize);
          },
          showSizeChanger: true,
          showQuickJumper: true,
          showTotal: (total) => `Total ${total} items`,
        }}
      />
    </div>
  );
};

export default TaskList;
