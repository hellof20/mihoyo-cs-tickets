import React, { useState } from 'react';
import { Table, Tag, Space, Select, Button, Tooltip, Typography } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { listTasks } from '../services/api';
import { TaskStatus } from '../types';

const { Option } = Select;
const { Text } = Typography;

const TaskList: React.FC = () => {
  const [pageSize, setPageSize] = useState(10);
  const [current, setCurrent] = useState(1);
  const [refreshing, setRefreshing] = useState(false);
  const navigate = useNavigate();

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
    queryKey: ['tasks', current, pageSize],
    queryFn: () => listTasks({
      limit: pageSize,
      offset: (current - 1) * pageSize
    }),
    enabled: false, // Disable automatic fetching
  });

  const handleViewFaq = (taskId: string) => {
    navigate(`/faq/${taskId}`);
  };

  const handleViewDetail = (clusterId: string) => {
    navigate(`/cluster/${clusterId}`);
  };

  const taskColumns = [
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
                   status === 'success' ? 'success' : 
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
    {
      title: 'Actions',
      key: 'actions',
      render: (_: any, record: TaskStatus) => (
        <Button 
          type="link" 
          onClick={() => handleViewFaq(record.task_id)}
          disabled={record.status !== 'success'}
        >
          View FAQ
        </Button>
      ),
    },
  ];


  return (
    <div style={{ padding: '24px' }}>
      <Space style={{ marginBottom: 16, display: 'flex', justifyContent: 'flex-end', width: '100%' }}>
        <Tooltip title="Refresh">
          <Button 
            icon={<ReloadOutlined />} 
            onClick={handleRefresh}
            loading={refreshing}
          />
        </Tooltip>
      </Space>

      <Table
        columns={taskColumns}
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
