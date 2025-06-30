import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { Table, Tag, Space, Button } from 'antd';
import { DownloadOutlined } from '@ant-design/icons';
import { useParams, useNavigate } from 'react-router-dom';
import { getFaq, FaqItem } from '../services/api';

const FaqPage: React.FC = () => {
  const { taskId } = useParams<{ taskId: string }>();
  const navigate = useNavigate();

  const handleDownload = () => {
    if (!faqData) return;
    
    // Convert data to CSV format
    const headers = faqColumns
      .filter(col => col.key !== 'action') // Exclude action column
      .map(col => col.title)
      .join(',');
    
    const rows = faqData.map(row => {
      return faqColumns
        .filter(col => col.key !== 'action') // Exclude action column
        .map(col => {
          const value = row[col.dataIndex as keyof typeof row];
          // Handle special cases like business and num_tickets that have Tag render
          const stringValue = typeof value === 'string' || typeof value === 'number' 
            ? value.toString() 
            : JSON.stringify(value);
          // Escape commas and quotes in the value
          return `"${stringValue.replace(/"/g, '""')}"`;
        })
        .join(',');
    });
    
    const csvContent = [headers, ...rows].join('\n');
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    link.setAttribute('href', url);
    link.setAttribute('download', `faq_${taskId}_data.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const { data: faqData, isLoading } = useQuery<FaqItem[]>({
    queryKey: ['faq', taskId],
    queryFn: () => getFaq(taskId!),
    enabled: !!taskId,
  });

  const faqColumns = [
    {
      title: 'Cluster ID',
      dataIndex: 'cluster_id',
      key: 'cluster_id',
      width: 120,
    },
    {
      title: 'Business',
      dataIndex: 'business',
      key: 'business',
      width: 120,
      render: (business: string) => (
        <Tag color="blue">{business}</Tag>
      ),
    },
    {
      title: 'Tickets',
      dataIndex: 'num_tickets',
      key: 'num_tickets',
      width: 100,
      render: (num: number) => (
        <Tag color="green">{num} tickets</Tag>
      ),
    },
    {
      title: 'Summary',
      dataIndex: 'summarized',
      key: 'summarized',
    },
    {
      title: 'Action',
      key: 'action',
      width: 120,
      render: (_: any, record: FaqItem) => (
        <Button 
          type="primary"
          onClick={() => navigate(`/cluster/${record.cluster_id}`)}
        >
          View Detail
        </Button>
      ),
    },
  ];

  return (
    <div style={{ padding: '24px' }}>
      <Space style={{ marginBottom: 16 }}>
        <Button onClick={() => navigate(-1)}>Back to Task List</Button>
        <Button
          type="primary"
          icon={<DownloadOutlined />}
          onClick={handleDownload}
        >
          Download Data
        </Button>
      </Space>
      <Table
        columns={faqColumns}
        dataSource={faqData}
        rowKey="cluster_id"
        loading={isLoading}
        pagination={false}
      />
    </div>
  );
};

export default FaqPage;
