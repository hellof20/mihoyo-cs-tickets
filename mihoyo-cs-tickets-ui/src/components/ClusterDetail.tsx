import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useParams, useNavigate } from 'react-router-dom';
import { Button, Card, Tag, Space, Spin, Table } from 'antd';
import { DownloadOutlined } from '@ant-design/icons';
import { getClusterDetail } from '../services/api';
import { Resizable } from 'react-resizable';
import type { TableProps } from 'antd';

const ResizableTitle = (props: any) => {
  const { onResize, width, ...restProps } = props;

  if (!width) {
    return <th {...restProps} />;
  }

  return (
    <Resizable
      width={width}
      height={0}
      handle={
        <span
          className="react-resizable-handle"
          onClick={(e) => {
            e.stopPropagation();
          }}
        />
      }
      onResize={onResize}
      draggableOpts={{ enableUserSelectHack: false }}
    >
      <th {...restProps} />
    </Resizable>
  );
};

const ClusterDetail: React.FC = () => {
  const { clusterId } = useParams<{ clusterId: string }>();
  const navigate = useNavigate();

  const handleDownload = () => {
    if (!clusterData) return;
    
    // Convert data to CSV format
    const headers = columns.map(col => col.title).join(',');
    const rows = clusterData.map(row => {
      return columns.map(col => {
        const value = row[col.dataIndex as keyof typeof row];
        // Handle special cases like language that has a Tag render
        const stringValue = typeof value === 'string' ? value : JSON.stringify(value);
        // Escape commas and quotes in the value
        return `"${stringValue.replace(/"/g, '""')}"`;
      }).join(',');
    });
    
    const csvContent = [headers, ...rows].join('\n');
    // Add UTF-8 BOM to help Excel recognize the encoding
    const BOM = '\uFEFF';
    const blob = new Blob([BOM + csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    link.setAttribute('href', url);
    link.setAttribute('download', `cluster_${clusterId}_data.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const [columns, setColumns] = useState([
    {
      title: 'Ticket ID',
      dataIndex: 'ticket_id',
      key: 'ticket_id',
      width: 120,
    },
    {
      title: 'Language',
      dataIndex: 'ticket_language',
      key: 'ticket_language',
      width: 100,
      render: (lang: string) => <Tag color="blue">{lang}</Tag>,
    },
    {
      title: 'Date',
      dataIndex: 'dt',
      key: 'dt',
      width: 120,
    },
    {
      title: 'Player Issue',
      dataIndex: 'player_issue_description',
      key: 'player_issue_description',
      width: 300,
    },
    {
      title: 'User Issue',
      dataIndex: 'user_issue',
      key: 'user_issue',
    },
  ]);

  const { data: clusterData, isLoading } = useQuery({
    queryKey: ['cluster', clusterId],
    queryFn: () => getClusterDetail(clusterId!),
    enabled: !!clusterId,
  });

  if (isLoading) {
    return (
      <div style={{ padding: '24px', textAlign: 'center' }}>
        <Spin size="large" />
      </div>
    );
  }

  if (!clusterData) {
    return (
      <div style={{ padding: '24px' }}>
        <Space direction="vertical">
          <div>Cluster not found</div>
          <Button onClick={() => navigate(-1)}>Go Back</Button>
        </Space>
      </div>
    );
  }

  const handleResize =
    (index: number) =>
    (_: any, { size }: any) => {
      const newColumns = [...columns];
      newColumns[index] = {
        ...newColumns[index],
        width: size.width,
      };
      setColumns(newColumns);
    };

  const mergedColumns = columns.map((col, index) => ({
    ...col,
    onHeaderCell: (column: any) => ({
      width: column.width,
      onResize: handleResize(index),
    }),
  }));

  return (
    <div style={{ padding: '24px' }}>
      <Space style={{ marginBottom: 16 }}>
        <Button onClick={() => navigate(-1)}>Back</Button>
        <Button 
          type="primary"
          icon={<DownloadOutlined />}
          onClick={handleDownload}
        >
          Download Data
        </Button>
      </Space>
      
      <Card title={`Cluster ${clusterId}`}>
        <Table
          bordered
          components={{
            header: {
              cell: ResizableTitle,
            },
          }}
          dataSource={clusterData}
          rowKey="ticket_id"
          columns={mergedColumns as TableProps['columns']}
          scroll={{ x: true }}
          pagination={{ pageSize: 10 }}
        />
      </Card>
    </div>
  );
};

export default ClusterDetail;
