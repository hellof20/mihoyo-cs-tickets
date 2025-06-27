import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { useParams, useNavigate } from 'react-router-dom';
import { Button, Card, Tag, Space, Spin, Table } from 'antd';
import { getClusterDetail, ClusterDetailItem } from '../services/api';

const ClusterDetail: React.FC = () => {
  const { clusterId } = useParams<{ clusterId: string }>();
  const navigate = useNavigate();

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

  return (
    <div style={{ padding: '24px' }}>
      <Space style={{ marginBottom: 16 }}>
        <Button onClick={() => navigate(-1)}>Back</Button>
      </Space>
      
      <Card title={`Cluster ${clusterId}`}>
        <Table
          dataSource={clusterData}
          rowKey="ticket_id"
          columns={[
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
          ]}
          scroll={{ x: true }}
          pagination={{ pageSize: 10 }}
        />
      </Card>
    </div>
  );
};

export default ClusterDetail;
