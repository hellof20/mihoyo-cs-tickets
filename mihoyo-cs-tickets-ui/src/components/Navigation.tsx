import React from 'react';
import { Menu } from 'antd';
import { useLocation, useNavigate } from 'react-router-dom';

const Navigation: React.FC = () => {
  const location = useLocation();
  const navigate = useNavigate();

  const items = [
    {
      key: '/submit',
      label: 'Submit Task',
    },
    {
      key: '/tasks',
      label: 'Task List',
    },
  ];

  return (
    <Menu
      mode="horizontal"
      selectedKeys={[location.pathname]}
      items={items}
      onClick={({ key }) => navigate(key)}
      style={{ marginBottom: '16px' }}
    />
  );
};

export default Navigation;
