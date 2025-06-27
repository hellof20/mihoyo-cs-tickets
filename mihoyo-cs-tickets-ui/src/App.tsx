import React from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ReactQueryDevtools } from '@tanstack/react-query-devtools';
import { Tabs, ConfigProvider } from 'antd';
import ClusterForm from './components/ClusterForm';
import TaskList from './components/TaskList';
import './App.css';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnMount: true, // Only fetch on first mount
      refetchOnWindowFocus: false, // Disable refetch on window focus
      refetchInterval: false, // Disable polling
      retry: 1,
    },
  },
});

function App() {
  const items = [
    {
      key: '1',
      label: 'Submit Task',
      children: <ClusterForm />,
    },
    {
      key: '2',
      label: 'Task List',
      children: <TaskList />,
    },
  ];

  return (
    <QueryClientProvider client={queryClient}>
      <ConfigProvider
        theme={{
          token: {
            colorPrimary: '#1890ff',
          },
        }}
      >
        <div className="App">
          <header className="App-header">
            <h1>Mihoyo CS Tickets Management</h1>
          </header>
          <main>
            <Tabs defaultActiveKey="1" items={items} />
          </main>
        </div>
      </ConfigProvider>
      <ReactQueryDevtools initialIsOpen={false} />
    </QueryClientProvider>
  );
}

export default App;
