import React from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ReactQueryDevtools } from '@tanstack/react-query-devtools';
import { ConfigProvider, message } from 'antd';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import ClusterForm from './components/ClusterForm';
import TaskList from './components/TaskList';
import FaqPage from './components/FaqPage';
import ClusterDetail from './components/ClusterDetail';
import Navigation from './components/Navigation';
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
  const [messageApi, contextHolder] = message.useMessage();

  return (
    <QueryClientProvider client={queryClient}>
      <ConfigProvider
        theme={{
          token: {
            colorPrimary: '#1890ff',
          },
        }}
      >
        {contextHolder}
        <BrowserRouter>
          <div className="App">
            <header className="App-header">
              <h1>Mihoyo CS Tickets Management</h1>
              <Navigation />
            </header>
            <main>
              <Routes>
                <Route path="/" element={<Navigate to="/submit" replace />} />
                <Route path="/submit" element={<ClusterForm messageApi={messageApi} />} />
                <Route path="/tasks" element={<TaskList />} />
                <Route path="/faq/:taskId" element={<FaqPage />} />
                <Route path="/cluster/:clusterId" element={<ClusterDetail />} />
              </Routes>
            </main>
          </div>
        </BrowserRouter>
      </ConfigProvider>
      <ReactQueryDevtools initialIsOpen={false} />
    </QueryClientProvider>
  );
}

export default App;
