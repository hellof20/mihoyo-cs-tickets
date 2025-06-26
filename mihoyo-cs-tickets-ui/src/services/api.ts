import axios from 'axios';
import { ClusterRequest, TaskStatus, TaskListParams } from '../types';

const API_BASE_URL = 'http://localhost:8000';

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 10000, // 10 seconds timeout
  headers: {
    'Content-Type': 'application/json',
  },
});

export const submitClusterIssues = async (request: ClusterRequest): Promise<TaskStatus> => {
  console.log('API Service - Sending request:', JSON.stringify(request, null, 2));
  try {
    const response = await api.post('/cluster_issues', request);
    console.log('API Service - Response received:', JSON.stringify(response.data, null, 2));
    return response.data;
  } catch (error: any) {
    console.error('API Service - Error:', error);
    if (error.code === 'ECONNABORTED') {
      throw new Error('Request timed out. Please check if the server is running.');
    }
    if (!error.response) {
      throw new Error('Cannot connect to server. Please check if it is running at ' + API_BASE_URL);
    }
    throw error.response?.data?.message || error.message || 'An unknown error occurred';
  }
};

export const getTaskStatus = async (taskId: string): Promise<TaskStatus> => {
  const response = await api.get(`/tasks/${taskId}`);
  return response.data;
};

export const listTasks = async (params: TaskListParams): Promise<TaskStatus[]> => {
  const response = await api.get('/tasks', { params });
  return response.data;
};
