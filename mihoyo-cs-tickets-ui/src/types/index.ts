export interface ClusterRequest {
  business: string;
  startDate: string;
  endDate: string;
  lang: string;
}

export interface TaskStatus {
  task_id: string;
  business: string;
  start_date: string;
  end_date: string;
  lang: string;
  status: string;
  created_at: string;
  updated_at: string;
  error_message: string;
}

export interface TaskListParams {
  limit?: number;
  offset?: number;
  lang?: string;
  status?: string;
}
