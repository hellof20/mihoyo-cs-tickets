import React from 'react';
import { Form, DatePicker, Select, Button, message } from 'antd';
import { useMutation } from '@tanstack/react-query';
import { submitClusterIssues } from '../services/api';
import { ClusterRequest } from '../types';

const { RangePicker } = DatePicker;

const ClusterForm: React.FC = () => {
  const [form] = Form.useForm();

  const mutation = useMutation({
    mutationFn: submitClusterIssues,
    onMutate: () => {
      message.loading({
        content: 'Creating task...',
        duration: 0,
        key: 'taskCreation',
        style: {
          marginTop: '20vh',
        },
      });
    },
    onSuccess: (data) => {
      message.destroy('taskCreation');
      message.success({
        content: `Task created successfully (ID: ${data.task_id})`,
        duration: 5,
        style: {
          marginTop: '20vh',
        },
      });
      form.resetFields();
    },
    onError: (error) => {
      message.destroy('taskCreation');
      message.error({
        content: `Failed to create task: ${error.message}`,
        duration: 5,
        style: {
          marginTop: '20vh',
        },
      });
    },
    retry: 0, // Don't retry on failure
  });

  const onFinish = (values: any) => {
    console.log('Form values received:', JSON.stringify(values, null, 2));
    
    if (!values.dateRange || !values.dateRange[0]) {
      console.log('Date range is missing or invalid');
      return;
    }

    try {
      const request: ClusterRequest = {
        business: values.business,
        startDate: values.dateRange[0].format('YYYY-MM-DD'),
        endDate: values.dateRange[1].format('YYYY-MM-DD'),
        lang: values.lang,
      };
      console.log('Submitting cluster request:', JSON.stringify(request, null, 2));
      mutation.mutate(request);
    } catch (error) {
      message.error({
        content: 'Failed to create request: Invalid form data',
        duration: 5,
        style: {
          marginTop: '20vh',
        },
      });
      console.error('Error creating request:', error);
    }
  };

  return (
    <Form
      form={form}
      name="clusterForm"
      onFinish={onFinish}
      onFinishFailed={(errorInfo) => {
        console.log('Form validation failed:', JSON.stringify(errorInfo, null, 2));
      }}
      onValuesChange={(changedValues, allValues) => {
        console.log('Form values changed:', JSON.stringify(changedValues, null, 2));
        console.log('All form values:', JSON.stringify(allValues, null, 2));
      }}
      layout="vertical"
      style={{ maxWidth: 600, margin: '0 auto', padding: '24px' }}
    >
      <Form.Item
        name="business"
        label="Business"
        rules={[{ required: true, message: 'Please select business!' }]}
      >
          <Select>
          <Select.Option value="绝区零">绝区零</Select.Option>
        </Select>
      </Form.Item>

      <Form.Item
        name="dateRange"
        label="Date Range"
        rules={[{ required: true, message: 'Please select date range!' }]}
      >
        <RangePicker style={{ width: '100%' }} />
      </Form.Item>

      <Form.Item
        name="lang"
        label="Language"
        rules={[{ required: true, message: 'Please select language!' }]}
      >
        <Select>
          <Select.Option value="all">All Languages</Select.Option>
          <Select.Option value="English(en-us)">English (en-us)</Select.Option>
          <Select.Option value="简体中文(zh-cn)">简体中文 (zh-cn)</Select.Option>
          <Select.Option value="俄罗斯语(ru-ru)">Русский (ru-ru)</Select.Option>
          <Select.Option value="日本語(ja-jp)">日本語 (ja-jp)</Select.Option>
          <Select.Option value="法语(fr-fr)">Français (fr-fr)</Select.Option>
        </Select>
      </Form.Item>

      <Form.Item>
        <Button type="primary" htmlType="submit" loading={mutation.isPending} block>
          Submit
        </Button>
      </Form.Item>
    </Form>
  );
};

export default ClusterForm;
