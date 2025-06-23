import toml
from google import pubsub_v1

class PubSubHandler:
    def __init__(self, config_path: str = "config.toml"):
        with open(config_path, "r") as f:
            config = toml.load(f)
        self.project_id = config['app']['project_id']
        self.subscription_name = config['pubsub']['subscription_name']
        self.subscriber_client = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber_client.subscription_path(self.project_id, self.subscription_name)

    def pull_message(self, max_messages: int = 1) -> pubsub_v1.types.PullResponse:
        print(f"从订阅 {self.subscription_path} 拉取消息...")
        request = pubsub_v1.PullRequest(
            subscription=self.subscription_path,
            max_messages=max_messages,
        )
        response = self.subscriber_client.pull(request=request)
        return response

    def acknowledge_message(self, ack_id: str) -> None:
        print(f"正在确认消息 (ack_id: {ack_id[:10]}...)...")
        request = pubsub_v1.AcknowledgeRequest(
            subscription=self.subscription_path,
            ack_ids=[ack_id],
        )
        self.subscriber_client.acknowledge(request=request)
        print("消息已确认。")
