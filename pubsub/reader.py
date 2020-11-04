import logging
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import os


def create_subscription(project_id, topic_id, subscription_id):
    """Create a new pull subscription on the given topic."""
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    # Wrap the subscriber in a 'with' block to automatically call close() to
    # close the underlying gRPC channel when done.
    with subscriber:
        subscription = subscriber.create_subscription(subscription_path, topic_path)

    print(f"Subscription created: {subscription}")


def read_data(project_id, subscription_id):
    # project_id = "your-project-id"
    # subscription_id = "your-subscription-id"
    # Number of seconds the subscriber should listen for messages
    # timeout = 5.0
    subscriber = pubsub_v1.SubscriberClient()
    # The `subscription_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/subscriptions/{subscription_id}`
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    def callback(message):
        print(f"Received {message}.")
        message.ack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=10000)
        except TimeoutError:
            streaming_pull_future.cancel()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="dataengineering-course-5fc3ec3747be.json"

logging.getLogger().setLevel(logging.INFO)
# create_subscription("dataengineering-course", "election_data", "election_data_sub")
read_data("dataengineering-course", "election_data_sub")
