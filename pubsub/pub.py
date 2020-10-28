import logging
import os
from google.cloud import pubsub_v1


def create_topic(project_id, topic_id):
    # create the publisher client
    publisher = pubsub_v1.PublisherClient()
    # The `topic_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/topics/{topic_id}`
    topic_path = publisher.topic_path(project_id, topic_id)
    # create the topic
    topic = publisher.create_topic(topic_path)
    print("Created topic: {}".format(topic.name))


def publish_messages(project_id, topic_id, data_file):
    publisher = pubsub_v1.PublisherClient()
    # The `topic_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/topics/{topic_id}`
    topic_path = publisher.topic_path(project_id, topic_id)
    with open(data_file, errors='replace') as fp:  # open in readonly mode
        lines = fp.readlines()
        for data in lines:
            print(data)
            # Data must be a bytestring
            data = data.encode("utf-8")
            # When you publish a message, the client returns a future.
            future = publisher.publish(topic_path, data)
            print(future.result())

    print(f"Published messages to {topic_path}.")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/s161158/Downloads/dataengineering-course-a9365e622cc2.json"
logging.getLogger().setLevel(logging.INFO)
#create_topic("dataengineering-course", "usdata")
publish_messages("dataengineering-course", "usdata", "twitter_twatter_dummy_splatter.csv")
