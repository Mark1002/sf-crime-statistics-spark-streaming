"""Kafka producer."""
import json
import time

from log_config import logging
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer

logger = logging.getLogger(__name__)


class ProducerServer:

    def __init__(self, input_file: str, topic_name: str, **kwargs):
        self.p = Producer({
            'bootstrap.servers': kwargs['bootstrap_servers'],
            'client.id': kwargs['client_id']
        })
        self.input_file = input_file
        self.topic_name = topic_name
        self.client = AdminClient(
            {'bootstrap.servers': kwargs['bootstrap_servers']}
        )

    def check_topic_exists(self) -> bool:
        """Checks if the given topic exists in Kafka."""
        topic_metadata = self.client.list_topics(timeout=5)
        return self.topic_name in set(t.topic for t in iter(topic_metadata.topics.values())) # noqa

    def create_topics(self):
        """Create kafka topic."""
        new_topics = [
            NewTopic(self.topic_name, num_partitions=3, replication_factor=1)
        ]
        result = self.client.create_topics(new_topics)
        for topic, f in result.items():
            try:
                f.result()  # The result itself is None
                logger.debug(f"Topic {topic} created")
            except Exception as e:
                logger.error(f"Failed to create topic {topic}: {e}")

    def delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            logger.debug('Message delivery failed: {}'.format(err))
        else:
            logger.debug('Message delivered to {} [{}]'.format(msg.topic(), msg.partition())) # noqa

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return str(round(time.time() * 1000))

    def produce(self, message):
        """Produce record to kafka."""
        logger.debug(f'message: {message}')
        while True:
            try:
                self.p.produce(
                    self.topic_name,
                    key=self.time_millis(),
                    value=self.dict_to_binary(message),
                    on_delivery=self.delivery_report
                )
                self.p.poll(0)
                break
            except BufferError as e:
                logger.error(e)
                self.p.poll(1)

    # TODO we're generating a dummy data
    def generate_data(self):
        # check if topic is exist or create
        if not self.check_topic_exists():
            self.create_topics()
        else:
            logger.debug(f'topic {self.topic_name} alreadly exist!')
        # read data from file
        with open(self.input_file, 'r') as f:
            messages = json.loads(f.read())
        # TODO send the correct data
        try:
            for message in messages:
                self.produce(message)
        except KeyboardInterrupt:
            logger.debug('aborted by user.')
        finally:
            self.p.flush()

    # TODO fill this in to return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        """Json dictionary to binary."""
        return json.dumps(json_dict)
