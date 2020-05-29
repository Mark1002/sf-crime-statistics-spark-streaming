"""Consumer for kafka."""
import json
from log_config import logging
from confluent_kafka import Consumer, KafkaException, OFFSET_BEGINNING

logger = logging.getLogger(__name__)


class ConsumerServer:

    def __init__(self, topic_name: str, config: dict):
        """Init."""
        self.c = Consumer(config)
        self.topic_name = topic_name

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place."""
        # reset all partition's consumer offeset to beginning
        for partition in partitions:
            partition.offset = OFFSET_BEGINNING
        consumer.assign(partitions)

    def consume(self, reset_offset_beginning: bool = False):
        """Perform consumer."""
        if reset_offset_beginning:
            self.c.subscribe([self.topic_name], on_assign=self.on_assign)
        else:
            self.c.subscribe([self.topic_name])
        try:
            while True:
                msg = self.c.poll(timeout=1.0)
                if msg is None:
                    logger.debug('Waiting for message or event/error in poll()') # noqa
                elif msg.error():
                    raise KafkaException(msg.error())
                else:
                    logger.debug(f'{msg.topic()}[{msg.partition()}] at offset {msg.offset()}, key: {msg.key()}') # noqa
                    message = json.loads(msg.value())
                    logger.debug(message)
        except KeyboardInterrupt:
            logger.info('aborted by user.')
        finally:
            self.c.close()


def main():
    """Main."""
    config = {
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': 'local-consumer',
        'session.timeout.ms': 6000,
        'auto.offset.reset': 'earliest'
    }
    consumer = ConsumerServer(
        topic_name='sf.crime.stat.topic',
        config=config
    )
    consumer.consume(reset_offset_beginning=False)


if __name__ == '__main__':
    main()
