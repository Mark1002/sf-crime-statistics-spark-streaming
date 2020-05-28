"""Kafka consumer."""
from log_config import logging

logger = logging.getLogger(__name__)


def main():
    """Main."""
    logger.debug('hello!')


if __name__ == "__main__":
    main()
