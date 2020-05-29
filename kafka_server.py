"""Execute kafka producer server."""

import producer_server


def run_kafka_server():
    """Rub kafka server."""

    input_file = "data/police-department-calls-for-service.json"

    # TODO fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic_name="sf.crime.stat.topic",
        bootstrap_servers='127.0.0.1:9092',
        client_id='sf-producer'
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
