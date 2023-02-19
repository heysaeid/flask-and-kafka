from flask import Flask
from confluent_kafka import Producer
from confluent_kafka import KafkaError


class FlaskKafkaProducer:

    def __init__(self, app: Flask = None) -> None:
        self.producer = None

        if app is not None:
            self.init_app(app)

    def init_app(self, app: Flask) -> None:
        app.extensions['kafka_producer'] = self
        self.producer = Producer(app.config['KAFKA_PRODUCER_CONFIGS'])

    def send_message(self, topic: str, message: any, flush: bool = False, poll: bool = True, poll_timeout = 1, **kwargs) -> None:
        try:
            self.producer.produce(topic=topic, value=message, **kwargs)
        except KafkaError as e:
            print(f'Error producing message to topic {topic}: {e}')
        else:
            if flush:
                self.producer.flush()
            if poll:
                self.producer.poll(poll_timeout)
        
    def close(self):
        self.producer.flush()
        self.producer.poll(0)