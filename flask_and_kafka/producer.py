from flask import Flask
from confluent_kafka import Producer
from confluent_kafka import KafkaError
from .log import producer_logger


class FlaskKafkaProducer:

    def __init__(self, app: Flask = None) -> None:
        self.producer = None

        if app is not None:
            self.init_app(app)

    def init_app(self, app: Flask) -> None:
        app.extensions['kafka_producer'] = self
        self.producer = Producer(app.config['KAFKA_PRODUCER_CONFIGS'])
        self.producer_logger = producer_logger(name='producer_logger', file=app.config.get('KAFKA_PRODUCER_LOG_PATH', 'logs/kafka_producer.log'))

    def send_message(self, topic: str, value: any, key: str = None, flush: bool = False, poll: bool = True, poll_timeout = 1, **kwargs) -> None:
        error = None
        try:
            self.producer.produce(topic=topic, key=key, value=value, **kwargs)
        except KafkaError as e:
            error = f'Error producing message to topic {topic}: {e}'
        else:
            if flush:
                self.producer.flush()
            if poll:
                self.producer.poll(poll_timeout)
        finally:
            self.producer_logger.info('', extra={
                'producer_log': {
                    'topic': topic,
                    'key': key,
                    'value': value,
                    'flush':flush,
                    'poll': poll,
                    'poll_timeout': poll_timeout,
                    'error': error,
                    'extra': {**kwargs}
                }
            })
        
    def close(self):
        self.producer.flush()
        self.producer.poll(0)