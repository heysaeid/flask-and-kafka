import threading
from typing import List
from typing import Tuple
from typing import Callable
from flask import Flask
from confluent_kafka import Consumer
from confluent_kafka import KafkaError
from confluent_kafka import KafkaException
from .log import consumer_logger


class FlaskKafkaConsumer:

    def __init__(self, app: Flask = None) -> None:
        self._app = app
        self.consumers = {}
        self.topics = {}
        self.threads = {}
        self.events = {}

        if app is not None:
            self.init_app(app)

    def init_app(self, app: Flask) -> None:
        self._app = app
        app.extensions['kafka_consumer'] = self
        self.consumer_logger = consumer_logger(name='consumer_logger', file=app.config.get('KAFKA_CONSUMER_LOG_PATH', 'logs/kafka_consumer.log'))

    def handle_message(self, topic: str, group_id: str, num_consumers: int = 1, **kwargs) -> Callable:
        def decorator(func):
            if group_id not in self.consumers:
                self.consumers[group_id] = []
                self.topics[group_id] = []

            for i in range(num_consumers):
                consumer = Consumer({
                    **self._app.config['KAFKA_CONSUMER_CONFIGS'],
                    'group.id': group_id,
                    'enable.auto.commit': False,
                    'auto.offset.reset': 'earliest',
                    **kwargs,
                })
                consumer.subscribe([topic])
                self.consumers[group_id].append(consumer)
                self.topics[group_id].append((topic, func))


            def wrapped_func(msg):
                func(msg)

            return wrapped_func

        return decorator
    
    def start(self) -> None:
        for group_id, consumers in self.consumers.items():
            for i, consumer in enumerate(consumers):
                key = f'{group_id}-{i}'
                self.events[key] = threading.Event()
                self.threads[key] = threading.Thread(
                    target=self._consume_messages, 
                    args=(consumer, self.topics[group_id], self.events[key])
                )
                self.threads[key].start()

    def stop(self) -> None:
        for event in self.events.values():
            event.set()

        for thread in self.threads.values():
            thread.join()

        self.consumers.clear()
        self.topics.clear()

    def _consume_messages(self, consumer: Consumer, topics: List[Tuple[str, Callable]], event: threading.Event) -> None:
        try:
            while not event.is_set():
                msg = consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())

                for topic, func in topics:
                    if msg.topic() == topic:
                        self.consumer_logger.info('', extra={"consumer_message": msg})
                        func(msg)
                        break
                    
                consumer.commit(asynchronous=True) # commit offsets after processing the batch of messages
        except KafkaException as e:
            print('Exception in Kafka consumer thread: %s', e)            
        finally:
            consumer.close() # close the consumer when the thread is terminated
