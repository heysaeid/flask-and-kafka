import time
import json
import importlib
import threading

from typing import List
from typing import Tuple
from typing import Callable
from enum import Enum

from flask import Flask
from confluent_kafka import DeserializingConsumer
from confluent_kafka import KafkaError
from confluent_kafka import KafkaException

from flask_and_kafka.producer import FlaskKafkaProducer
from .enums import FailedIssueTypeEnum, KafkaStatusEnum, KafkaRetryTopicEnum, KafkaFailTopicEnum
from .log import consumer_logger


class FlaskKafkaConsumer:

    def __init__(self, app: Flask = None) -> None:
        self._app = app
        self.consumers = {}
        self.topics = {}
        self.threads = {}
        self.events = {}
        self._retry = False
        self._retry_and_fail_topics_prefix = None,
        self._producer = None
        self._retry_topics_enum = None
        self._fail_topic_enum = None,

        if app is not None:
            self.init_app(app)

    def init_app(self, app: Flask) -> None:
        self._app = app
        app.extensions['kafka_consumer'] = self
        self.consumer_logger = consumer_logger(name='consumer_logger', file=app.config.get('KAFKA_CONSUMER_LOG_PATH', 'logs/kafka_consumer.log'))

    def init_retry_process(
        self, 
        retry_and_fail_topics_prefix: str,
        producer: FlaskKafkaProducer,
        consumer_retry_helper: 'ConsumerRetryHelper',
        retry_topics_enum: Enum = KafkaRetryTopicEnum,
        fail_topic_enum: Enum = KafkaFailTopicEnum,
    ):
        """
        Note: Type of messages that produced must be dictionary for retry process 
        """
        self._retry = True
        self._retry_and_fail_topics_prefix = retry_and_fail_topics_prefix
        self._producer = producer
        self._retry_topics_enum = retry_topics_enum
        self._fail_topic_enum = fail_topic_enum
        consumer_retry_helper.register_retry_and_fail_consumers()  

    def register_consumers(self, consumers: List[str]) -> None:
        for consumer_name in consumers:
            importlib.import_module(consumer_name)

    def handle_message(self, topic: str, group_id: str, num_consumers: int = 1, app_context: bool = False, retry_attempt_number: int = 0, **kwargs) -> Callable:
        """
            A decorator that registers a message handler function for the given topic and group ID.

            Args:
                topic (str): The Kafka topic to subscribe to.
                group_id (str): The Kafka consumer group ID to use.
                num_consumers (int, optional): The number of Kafka consumer threads to spawn (default is 1).
                app_context (bool, optional): Whether to run the message handler function inside a Flask application context (default is False).
                retry_attempt_number (int, optional): number of retry attempts with considering KafkaRetryTopicEnum
                **kwargs: Additional arguments to pass to the Kafka consumer constructor.

            Returns:
                Callable: A decorator function that wraps the message handler function.

            Usage:
                @flask_kafka.handle_message('my-topic', 'my-group', app_context=True)
                def my_message_handler(msg):
                    # This function will be executed in a Flask application context.
                    db.session.add(MyModel(msg.value()))
                    db.session.commit()
        """
        def decorator(func):

            def with_app_context_handler(msg, func):
                with self._app.app_context():
                    status = func(msg)
                    return status
                
            def wrapped_func(msg):
                if app_context:
                    status = with_app_context_handler(msg, func)
                else:
                    status = func(msg)
                
                if self._retry:
                    consume_config = self.get_consumer_config(topic, msg)
                    current_retry = consume_config.get("retry_attempt_count", 0)
                    if status == KafkaStatusEnum.retry.value:
                        self.retry_again(
                            retry_attempt_number,
                            current_retry,
                            topic,
                            msg.value(),
                        )
                    elif status == KafkaStatusEnum.failed.value:
                        self.send_to_failed_topic(topic)

            if group_id not in self.consumers:
                self.consumers[group_id] = []
                self.topics[group_id] = []

            for i in range(num_consumers):
                if not self._app.config['KAFKA_CONSUMER_CONFIGS'].get('value.deserializer'):
                    self._app.config['KAFKA_CONSUMER_CONFIGS']['value.deserializer'] = self.json_deserializer
                consumer = DeserializingConsumer({
                    **self._app.config['KAFKA_CONSUMER_CONFIGS'],
                    'group.id': group_id,
                    'enable.auto.commit': False,
                    'auto.offset.reset': 'earliest',
                    **kwargs,
                })
                consumer.subscribe([topic])
                self.consumers[group_id].append(consumer)
                self.topics[group_id].append((topic, wrapped_func))

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

    def _consume_messages(self, consumer: DeserializingConsumer, topics: List[Tuple[str, Callable]], event: threading.Event) -> None:
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

                self._call_message_handlers(msg, topics)
                    
                consumer.commit(asynchronous=True) # commit offsets after processing the batch of messages
        except KafkaException as e:
            print('Exception in Kafka consumer thread: %s', e)            
        finally:
            consumer.close() # close the consumer when the thread is terminated

    def _call_message_handlers(self, msg, topics):
        for topic, func in topics:
            if msg.topic() == topic:
                self.consumer_logger.info('', extra={"consumer_message": msg})
                func(msg)
                break
    
    def retry_again(
        self, 
        retry_attempt_number: int,
        current_retry: int,
        topic: str, 
        msg: dict,
    ) -> None:
        if retry_attempt_number > 0:
            if current_retry < retry_attempt_number:
                retry_topic, retry_time = self._retry_topics_enum.retry_topics_attempt_time_map.value[current_retry]
                self._producer.send_message(
                    self.get_topic_with_prefix(retry_topic),
                    {
                        "consume_config": {
                            "topic": topic,
                            "retry_attempt_count": current_retry,
                            "retry_timestamp": time.time() + retry_time,
                        },
                        **msg 
                    }
                )
            else:
                self.send_to_failed_topic(topic)

    def get_consumer_config(self, topic: str, msg: dict) -> tuple[dict, str]:
        if topic in [item[0] for item in self._retry_topics_enum.retry_topics_attempt_time_map.value.values()]:
            consume_config = msg.value().get("consume_config", {})
        else:
            consume_config = msg.value().pop("consume_config", {})
        
        return consume_config
    
    def send_to_failed_topic(self, topic: str):
        self.send_to_kafka_failed_topic(
            issue_type = FailedIssueTypeEnum.consumer,
            topic = topic,
        )
    
    def send_to_kafka_failed_topic(self, issue_type = FailedIssueTypeEnum, **kwargs):
        self._producer.send_message(
            self.get_topic_with_prefix(self._fail_topic_enum.failed_topic.value), 
            {
                "issue_type": issue_type,
                **kwargs,
            }
        )
    
    def get_topic_with_prefix(self, topic):
        if self._retry_and_fail_topics_prefix:
            return f"{self._retry_and_fail_topics_prefix}{topic}"
        return topic
    
    def json_deserializer(self, msg, s_obj):
        try:
            msg = json.loads(msg)
        except Exception as e:
            ...
        return msg


class ConsumerRetryHelper:

    def __init__(
        self, 
        kafka_consumer: FlaskKafkaConsumer, 
        consumer_group_postfix: str = '_consumer_group'
    ):
        self.kafka_consumer = kafka_consumer
        self.consumer_group_postfix = consumer_group_postfix
        
    def retry_process(self, msg: dict):
        try:
            consumer_config = msg["consume_config"]
            msg["consume_config"]["retry_attempt_count"] += 1 
            time.sleep(max(consumer_config["retry_timestamp"] - time.time(), 0))
            self.kafka_consumer._producer.send_message(consumer_config["topic"], msg)
        except Exception as e:
            return KafkaStatusEnum.failed.value
        return KafkaStatusEnum.success.value
    
    def register_retry_and_fail_consumers(self):
        self._register_retry_consumers()
        self._register_fail_consumers()

    def _register_retry_consumers(self):
        @self.kafka_consumer.handle_message(
            topic=self.kafka_consumer.get_topic_with_prefix(self.kafka_consumer._retry_topics_enum.retry_5m_topic.value),
            group_id=f"{self.kafka_consumer.get_topic_with_prefix(self.kafka_consumer._retry_topics_enum.retry_5m_topic.value)}{self.consumer_group_postfix}"
        )
        def retry_5m_consumer(msg: dict):
            return self.retry_process(msg.value())
    
        @self.kafka_consumer.handle_message(
            topic=self.kafka_consumer.get_topic_with_prefix(self.kafka_consumer._retry_topics_enum.retry_10m_topic.value),
            group_id=f"{self.kafka_consumer.get_topic_with_prefix(self.kafka_consumer._retry_topics_enum.retry_10m_topic.value)}{self.consumer_group_postfix}"
        )
        def retry_10m_consumer(msg: dict):
            return self.retry_process(msg.value())
    
        @self.kafka_consumer.handle_message(
            topic=self.kafka_consumer.get_topic_with_prefix(self.kafka_consumer._retry_topics_enum.retry_15m_topic.value), 
            group_id=f"{self.kafka_consumer.get_topic_with_prefix(self.kafka_consumer._retry_topics_enum.retry_15m_topic.value)}{self.consumer_group_postfix}"
        )
        def retry_15m_consumer(msg: dict):
            return self.retry_process(msg.value())
        
        @self.kafka_consumer.handle_message(
            topic=self.kafka_consumer.get_topic_with_prefix(self.kafka_consumer._retry_topics_enum.retry_30m_topic.value), 
            group_id=f"{self.kafka_consumer.get_topic_with_prefix(self.kafka_consumer._retry_topics_enum.retry_30m_topic.value)}{self.consumer_group_postfix}"
        )
        def retry_30m_consumer(msg: dict):
            return self.retry_process(msg.value())
        
        @self.kafka_consumer.handle_message(
            topic=self.kafka_consumer.get_topic_with_prefix(self.kafka_consumer._retry_topics_enum.retry_1h_topic.value),
            group_id=f"{self.kafka_consumer.get_topic_with_prefix(self.kafka_consumer._retry_topics_enum.retry_1h_topic.value)}{self.consumer_group_postfix}"
        )
        def retry_1h_consumer(msg: dict):
            return self.retry_process(msg.value())
    
    def _register_fail_consumers(self):
        @self.kafka_consumer.handle_message(
            topic=self.kafka_consumer.get_topic_with_prefix(self.kafka_consumer._fail_topic_enum.failed_topic.value), 
            group_id=f"{self.kafka_consumer.get_topic_with_prefix(self.kafka_consumer._fail_topic_enum.failed_topic.value)}{self.consumer_group_postfix}"
        )
        def failed_consumer(msg: dict):
            print('Failed message: ' + str(msg.value()))