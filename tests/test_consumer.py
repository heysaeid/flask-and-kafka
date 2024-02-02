import unittest
from unittest import mock

import confluent_kafka
from flask import Flask

from flask_and_kafka import FlaskKafkaConsumer


class TestFlaskKafkaConsumer(unittest.TestCase):
    topic = "test_topic"
    group_id = "test_group"
    num_consumers = 1

    def setUp(self):
        self.app = Flask(__name__)
        self.app.config["KAFKA_CONSUMER_CONFIGS"] = {
            "bootstrap.servers": "localhost:9092",
            "auto.offset.reset": "earliest",
        }
        self.consumer = FlaskKafkaConsumer(self.app)

    def test_handle_message_decorator(self):
        @self.consumer.handle_message(
            topic=self.topic,
            group_id=self.group_id,
            num_consumers=self.num_consumers,
        )
        def test_handler(msg):
            pass

        self.assertEqual(len(self.consumer.consumers), self.num_consumers)
        self.assertEqual(len(self.consumer.topics[self.group_id]), 1)

    def test_handle_message_decorator_with_5_num_consumers(self):
        @self.consumer.handle_message(self.topic, self.group_id, 5)
        def test_handler(msg):
            pass

        self.assertEqual(len(self.consumer.consumers), self.num_consumers)

    def test_handle_message_decorator_with_2_different_consumer(self):
        @self.consumer.handle_message(self.topic, self.group_id, 1)
        def test_handler(msg):
            pass

        @self.consumer.handle_message(
            self.topic + "_1", self.group_id + "_1", 1
        )
        def test_handler2(msg):
            pass

        self.assertEqual(len(self.consumer.consumers), 2)
        self.assertEqual(len(self.consumer.topics), 2)

    def test_call_message_handler(self):
        handler_was_called = False

        @self.consumer.handle_message(self.topic, self.group_id, 1)
        def test_handler(msg):
            nonlocal handler_was_called
            handler_was_called = True
            self.assertEqual(msg.topic(), self.topic)
            self.assertEqual(msg.partition(), 0)
            self.assertEqual(msg.offset(), 1)
            self.assertEqual(msg.value(), b"test value")
            self.assertEqual(msg.key(), b"key value")
            self.assertEqual(msg.error(), None)
            self.assertEqual(msg.headers(), {"first": "one", "second": 2})

        msg = mock.Mock(confluent_kafka.Message)
        msg.topic.return_value = self.topic
        msg.partition.return_value = 0
        msg.offset.return_value = 1
        msg.value.return_value = b"test value"
        msg.key.return_value = b"key value"  # can be bytes or string.
        msg.headers.return_value = {"first": "one", "second": 2}
        msg.error.return_value = None

        self.consumer._call_message_handlers(
            msg, self.consumer.topics[self.group_id]
        )
        self.assertTrue(handler_was_called, "handler was not called")

    def test_start_stop(self):
        @self.consumer.handle_message(
            self.topic, self.group_id, self.num_consumers
        )
        def test_handler(msg):
            pass

        with mock.patch.object(
            self.consumer, "_consume_messages", return_value=None
        ) as mock_consume:
            self.consumer.start()
            self.assertEqual(len(self.consumer.threads), 1)
            self.consumer.stop()
            self.assertEqual(len(self.consumer.consumers), 0)
            self.assertEqual(len(self.consumer.topics), 0)
            mock_consume.assert_called_once()
