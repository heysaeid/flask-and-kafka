import json
import tempfile
import unittest
from unittest.mock import  patch, Mock
from flask import Flask
from flask_and_kafka import FlaskKafkaProducer
from flask_and_kafka.log import producer_logger


class TestFlaskKafkaProducer(unittest.TestCase):
    message = 'Hello, Kafka!'
    topic = 'test-topic'

    @patch('confluent_kafka.Producer')
    def setUp(self, mock_producer):
        self.app = Flask(__name__)
        self.app.config['KAFKA_PRODUCER_CONFIGS'] = {
            'bootstrap.servers': 'localhost:9092'
        }
        self.producer = FlaskKafkaProducer(self.app)
        self.mock_producer = Mock(spec=self.producer.producer)

    def test_send_message(self):
        with patch.object(self.producer, 'producer', self.mock_producer):
            self.producer.send_message(self.topic, self.message)

        self.mock_producer.produce.assert_called_once_with(topic=self.topic, value=self.message, key=None, headers=None)
        self.assertEqual(self.mock_producer.produce.call_count, 1)

    def test_send_message_headers(self):
        headers = {'header1': 'value1', 'header2': 'value2'}
        log_file = tempfile.NamedTemporaryFile()

        with patch.object(self.producer, 'producer', self.mock_producer):
            self.producer.producer_logger = producer_logger(name="test", file=log_file.name)
            self.producer.send_message(self.topic, self.message, headers=headers)

        self.mock_producer.produce.assert_called_once_with(topic=self.topic, value=self.message, key=None, headers=headers)
        self.assertEqual(self.mock_producer.produce.call_count, 1)

        log_records = [json.loads(line) for line in log_file.readlines()]
        self.assertEqual(log_records[0]['headers'], headers)

    def test_send_message_byte_values(self):
        message_bytes = b'Hello, Bytes!'
        key_bytes = b'key'
        log_file = tempfile.NamedTemporaryFile()

        self.producer.producer_logger = producer_logger(name="test_producer", file=log_file.name)
        with patch.object(self.producer, 'producer', self.mock_producer):
            self.producer.send_message(self.topic, message_bytes, key=key_bytes)

        self.mock_producer.produce.assert_called_once_with(topic=self.topic, value=message_bytes, key=key_bytes,
                                                           headers=None)
        self.assertEqual(self.mock_producer.produce.call_count, 1)

        log_records = [json.loads(line) for line in log_file.readlines()]
        self.assertEqual(log_records[0]['key'], "b'key'")
        self.assertEqual(log_records[0]['value'], "b'Hello, Bytes!'")

    def test_send_message_flush(self):
        with patch.object(self.producer, 'producer', self.mock_producer):
            self.producer.send_message(self.topic, self.message, flush=True)
        self.mock_producer.produce.assert_called_once_with(topic=self.topic, value=self.message, key=None, headers=None)
        self.mock_producer.flush.assert_called_once_with()

    def test_send_message_poll(self):
        with patch.object(self.producer, 'producer', self.mock_producer):
            self.producer.send_message(self.topic, self.message, poll=True)
        self.mock_producer.produce.assert_called_once_with(topic=self.topic, value=self.message, key=None, headers=None)
        self.mock_producer.poll.assert_called_once_with(1)

    def test_close(self):
        with patch.object(self.producer, 'producer', self.mock_producer):
            self.producer.close()
        self.mock_producer.flush.assert_called_once_with()
        self.mock_producer.poll.assert_called_once_with(0)
