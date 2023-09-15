import unittest
from unittest.mock import  patch, Mock
from flask import Flask
from flask_and_kafka import FlaskKafkaProducer
from flask_and_kafka import formatters


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

        self.mock_producer.produce.assert_called_once_with(topic=self.topic, value=self.message, key=None)
        self.assertEqual(self.mock_producer.produce.call_count, 1)

    def test_send_message_byte_values(self):
        mock_json = Mock()
        message_bytes = b'Hello, Bytes!'
        key_bytes = b'key'
        with patch.object(self.producer, 'producer', self.mock_producer):
            with patch.object(formatters, 'json', mock_json):
                self.producer.send_message(self.topic, message_bytes, key=key_bytes)

        self.mock_producer.produce.assert_called_once_with(topic=self.topic, value=message_bytes, key=key_bytes)
        self.assertEqual(self.mock_producer.produce.call_count, 1)

        self.assertEqual("b'key'", mock_json.dumps.call_args[0][0]['key'])
        self.assertEqual("b'Hello, Bytes!'", mock_json.dumps.call_args[0][0]['value'])

    def test_send_message_flush(self):
        with patch.object(self.producer, 'producer', self.mock_producer):
            self.producer.send_message(self.topic, self.message, flush=True)
        self.mock_producer.produce.assert_called_once_with(topic=self.topic, value=self.message, key=None)
        self.mock_producer.flush.assert_called_once_with()

    def test_send_message_poll(self):
        with patch.object(self.producer, 'producer', self.mock_producer):
            self.producer.send_message(self.topic, self.message, poll=True)
        self.mock_producer.produce.assert_called_once_with(topic=self.topic, value=self.message, key=None)
        self.mock_producer.poll.assert_called_once_with(1)

    def test_close(self):
        with patch.object(self.producer, 'producer', self.mock_producer):
            self.producer.close()
        self.mock_producer.flush.assert_called_once_with()
        self.mock_producer.poll.assert_called_once_with(0)
