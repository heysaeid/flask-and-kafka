import unittest
from unittest.mock import  patch, Mock
from flask import Flask
from flask_and_kafka import FlaskKafkaProducer


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
