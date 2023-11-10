from flask import Flask
from flask_and_kafka import FlaskKafkaProducer
from flask_and_kafka import close_kafka


app = Flask(__name__)

app.config['KAFKA_PRODUCER_CONFIGS'] = {"bootstrap.servers": 'localhost:29092'}
kafka_producer = FlaskKafkaProducer(app)

kafka_producer.send_message(topic='test-topic', value={"message_num": 1})
kafka_producer.send_message(topic='test-topic', value={"message_num": 2})
kafka_producer.send_message(topic='test-topic', value={"message_num": 3})
kafka_producer.send_message(topic='test-topic', value={"message_num": 4})


if __name__ == '__main__':
    close_kafka(producer=kafka_producer)
    app.run()