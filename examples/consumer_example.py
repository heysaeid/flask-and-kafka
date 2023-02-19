from flask import Flask
from flask_and_kafka import FlaskKafkaConsumer
from flask_and_kafka import close_kafka


app = Flask(__name__)
app.config['KAFKA_CONSUMER_CONFIGS'] = {"bootstrap.servers": 'localhost:9092'}
kafka_consumer = FlaskKafkaConsumer(app)

@kafka_consumer.handle_message(topic='test-topic', group_id='group1')
def handle_logistic_message(msg):
    print(msg)

if __name__ == '__main__':
    kafka_consumer.start()
    close_kafka(consumer=kafka_consumer)
    app.run()