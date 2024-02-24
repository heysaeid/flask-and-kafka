import traceback
from flask import Flask
from flask_and_kafka import FlaskKafkaConsumer, FlaskKafkaProducer, ConsumerRetry
from flask_and_kafka import close_kafka
from flask_and_kafka import KafkaStatusEnum

app = Flask(__name__)

app.config['KAFKA_CONSUMER_CONFIGS'] = {"bootstrap.servers": 'localhost:29092'}
app.config['KAFKA_PRODUCER_CONFIGS'] = {"bootstrap.servers": 'localhost:29092'}

kafka_consumer = FlaskKafkaConsumer(app)
kafka_producer = FlaskKafkaProducer(app)

kafka_consumer_retry_helper = ConsumerRetry(kafka_consumer)
kafka_consumer.init_retry_process(
    retry_and_fail_topics_prefix = "local_", 
    producer = kafka_producer, 
    consumer_retry_helper = kafka_consumer_retry_helper
)

@kafka_consumer.handle_message(topic='test-topic', group_id='group1', retry_attempt_number=5)
def handle_logistic_message(msg):
    """
    Return value can be three option of KafkaStatusEnum:
        KafkaStatusEnum.retry.value -> send message to the retry topics
        KafkaStatusEnum.success.value -> for successfull ending fuction 
        KafkaStatusEnum.failed.value -> send message to the fail topic
    """
    try:
        print(msg.value())
    except Exception as e:
        return traceback.format_exc(), KafkaStatusEnum.failed.value
    return "", KafkaStatusEnum.success.value

if __name__ == '__main__':
    kafka_consumer.start()
    close_kafka(consumer=kafka_consumer)
    app.run()