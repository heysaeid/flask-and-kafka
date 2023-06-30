# Flask And Kafka
Easily write your kafka producers and consumers in flask.

This plugin was developed using confluent-kafka to help you use your producers and consumers alongside your Flask project as easily as possible. Also, kafka-and-consumer logs all messages in producer and consumer


## Installation
Install it with the following command:
```
pip install flask-and-kafka
```

## Usage
Using consumer:
```
from flask import Flask
from flask_and_kafka import FlaskKafkaConsumer
from flask_and_kafka import close_kafka


app = Flask(__name__)
app.config['KAFKA_CONSUMER_CONFIGS'] = {"bootstrap.servers": 'localhost:9092'}
app.config['KAFKA_CONSUMER_LOG_PATH'] = "logs/kafka_consumer.log"

kafka_consumer = FlaskKafkaConsumer(app)

@kafka_consumer.handle_message(topic='test-topic', group_id='group1')
def handle_logistic_message(msg):
    print(msg.value())

if __name__ == '__main__':
    kafka_consumer.start()
    close_kafka(consumer=kafka_consumer)
    app.run()
```
You can also write your own consumers in separate modules and use them using the register_consumers method

consumers/test_consumer.py
‍‍‍‍
```
@kafka_consumer.handle_message(topic='test-topic', group_id='group1')
def handle_logistic_message(msg):
    print(msg.value())
```
app.py
```
from flask import Flask
from flask_and_kafka import FlaskKafkaConsumer
from flask_and_kafka import close_kafka


app = Flask(__name__)
app.config['KAFKA_CONSUMER_CONFIGS'] = {"bootstrap.servers": 'localhost:9092'}
app.config['KAFKA_CONSUMER_LOG_PATH'] = "logs/kafka_consumer.log"

kafka_consumer = FlaskKafkaConsumer(app)
kafka_consumer.register_consumers(['consumers.test_consumer'])

if __name__ == '__main__':
    kafka_consumer.start()
    close_kafka(consumer=kafka_consumer)
    app.run()
```
<hr style="border:2px solid gray">
Using producer:

```
from flask import Flask
from flask_and_kafka import FlaskKafkaProducer
from flask_and_kafka import close_kafka


app = Flask(__name__)
app.config['KAFKA_PRODUCER_CONFIGS'] = {"bootstrap.servers": 'localhost:9092'}
app.config['KAFKA_PRODUCER_LOG_PATH'] = "logs/kafka_producer.log"
kafka_producer = FlaskKafkaProducer(app)

kafka_producer.send_message(topic='test-topic', value="Hello, World!")
kafka_producer.send_message(topic='test-topic', value="Hello, World!")
kafka_producer.send_message(topic='test-topic', value="Hello, World!")
kafka_producer.send_message(topic='test-topic', value="Hello, World!")

if __name__ == '__main__':
    close_kafka(producer=kafka_producer)
    app.run()
```

**‌Note that you must use close_kafka to close consumer and producer.**
<hr style="border:2px solid gray">

#### FlaskKafkaConsumer - handle_message decorator:

A decorator that registers a message handler function for the given topic and group ID.

Args:
+ topic (str): The Kafka topic to subscribe to.
+ group_id (str): The Kafka consumer group ID to use.
+ num_consumers (int, optional): The number of Kafka consumer threads to spawn (default is 1).
+ app_context (bool, optional): Whether to run the message handler function inside a Flask application context (default is False).
+ **kwargs: Additional arguments to pass to the Kafka consumer constructor.

Returns: Callable: A decorator function that wraps the message handler function.


#### FlaskKafkaProducer - send_message method:

Send a message to the specified Kafka topic with the given key and value.

Args:
+ topic (str): The Kafka topic to send the message to.
+ value (any): The message value to send.
+ key (str, optional): The message key to use (default: None).
+ flush (bool, optional): Whether to flush the producer's message buffer immediately after sending the message (default: False).
+ poll (bool, optional): Whether to wait for any outstanding messages to be sent before returning (default: True).
+ poll_timeout (float, optional): The maximum amount of time to wait for outstanding messages to be sent, in seconds (default: 1).
+ **kwargs: Additional keyword arguments to pass to the underlying Kafka producer.

Returns: None

Raises: KafkaError: If there is an error producing the message.

Note:

+ If `flush` is True, any outstanding messages in the producer's buffer will be sent immediately after the current message is sent.
+ If `poll` is True, the producer will wait for any outstanding messages to be sent before returning, up to the specified `poll_timeout`.
+ The `poll` argument is only relevant if `flush` is False, since the producer always waits for outstanding messages to be sent before flushing. 
