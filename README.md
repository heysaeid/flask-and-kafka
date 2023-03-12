# Flask And Kafka
Easily write your kafka producers and consumers in flask.

This plugin was developed using confluent-kafka to help you use your producers and consumers alongside your Flask project as easily as possible.

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

handle_message decorator:

+ topic:* your-topic-name
+ group_id:* your-consumer-group-id
+ num_consumers: 1 # The number of consumer instances
+ app_context: False # If you need to push the app context, set it to True
+ **kwargs

send_message:

+ topic:* your-topic-name
+ value:* value
+ key: None
+ flush: False
+ poll: True
+ poll_timeout: 1
+ **kwargs