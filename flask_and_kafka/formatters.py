import logging
import json


class ConsumerFormatter(logging.Formatter):

    def format(self, record):
        super().format(record)
        return json.dumps({
            "time": record.asctime, 
            "threadName": record.threadName,
            'topic': record.consumer_message.topic(),
            'key': str(record.consumer_message.key()),
            'value': str(record.consumer_message.value()),
            'partition': record.consumer_message.partition(),
            'offset': record.consumer_message.offset(),
            'error': record.consumer_message.error()
        })


class ProducerFormatter(logging.Formatter):

    def format(self, record):
        super().format(record)
        for entry_name, entry_value in record.producer_log.items():
            if isinstance(entry_value, bytes):
                record.producer_log[entry_name] = repr(entry_value)
        return json.dumps({
            "time": record.asctime,
            "threadName": record.threadName, 
            **record.producer_log
        })
