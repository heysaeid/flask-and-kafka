import logging
import json


class ConsumerFormatter(logging.Formatter):
    def format(self, record):
        super().format(record)

        return json.dumps({
            "time": record.asctime, 
            "threadName": record.threadName,
            'topic': record.consumer_message.topic(),
            'key': convert_bytes_to_repr(record.consumer_message.key()),
            'value': convert_bytes_to_repr(record.consumer_message.value()),
            'partition': record.consumer_message.partition(),
            'offset': record.consumer_message.offset(),
            'error': record.consumer_message.error()
        })



class ProducerFormatter(logging.Formatter):

    def format(self, record):
        super().format(record)
        producer_log = dict(record.producer_log)  # duplicate the dict to not modify the original.
        producer_log['key'] = convert_bytes_to_repr(producer_log['key'])
        producer_log['value'] = convert_bytes_to_repr(producer_log['value'])
        return json.dumps({
            "time": record.asctime,
            "threadName": record.threadName,
            **producer_log
        })


def convert_bytes_to_repr(value) -> str:
    if isinstance(value, bytes):
        return repr(value)
    return value
