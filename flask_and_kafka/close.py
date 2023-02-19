from typing import Callable
import signal


def close_kafka(producer = None, consumer = None, func: Callable = None) -> None:

    def handle_sig(*args):
        if producer is not None:
            producer.close()
        if consumer is not None:
            consumer.stop()
        if func is not None:
            func()
        exit(0)

    signal.signal(signal.SIGTERM, handle_sig)
    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGQUIT, handle_sig)
    signal.signal(signal.SIGHUP, handle_sig)
