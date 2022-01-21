# Added by Cyware
#
# @author Shamail Tayyab
# @date Sat Nov  6 00:21:13 IST 2021

from ..logging import get_logger
from .middleware import Middleware
from .threading import Interrupt


class Shutdown(Interrupt):
    """Exception used to interrupt worker threads when their worker
    processes have been signaled for termination.
    """


class Kafka(Middleware):
    """Middleware that interrupts Kafka Processes
    Currently, this is only available on CPython.
    """

    def __init__(self):
        self.logger = get_logger(__name__, type(self))
        self.logger.info("Init KafkaMiddleware for Dramatiq")

    def before_worker_thread_shutdown(self, broker, worker):
        self.logger.debug("Sending shutdown notification to kafka workers...")
        broker.killself()
