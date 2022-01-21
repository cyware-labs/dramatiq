# This file is a part of Dramatiq.
#
# Added by Cyware

import json
import os
import random
import time
import warnings
from queue import Queue
from threading import Lock
from uuid import uuid4

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import RequestTimedOutError, TopicAlreadyExistsError

from ..broker import Broker, Consumer, MessageProxy
from ..common import current_millis, dq_name, q_name
from ..errors import QueueJoinTimeout
from ..logging import get_logger
from ..message import Message

MAINTENANCE_SCALE = 1000000
MAINTENANCE_COMMAND_BLACKLIST = {"ack", "nack"}

#: How many commands out of a million should trigger queue
#: maintenance.
DEFAULT_MAINTENANCE_CHANCE = 1000

#: The amount of time in milliseconds that dead-lettered messages are
#: kept in Kafka for.
DEFAULT_DEAD_MESSAGE_TTL = 86400000 * 7

#: The amount of time in milliseconds that has to pass without a
#: heartbeat for a worker to be considered offline.
DEFAULT_HEARTBEAT_TIMEOUT = 60000

NUMBER_PARTITIONS = max(
    int(os.getenv("PUBLISHING_DRAMATIQ_PROCESS_CNT", 1)),
    int(os.getenv("CORE_RULES_DRAMATIQ_PROCESSS_CNT", 1)),
    int(os.getenv("INTEGRATION_DRAMATIQ_PROCESS_CNT", 1)),
)

MAX_REQUEST_SIZE = int(os.getenv("MAX_REQUEST_SIZE", 10485760))

ENABLE_AUTO_COMMIT = (
    True if os.getenv("enable_auto_commit", "true") in ["true", "True"] else False
)
DRAMATIQ_TOPIC_CREATION = (
    True if os.getenv("DRAMATIQ_TOPIC_CREATION") in ["true", "True"] else False
)

MAX_RECORDS = int(os.getenv("MAX_RECORDS", 5))

REPLICATION_FACTOR = 1

PRODUCE_TIMEOUT = 30

DEAD_QUEUE = "dead_queue"


class KafkaBroker(Broker):
    """A broker than can be used with Kafka.

    Examples:

    you want to use a connection URLs:

      >>> KafkaBroker(urls=["192.168.99.100:9092"])



    Parameters:
      url(list): An optional connection URL.  If both a URL and
        connection parameters are provided, the URL is used.
      middleware(list[Middleware])
      maintenance_chance(int): How many commands out of a million
        should trigger queue maintenance.
      namespace(str): The str with which to prefix all keys.
      heartbeat_timeout(int): The amount of time (in ms) that has to
        pass without a heartbeat for a broker process to be considered
        offline.
      dead_message_ttl(int): The amount of time (in ms) that
        dead-lettered messages are kept in .
      requeue_deadline(int): Deprecated.  Does nothing.
      requeue_interval(int): Deprecated.  Does nothing.
      client(kafka.KafkaAdminClient): A KafkaAdmin client to use.

    ..
    """

    def __init__(
        self,
        *,
        urls,
        middleware=None,
        namespace="dramatiq",
        maintenance_chance=DEFAULT_MAINTENANCE_CHANCE,
        heartbeat_timeout=DEFAULT_HEARTBEAT_TIMEOUT,
        dead_message_ttl=DEFAULT_DEAD_MESSAGE_TTL,
        requeue_deadline=None,
        requeue_interval=None,
        client=None,
        **parameters,
    ):
        super().__init__(middleware=middleware)

        self.urls = urls

        if requeue_deadline or requeue_interval:
            message = "requeue_{deadline,interval} have been deprecated and no longer do anything"
            warnings.warn(message, DeprecationWarning, stacklevel=2)

        self.broker_id = str(uuid4())
        self.namespace = namespace
        self.maintenance_chance = maintenance_chance
        self.heartbeat_timeout = heartbeat_timeout
        self.dead_message_ttl = dead_message_ttl
        self.data_queues = {}

        self.queues = set()
        self.dead_queues = set()
        self.running = True

        self.dead_queue_name = DEAD_QUEUE
        self.kafka_client = KafkaAdminClient(
            bootstrap_servers=urls,
            request_timeout_ms=2000,
        )
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=urls,
            max_request_size=MAX_REQUEST_SIZE,
        )
        self.kafka_consumer = KafkaConsumer(
            bootstrap_servers=urls,
            group_id="TENANT",
            request_timeout_ms=12000,
            # session_timeout_ms=5000,
            # heartbeat_interval_ms=5000,
            enable_auto_commit=ENABLE_AUTO_COMMIT,
        )

    @property
    def consumer_class(self):
        return _KafkaConsumer

    def close(self):
        self.logger.debug("Closing kafka Producer and AdminClient...")
        self.running = False
        self.kafka_client.close()
        self.kafka_producer.close()
        self.logger.debug("Closed all Kafka channels for Producer and AdminClient")

    def killself(self):
        self.close()
        self.consumer_backed_up.killself()
        self.logger.warning("Done KillSelf!")

    def consume(
        self,
        queue_name,
        prefetch=1,
        timeout=5000,
        enable_auto_commit=True,
        poll_timeout=1,
        bulk_fetch=1,
    ):
        """Create a new consumer for a queue.

        Parameters:
          queue_name(str): The queue to consume.
          prefetch(int): The number of messages to prefetch.
          timeout(int): The idle timeout in milliseconds.
          enable_auto_commit(bool): If True , the consumer’s offset
          will be periodically committed in the background. Default: False.
          poll_timeout: timeout for waiting for events to select bulk.
          bulk_fetch: number of event to select in the bulk for processing.

        Returns:
          Consumer: A consumer that retrieves messages from Kafka.
        """
        # Not to be confused by Kafka Consumer, this is Dramatiq's consumer.
        # Essentially an instance of _KafkaConsumer
        self.logger.info("CREATE NEW CONSUMER")
        self.consumer_backed_up = self.consumer_class(
            self,
            self.queues,
            queue_name,
            prefetch,
            timeout,
            enable_auto_commit,
            poll_timeout,
            bulk_fetch,
        )
        return self.consumer_backed_up

    def get_message(self, queue_name):
        que = self.data_queues.get(queue_name)
        if que is not None and not que.empty():
            return que.get()

        res = self.kafka_consumer.poll(2 * 1000, max_records=MAX_RECORDS)

        for _, records in res.items():
            for record in records:
                record_obj = json.loads(record.value.decode())
                queue_name = record_obj.get("queue_name")
                queue = record_obj.get("queue_name")
                if queue not in self.data_queues:
                    self.data_queues[queue] = Queue()
                self.data_queues[queue].put(record.value)

        if self.running is False:
            return

        que = self.data_queues.get(queue_name)
        if que is None:
            return None
        if que.empty():
            return None
        return que.get()

    def declare_queue(self, queue_name):
        """Declare a queue.  Has no effect if a queue with the given
        name has already been declared.

        Parameters:
          queue_name(str): The name of the new queue.
        """
        if queue_name not in self.queues:
            try:
                self.emit_before("declare_queue", queue_name)
                new_topic = NewTopic(queue_name, NUMBER_PARTITIONS, REPLICATION_FACTOR)
                if DRAMATIQ_TOPIC_CREATION:
                    self.kafka_client.create_topics([new_topic])
                self.queues.add(queue_name)
                self.emit_after("declare_queue", queue_name)
            except TopicAlreadyExistsError:
                self.queues.add(queue_name)
            delayed_name = dq_name(queue_name)
            try:
                new_delayed_topic = NewTopic(
                    delayed_name, NUMBER_PARTITIONS, REPLICATION_FACTOR
                )
                if DRAMATIQ_TOPIC_CREATION:
                    self.kafka_client.create_topics([new_delayed_topic])
                self.delay_queues.add(delayed_name)
                self.emit_after("declare_delay_queue", delayed_name)
            except TopicAlreadyExistsError:
                self.delay_queues.add(delayed_name)

        self.declare_dead_queue()

    def declare_dead_queue(self):
        """Declare the dead queue.  Has no effect if a dead queue is already declred.
        """
        if self.dead_queue_name not in self.dead_queues:
            try:
                new_topic = NewTopic(self.dead_queue_name, 1, 1)
                self.kafka_client.create_topics([new_topic])
                self.dead_queues.add(self.dead_queue_name)
            except TopicAlreadyExistsError:
                self.dead_queues.add(self.dead_queue_name)
                self.logger.info("Dead queue already exists")

    def do_enqueue(self, queue_name, message):
        future = self.kafka_producer.send(queue_name, message)
        try:
            return future.get(timeout=PRODUCE_TIMEOUT)
        except RequestTimedOutError:
            self.logger.exception(
                "kafka Producer timed out, as per design kafka producer \
                will retry this message again unless retry is off.\
                message: %s, queue_name: %s",
                message.message_id,
                message.queue_name,
            )
            return None

    def enqueue(self, message, *, delay=None):
        """Enqueue a message.

        Parameters:
          message(Message): The message to enqueue.
          delay(int): The minimum amount of time, in milliseconds, to
            delay the message by.  Must be less than 7 days.

        Raises:
          ValueError: If ``delay`` is longer than 7 days.
        """
        queue_name = message.queue_name

        if delay is not None:
            queue_name = dq_name(queue_name)
            message_eta = current_millis() + delay
            message = message.copy(
                queue_name=queue_name,
                options={
                    "eta": message_eta,
                },
            )

        self.logger.debug(
            "Enqueueing message %r on queue %r.", message.message_id, queue_name
        )
        self.emit_before("enqueue", message, delay)
        self.do_enqueue(queue_name, message.encode())
        self.emit_after("enqueue", message, delay)
        return message

    def get_declared_queues(self):
        """Get all declared queues.

        Returns:
          set[str]: The names of all the queues declared so far on
          this Broker.
        """
        return self.queues.copy()

    def flush(self, queue_name):
        """Drop all the messages from a queue.

        Parameters:
          queue_name(str): The queue to flush.
        """
        for name in (queue_name, dq_name(queue_name)):
            self.do_purge(name)

    def flush_all(self):
        """Drop all messages from all declared queues."""
        for queue_name in self.queues:
            self.flush(queue_name)

    def join(self, queue_name, *, interval=100, timeout=None):
        """Wait for all the messages on the given queue to be
        processed.  This method is only meant to be used in tests to
        wait for all the messages in a queue to be processed.

        Raises:
          QueueJoinTimeout: When the timeout elapses.

        Parameters:
          queue_name(str): The queue to wait on.
          interval(Optional[int]): The interval, in milliseconds, at
            which to check the queues.
          timeout(Optional[int]): The max amount of time, in
            milliseconds, to wait on this queue.
        """
        deadline = timeout and time.monotonic() + timeout / 1000
        while True:
            if deadline and time.monotonic() >= deadline:
                raise QueueJoinTimeout(queue_name)

            size = 0
            for name in (queue_name, dq_name(queue_name)):
                size += self.do_qsize(name)

            if size == 0:
                return

            time.sleep(interval / 1000)

    def _should_do_maintenance(self, command):
        return int(
            command not in MAINTENANCE_COMMAND_BLACKLIST
            and random.randint(1, MAINTENANCE_SCALE) <= self.maintenance_chance
        )

    _max_unpack_size_val = None
    _max_unpack_size_mut = Lock()

    def _max_unpack_size(self):
        cls = type(self)
        if cls._max_unpack_size_val is None:
            with cls._max_unpack_size_mut:
                if cls._max_unpack_size_val is None:
                    cls._max_unpack_size_val = self.scripts["maxstack"]()
        return cls._max_unpack_size_val

    def _dispatch(self, command):
        # Micro-optimization: by hoisting these up here we avoid
        # allocating the list on every call.
        dispatch = self.scripts["dispatch"]
        keys = [self.namespace]

        def do_dispatch(queue_name, *args):
            timestamp = current_millis()
            args = [
                command,
                timestamp,
                queue_name,
                self.broker_id,
                self.heartbeat_timeout,
                self.dead_message_ttl,
                self._should_do_maintenance(command),
                self._max_unpack_size(),
                *args,
            ]
            return dispatch(args=args, keys=keys)

        return do_dispatch

    def __getattr__(self, name):
        if not name.startswith("do_"):
            raise AttributeError("attribute %s does not exist" % name)

        command = name[len("do_") :]
        return self._dispatch(command)


class _KafkaConsumer(Consumer):
    def __init__(
        self,
        broker,
        queues,
        this_queue_name,
        prefetch,
        timeout,
        enable_auto_commit,
        poll_timeout,
        bulk_fetch,
    ):
        self.logger = get_logger(__name__, type(self))
        self.logger.warning("Initialize New _KafkaConsumer" + str(id(self)))
        self.logger.warning(
            "Broker: "
            + str(id(broker))
            + ", Queue: "
            + str(this_queue_name)
            + ", Prefetch Count: "
            + str(prefetch)
            + ", Enable auto commit: "
            + str(enable_auto_commit)
            + ", Poll Timeout: "
            + str(poll_timeout)
            + ", Bulk Fetch: "
            + str(bulk_fetch)
        )
        self.broker = broker
        self.prefetch = prefetch
        self.running = True
        self.timeout = timeout
        self.enable_auto_commit = enable_auto_commit
        self.message_cache = []
        self.queued_message_ids = set()
        self.misses = 0
        self.poll_timeout = poll_timeout
        self.bulk_fetch = bulk_fetch
        self.parent = broker
        self.kafka_consumer = broker.kafka_consumer
        self.subscribe(this_queue_name)
        self.queue_name = this_queue_name

    @property
    def outstanding_message_count(self):
        pass

    def subscribe(self, queue_name):
        """
        kafkaconumer subscribe method is not incremental list
        passed to subscribe will replace current subscribed topics
        so we have to fetch current subscribed first(or store it on process
        level in local list) and than append our topic to it.
        """
        current_subscription = self.kafka_consumer.subscription()
        current_subscription = set([q_name(topic_name) for topic_name in current_subscription]) if current_subscription else set()
        current_subscription.add(q_name(queue_name))
        new_subscription = set()
        for queue_name in current_subscription:
            new_subscription.add(q_name(queue_name))
            new_subscription.add(dq_name(queue_name))
        self.logger.info("Subscribing to :" + str(new_subscription))
        self.kafka_consumer.subscribe(new_subscription)

    def killself(self):
        self.logger.info("Done: KillSelf _KafkaConsumer..")
        self.running = False

    def ack(self, message):
        self.logger.info("Done: Ack message %s", message.message_id)
        self.commit()

    def nack(self, message):
        self.logger.info("Pushing message %s to the dead queue", message.message_id)
        new_message = message.copy(queue_name=self.broker.dead_queue_name,
                                   options={"dead": True, "original_queue_name": message.queue_name})
        self.broker.enqueue(new_message)

    def requeue(self, messages):
        self.logger.info("Commiting pending offset to avoid the duplicate messages.")
        self.commit()
        for message in messages:
            self.logger.info("Pushing message %s to topic again", message.message_id)
            self.broker.enqueue(messages)

    def commit(self):
        self.kafka_consumer.commit()

    def __next__(self):
        message = self.parent.get_message(self.queue_name)
        if message is None:
            return None
        message = Message.decode(message)
        return MessageProxy(message)
