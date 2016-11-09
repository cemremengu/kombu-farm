import abc
import logging

from kombu import Exchange, Queue, Connection
from kombu.mixins import ConsumerMixin

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

class Harvester(ConsumerMixin, metaclass=abc.ABCMeta):
    """
    Base harvester class that specific harvester instances must extend
    and implement the hook methods
    """
    def __init__(self, source, queue, exchange='', routing_key=''):
        self.connection = Connection(source)
        self.queue = queue
        self.exchange = exchange
        self.routing_key = routing_key

    def get_consumers(self, Consumer, channel):
        return [Consumer(
            queues=[Queue(self.queue, exchange=self.exchange, routing_key=self.routing_key)],
            accept=['json', 'pickle', 'msgpack', 'yaml'], callbacks=[self.task])]

    def task(self, body, message):
        """
        Main task pipeline.
        Gather -> process -> dump . Yay!
        """
        try:
            self.dump(self.process(self.gather(body)))
            message.ack()
        except Exception as ex:
            LOGGER.error(ex)

    @abc.abstractmethod
    def gather(self, body):
        """
        Gather the data from queue and
        transform it as you wish according to how you process.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def process(self, data):
        """
        Process the data. This is your harvesters reason to live.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def dump(self, result):
        """
        Dump the results to wherever you want.
        This can be another queue, database, redis, pickle or paper
        """
        raise NotImplementedError
