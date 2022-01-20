import string
import random

from enum import Enum


class Var(Enum):
    """Configuration description for QCG-PilotJob

    Each entry contains:
      name (str): name of the configuration entry
      default (str): default value for the entry
      get (lambda, optional): custom function that based on passed configuration dict return proper value

    By default the 'get' method for this class return value in dictionary related to the selected entry. In case
    where entry contains 'get' attribute, this method will be used to return the final configuration value.
    """

    EXECUTOR_WD = {
        'name': 'wd',
        'default': '.'
    }

    AUX_DIR = {
        'name': 'aux.dir',
        'default': '.qcgpjm'
    }

    EXECUTION_NODES = {
        'name': 'nodes',
        'default': None
    }

    ENVIRONMENT_SCHEMA = {
        'name': 'envs',
        'default': 'auto'
    }

    RESOURCES = {
        'name': 'resources',
        'default': 'auto'
    }

    FILE_PATH = {
        'name': 'file',
        'default': 'qcg_pm_reqs.json'
    }

    ZMQ_IP_ADDRESS = {
        'name': 'zmq.ip',
        'default': '*'
    }

    ZMQ_PORT = {
        'name': 'zmq.port',
        'default': None
    }

    ZMQ_PORT_MIN_RANGE = {
        'name': 'zmq.port.min',
        'default': 2222,
    }

    ZMQ_PORT_MAX_RANGE = {
        'name': 'zmq.port.max',
        'default': 9999,
    }

    ZMQ_IFACE_ADDRESS = {
        'name': 'zmq.address',
        'get': lambda conf:
               'tcp://{}:{}'.format(str(conf.get(Var.ZMQ_IP_ADDRESS)), str(conf.get(Var.ZMQ_PORT)))
               if conf.get(Var.ZMQ_PORT) else
               'tcp://{}'.format(str(conf.get(Var.ZMQ_IP_ADDRESS)))
    }

    ZMQ_PUB_PORT = {
        'name': 'zmq.pub.port',
        'default': None
    }

    ZMQ_PUB_ADDRESS = {
        'name': 'zmq.pub.address',
        'get': lambda conf:
              'tcp://{}:{}'.format(str(conf.get(Var.ZMQ_IP_ADDRESS)), str(conf.get(Var.ZMQ_PUB_PORT)))
              if conf.get(Var.ZMQ_PUB_PORT) else
              'tcp://{}'.format(str(conf.get(Var.ZMQ_IP_ADDRESS)))
    }

    REPORT_FORMAT = {
        'name': 'report.format',
        'default': 'text'
    }

    REPORT_FILE = {
        'name': 'report.file',
        'default': 'jobs.report'
    }

    LOG_LEVEL = {
        'name': 'log.level',
        'default': 'info'
    }

    SYSTEM_CORE = {
        'name': 'system.core',
        'default': False
    }

    ADDRESS_FILE = {
        'name': 'address.file',
        'default': 'address'
    }

    FINAL_STATUS_FILE = {
        'name': 'final.status.file',
        'default': 'final_status'
    }

    DISABLE_NL = {
        'name': 'nl.disable',
        'default': False
    }

    PROGRESS = {
        'name': 'progress',
        'default': False
    }

    GOVERNOR = {
        'name': 'governor',
        'default': False
    }

    PARENT_MANAGER = {
        'name': 'manager.parent',
        'default': None
    }

    MANAGER_ID = {
        'name': 'manager.id',
        'default': None
    }

    MANAGER_TAGS = {
        'name': 'manager.tags',
        'default': None
    }

    SLURM_PARTITION_NODES = {
        'name': 'slurm.nodes.partition',
        'default': None
    }

    SLURM_LIMIT_NODES_RANGE_BEGIN = {
        'name': 'slurm.nodes.limit.begin',
        'default': None
    }

    SLURM_LIMIT_NODES_RANGE_END = {
        'name': 'slurm.nodes.limit.end',
        'default': None
    }

    EXECUTOR_NAME = {
        'name': 'executor.name',
        'default': f'executor-{"".join(random.sample(string.ascii_uppercase, k=5))}'
    }

    IQ_ADDRESS = {
        'name': 'inputqueue.address',
        'default': None,
    }

    IQ_KEY = {
        'name': 'inputqueue.key',
        'default': None,
    }


class Configuration:

    def __init__(self, values=None):
        """Store configuration.

        Args:
            values (dict): a dictionary with variable values
        """
        self.values = values or dict()

    def set(self, var, value):
        """Set value for the variable.

        Args:
            var (Var): a variable to set value
            value (object): variable's value
        """
        self.values[var] = value

    def get(self, var):
        """Return configuration entry value from dictionary

        Args:
            var (Var): a variable for which the value return
        """
        if 'get' in var.value:
            return var.value['get'](self)

        return self.values.get(var, var.value['default'])

