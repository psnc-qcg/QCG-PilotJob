from enum import Enum


class Config(Enum):
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
        'cmd_opt': '--wd',
        'default': '.'
    }

    AUX_DIR = {
        'name': 'aux.dir',
        'cmd_opt': None,
        'default': '.qcgpjm'
    }

    EXECUTION_NODES = {
        'name': 'nodes',
        'cmd_opt': '--nodes',
        'default': None
    }

    ENVIRONMENT_SCHEMA = {
        'name': 'envs',
        'cmd_opt': '--envschema',
        'default': 'auto'
    }

    RESOURCES = {
        'name': 'resources',
        'cmd_opt': '--resources',
        'default': 'auto'
    }

    FILE_PATH = {
        'name': 'file',
        'cmd_opt': '--file-path',
        'default': 'qcg_pm_reqs.json'
    }

    ZMQ_IP_ADDRESS = {
        'name': 'zmq.ip',
        'cmd_opt': None,
        'default': '*'
    }

    ZMQ_PORT = {
        'name': 'zmq.port',
        'cmd_opt': '--net-port',
        'default': None
    }

    ZMQ_PORT_MIN_RANGE = {
        'name': 'zmq.port.min',
        'cmd_opt': '--net-port-min',
        'default': 2222,
    }

    ZMQ_PORT_MAX_RANGE = {
        'name': 'zmq.port.max',
        'cmd_opt': '--net-port-max',
        'default': 9999,
    }

    ZMQ_IFACE_ADDRESS = {
        'name': 'zmq.address',
        'cmd_opt': None,
        'get': lambda conf:
               'tcp://{}:{}'.format(str(Config.ZMQ_IP_ADDRESS.get(conf)), str(Config.ZMQ_PORT.get(conf)))
               if Config.ZMQ_PORT.get(conf) else
               'tcp://{}'.format(str(Config.ZMQ_IP_ADDRESS.get(conf)))
    }

    REPORT_FORMAT = {
        'name': 'report.format',
        'cmd_opt': '--report-format',
        'default': 'json'
    }

    REPORT_FILE = {
        'name': 'report.file',
        'cmd_opt': '--report-file',
        'default': 'jobs.report'
    }

    LOG_LEVEL = {
        'name': 'log.level',
        'cmd_opt': '--log',
        'default': 'info'
    }

    SYSTEM_CORE = {
        'name': 'system.core',
        'cmd_opt': '--system-core',
        'default': False
    }

    ADDRESS_FILE = {
        'name': 'address.file',
        'cmd_opt': None,
        'default': 'address'
    }

    FINAL_STATUS_FILE = {
        'name': 'final.status.file',
        'cmd_opt': None,
        'default': 'final_status.json'
    }

    DISABLE_NL = {
        'name': 'nl.disable',
        'cmd_opt': '--disable-nl',
        'default': False
    }

    PROGRESS = {
        'name': 'progress',
        'cmd_opt': '--show-progress',
        'default': False
    }

    GOVERNOR = {
        'name': 'governor',
        'cmd_opt': '--governor',
        'default': False
    }

    RESUME = {
        'name': 'resume.path',
        'cmd_opt': '--resume',
        'default': None
    }

    PARENT_MANAGER = {
        'name': 'manager.parent',
        'cmd_opt': '--parent',
        'default': None
    }

    MANAGER_ID = {
        'name': 'manager.id',
        'cmd_opt': '--id',
        'default': None
    }

    MANAGER_TAGS = {
        'name': 'manager.tags',
        'cmd_opt': '--tags',
        'default': None
    }

    SLURM_PARTITION_NODES = {
        'name': 'slurm.nodes.partition',
        'cmd_opt': '--slurm-partition-nodes',
        'default': None
    }

    SLURM_LIMIT_NODES_RANGE_BEGIN = {
        'name': 'slurm.nodes.limit.begin',
        'cmd_opt': '--slurm-limit-nodes-range-begin',
        'default': None
    }

    SLURM_LIMIT_NODES_RANGE_END = {
        'name': 'slurm.nodes.limit.end',
        'cmd_opt': '--slurm-limit-nodes-range-end',
        'default': None
    }

    OPENMPI_MODEL_MODULE = {
        'name': 'model.openmpi.module',
        'cmd_opt': '--openmpi-module',
        'default': 'openmpi'
    }

    ENABLE_PROC_STATS = {
        'name': 'enable.proc.stats',
        'cmd_opt': '--enable-proc-stats',
        'default': False
    }

    ENABLE_RT_STATS = {
        'name': 'enable.rt.stats',
        'cmd_opt': '--enable-rt-stats',
        'default': False
    }

    WRAPPER_RT_STATS = {
        'name': 'wrapper.rt.stats',
        'cmd_opt': '--wrapper-rt-stats',
        'default': 'qcg_pj_launch_wrapper'
    }

    def get(self, config):
        """Return configuration entry value from dictionary

        Args:
            config (dict(str,str)) - configuration values
        """
        if 'get' in self.value:
            return self.value['get'](config)

        return config.get(self, self.value['default'])

