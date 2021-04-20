import logging
import os
import json
import socket
from datetime import datetime


_logger = logging.getLogger(__name__)


class RunTimeStats:
    """The run-time statistics of launched processes.
    With use of wrapper application, this class will register the start and finish moments of launched application
    (w/o delays and overheads provided by the asyncio and QCG-PilotJob workload).
    The mechanism of gathering runtime statistics is following:
        * the unix named pipe is used to communicate with wrappers (single pipe globally for all processes)
        * the path to the pipe is passed to the wrapper where the start & stop moment should be written
        * in the mean time, this class reads the pipe for any statistics and gathers them
    To stop gathering the metrics, the line with FINISH string should be placed in the pipe.

    Attributes:
        pipe_path (str) - path to the unix named pipe used to collect runtime statistics
        rt_stats (dict(str, dict())) - a dictionary with gathered statistics
    """
    def __init__(self, pipe_path):
        self.pipe_path = pipe_path
        self.rt_stats = dict()
        self.nodename = socket.gethostname()

#        _logger.info(f'the pipe path {self.pipe_path}')

    def gather(self):
        """Read the pipe for any metrics until FINISH string appear."""
        try:
            finish = False

            # cyclically gather info about processes
            while not finish:
                with open(self.pipe_path) as fifo:
                    while True:
                        line = fifo.readline()
                        if not line:
                            break

                        if line == 'FINISH':
                            finish = True
                            break

                        segments = line.split(',')
                        if len(segments) != 3:
                            _logger.warning(f'unknown format of rt stat [{line}]')
                        else:
                            pid, start_sec, stop_sec = segments
                            started = datetime.fromtimestamp(float(start_sec))
                            finished = datetime.fromtimestamp(float(stop_sec))
                            _logger.debug(f'child {pid} started: {started}, finished: {finished}')
                            self.rt_stats[pid] = {'s': started, 'f': finished}
        except Exception:
            _logger.exception(f'error to gather rt metrics')
            raise
        finally:
            _logger.info(f'finishing gathering run times statistics')
            try:
                rt_fname=f'rtimes_{self.nodename}_{str(datetime.now())}_{str(os.getpid())}.log'

                def set_encoder(obj):
                    if isinstance(obj, set):
                        return list(obj)
                    else:
                        return str(obj)

                with open(rt_fname, 'wt') as out_f:
                    out_f.write(json.dumps({ 'node': self.nodename,
                                             'rt': self.rt_stats },
                                           indent=2, default=set_encoder))
            except Exception as exc:
                _logger.error(f'failed to save run times statistics: {str(exc)}')
                _logger.exception(exc)

