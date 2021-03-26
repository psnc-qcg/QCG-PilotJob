import asyncio
import logging
import psutil
import signal
from datetime import datetime


_logger = logging.getLogger(__name__)


async def terminate_subprocess(process, pname, timeout_secs):
    try:
        _logger.info(f'terminating process {pname} ...')
#        process.terminate()
#        process.send_signal(signal.SIGINT)
        process.send_signal(signal.SIGINT)
    except Exception as exc:
        _logger.error(f'failed to cancel process {pname}: {str(exc)}')

    cancel_start_time = datetime.now()

    while True:
        # wait a moment
        _logger.info(f'sleeping waiting for process ...')
        await asyncio.sleep(1)

        # check if processes still exists
        if not process:
            _logger.debug(f'canceled process {pname} finished')
            break
        else:
            pid = process.pid

            try:
                _logger.info(f'checking pid {pid} ...')
                p = psutil.Process(pid)
                _logger.debug(f"process's {pname} pid {pid} still exists")
                # process not finished
            except psutil.NoSuchProcess:
                _logger.debug(f"process's {pname} pid {pid} doesn't exists")
                # process finished
                break
            except Exception as exc:
                _logger.warning(f'failed to check process {pname} status: {str(exc)}')

            if (datetime.now() - cancel_start_time).total_seconds() > timeout_secs:
                # send SIGKILL signal
                try:
                    _logger.info(f'killing processes {pname} pid {pid}')
                    process.kill()
                except Exception as exc:
                    _logger.warning(f'failed to kill process {pname} pid {pid}: {str(exc)}')
                return


def _update_process_status(process, stats, nodename):
    with process.oneshot():
        status = stats.setdefault(process.pid, {})

        if not 'created' in status:
            status.setdefault('created', datetime.fromtimestamp(process.create_time()))

        status.setdefault('pid', process.pid)
        status.setdefault('node', nodename)
        if not 'parent' in status:
            status.setdefault('parent', process.ppid())
        if not 'parent_name' in status:
            status.setdefault('parent_name', psutil.Process(int(process.ppid())).name())
        if not 'name' in status:
            status.setdefault('name', process.name())
        if not 'cmdline' in status:
            status.setdefault('cmdline', process.cmdline())
        if not 'cpu_affinity' in status:
            status.setdefault('cpu_affinity', process.cpu_affinity())
        status.setdefault('cpu_times', process.cpu_times())
        status.setdefault('memory_percent', process.memory_percent())
        status.setdefault('memory_info', process.memory_info())
        status['last'] = datetime.now()

#        childrens = process.children()
        childrens = process.children(recursive=True)
        for child in childrens:
#            _update_process_status(child, status.setdefault('childs', {}).setdefault(str(child.pid), {}), nodename)
            with child.oneshot():
                child_status = stats.setdefault(child.pid, {})
#                child_status = status.setdefault('childs', {}).setdefault(str(child.pid), {})

                if not 'created' in child_status:
                    child_status['created'] = datetime.fromtimestamp(child.create_time())

                child_status['pid'] = child.pid
                child_status['node'] = nodename
                if not 'parent' in child_status:
                    child_status['parent'] = child.ppid()
                if not 'parent_name' in child_status:
                    child_status['parent_name'] = psutil.Process(int(child.ppid())).name()
                if not 'name' in child_status:
                    child_status['name'] = child.name()
                if not 'cmdline' in child_status:
                    child_status['cmdline'] = child.cmdline()
                if not 'cpu_affinity' in child_status:
                    child_status['cpu_affinity'] = child.cpu_affinity()
                child_status['cpu_times'] = child.cpu_times()
                child_status['memory_percent'] = child.memory_percent()
                child_status['memory_info'] = child.memory_info()
                child_status['last'] = datetime.now()

                stats.setdefault(child_status['parent'], {}).setdefault('childs', set()).add(child.pid)


def update_processes_status(proc_selector, proc_statuses, nodename):
    for process in psutil.process_iter(['pid', 'name']):
        try:
            if proc_selector(process):
                _update_process_status(process, proc_statuses, nodename)
        except psutil.NoSuchProcess:
            # this situation might happen
            pass

