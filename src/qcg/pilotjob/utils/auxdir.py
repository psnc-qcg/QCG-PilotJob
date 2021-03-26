import os
import re


AUX_DIR_PTRN = re.compile(r'\.qcgpjm-service-.*')
PROC_TRACE_PTRN = re.compile(r'ptrace_.*\.log')


def is_aux_dir(path):
    """Check if given path can be an auxiliary directory. The name pattern and type (directory) is checked.

    Args:
        path (str): path to check

    Returns:
        str: an absolute path if given path exists, is directory and name matches the auxiliary pattern, else None
    """
    apath = os.path.abspath(path)
    return apath if os.path.isdir(apath) and AUX_DIR_PTRN.match(os.path.basename(apath)) else None


def find_aux_dirs(path):
    """Find in given path directories which names matches auxiliary directory name pattern.

    Args:
        path (str): path where auxiliary directories will be searched

    Returns:
        list(str): list of paths with directories matches auxiliary name pattern
    """
    apath = os.path.abspath(path)
    return [os.path.join(apath, entry) for entry in os.listdir(apath)
            if AUX_DIR_PTRN.match(entry) and os.path.isdir(os.path.join(apath, entry))]


def find_single_aux_dir(path):
    """Find exactly one auxiliary directory in given path, in other case raise exception.

    Args:
        path (str): path where to find auxiliary directory

    Returns:
        str: auxiliary directory path in given path
    """
    paths = find_aux_dirs(path)
    if len(paths) == 0:
        raise Exception('No auxiliary directory exist in path {}'.format(path))

    if len(paths) > 1:
        raise Exception('Too many auxiliary directories in given path ({})'.format(','.join(paths)))

    return paths[0]


def find_latest_aux_dir(path):
    """Find exactly one, the last modified, auxiliary directory in given path, in other case raise exception.

    Args:
        path (str): path where to find auxiliary directory

    Returns:
        str: auxiliary directory path in given path
    """
    paths = find_aux_dirs(path)
    if len(paths) == 0:
        raise Exception('No auxiliary directory exist in path {}'.format(path))

    return max(paths, key=os.path.getmtime)


def find_report_files(path):
    """Find all files named `jobs.report` in qcg-pilotjob auxiliary directories in given path.
    First we look for auxilary directories, and then in those dirs we look for `jobs.report` files.

    Args:
        path (str): path to directory where to look for report files

    Returns:
        list(str): list of all `jobs.report` files in given path.
    """
    report_files = []

    for aux_path in find_aux_dirs(path):
        report_file = os.path.join(aux_path, 'jobs.report')
        if os.path.isfile(report_file):
            report_files.append(report_file)

    return report_files


def find_log_files(path):
    """Find all files named `service.log` in qcg-pilotjob auxiliary directories in given path.
    First we look for auxilary directories, and then in those dirs we look for `service.log` files.

    Args:
        path (str): path to directory where to look for log files

    Returns:
        list(str): list of all `service.log` files in given path.
    """
    log_files = []

    for aux_path in find_aux_dirs(path):
        log_file = os.path.join(aux_path, 'service.log')
        if os.path.isfile(log_file):
            log_files.append(log_file)

    return log_files


def find_proc_traces_files(path):
    """Find in given path files which names matches processes trace log name pattern.

    Args:
        path (str): path where processes trace logs will be searched

    Returns:
        list(str): list of paths with files matches processes trace logs name pattern
    """
    apath = os.path.abspath(path)
    return [os.path.join(apath, entry) for entry in os.listdir(apath)
            if PROC_TRACE_PTRN.match(entry) and os.path.isfile(os.path.join(apath, entry))]

