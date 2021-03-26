import sys
import os
import json
import statistics
import logging
from datetime import datetime, timedelta

_logger = logging.getLogger(__name__)


class ProcTraces:

    def __init__(self, paths, ignore_errors=True):
        """Analyze process traces.

        Atributes:
            paths (list(str)) - paths with traces files
            ignore_errors (bool) - raise exception when error occur
            nodes_procs (dict(str,dict)) - a dictionary with node names as keys and process data as values
                each process data is dictionary with pid (as string) as a key and dictionary of attributes as value
        """
        self.paths = paths
        self.ignore_errors = ignore_errors

        self.nodes_procs = {}
        self.read()

    def read(self):
        """Read process traces from log files."""
        self.nodes_procs = {}
        for path in self.paths:
            with open(path, 'rt') as proc_f:
                node_procs = json.load(proc_f)
                for attr in ['node', 'pids']:
                    if not attr in node_procs:
                        if not self.ignore_errors:
                            raise ValueError(f'missing {attr} attribute in trace file {path}')
                        else:
                            _logger.warning(f'missing {attr} attribute in trace file {path}')
                            continue

                self.nodes_procs.setdefault(node_procs['node'], {}).update(node_procs['pids'])
                _logger.debug(f'read {len(node_procs["pids"])} processed from node {node_procs["node"]}')

        _logger.debug(f'read totally {sum({len(node) for node in self.nodes_procs.values()})} processes from {len(self.nodes_procs)} nodes')

    def get_process(self, job_pid, node_name=None):
        """Find process data with given pid.

        If `node_name` is not specified, and there are more than single process with given `pid`
        on all nodes the first encountered process is returned.

        Args:
            job_pid (str,int) - process identifier
            node_name (str) - optional node name where to look process, if not defined all nodes
                are searched

        Returns:
            dict: process data
        """
        spid = str(job_pid)

        if node_name is not None:
            return self.nodes_procs.get(node_name, {}).get(spid)
        else:
            for node_name, node_procs in self.nodes_procs.items():
                if spid in node_procs:
                    return node_procs[spid]

            return None

    def _find_orted_procs_with_jobid(self, orted_jobid):
        """Look for processes with name `orted` and 'ess_base_jobid` argument set to `orted_jobid`.

        Args:
            orted_jobid (str) - orted identifier

        Returns:
            list(str) - list of found orted processes with given identifier
        """
        orted_procs = []
        for node_name, node_procs in self.nodes_procs.items():
            for pid, proc in node_procs.items():
                if proc.get('name', 'X') == 'orted':
                    cmdargs = proc.get('cmdline', [])
                    if len(cmdargs) > 3:
                        arg_idx = cmdargs.index('ess_base_jobid')
                        base_jobid = None
                        if len(cmdargs) > arg_idx + 1:
                             base_jobid = cmdargs[arg_idx + 1]

                        if base_jobid == orted_jobid:
                            orted_procs.append(pid)

        return orted_procs

    def _check_orted_jobid(self, srun_process):
        """Check if arguments of given process there is `ess_base_jobid` named argument, and if yes
        return following argument which should be orted identifier.

        Args:
            srun_process (dict) - process data to check

        Returns:
            str: orted identifier if found in arguments of given process
            None: if such identifier has not been found
        """
        cmdargs = srun_process.get('cmdline', [])
        if len(cmdargs) > 3 and cmdargs[0] == 'srun' and 'orted' in cmdargs:
            # find index of 'ess_base_jobid' argument
            arg_idx = cmdargs.index('ess_base_jobid')
            if len(cmdargs) > arg_idx + 1:
                return cmdargs[arg_idx + 1]

        return None

    def _find_slurmstepd_with_step_id(self, slurm_step_id):
        """Look for processes with name `slurmstepd` and 'slurmstepd:` argument set to `slurm_step_id`.

        Args:
            slurm_step_id (str) - slurm step identifier

        Returns:
            list(str) - list of found `slurmstepd` processes with given identifier
        """
        stepd_procs = []
#        print(f'looking for slurmstepd with step id {slurm_step_id}')
        for node_name, node_procs in self.nodes_procs.items():
            for pid, proc in node_procs.items():
                if proc.get('name', 'X') == 'slurmstepd':
                    cmdargs = proc.get('cmdline', [])
#                    print(f'found slurmstepd with args: {cmdargs}')
                    if len(cmdargs) >= 2:
                        arg_idx = cmdargs.index('slurmstepd:')
                        stepid = None
                        if len(cmdargs) > arg_idx + 1:
                            stepid = cmdargs[arg_idx + 1].strip('[]')
#                            print(f'found stepid in args: {stepid}')

                        if stepid == slurm_step_id:
#                            print(f'stepid matches {slurm_step_id}')
                            stepd_procs.append(pid)
#                        else:
#                            print(f'stepid NOT matches {slurm_step_id}')

        return stepd_procs

    def childs_on_other_nodes(self, process, slurm_step_id=None):
        """Find child process on other nodes not explicitely linked.
        For example when launching openmpi application where some of the instances will be launched
        on other nodes, mpirun should launch 'orted' deamon (via slurm) with identifier. When
        we find that such process has been created, we can look for 'orted' processes on other nodes
        with the same identifier.

        Args:
            process (dict) - process data
            slurm_step_id (str) - a slurm's step identifier (optional)

        Return:
            list(str): list of process identifiers that has been run on other nodes
        """
        if process.get('name') == 'srun':
            orted_jobid = self._check_orted_jobid(process)
            if orted_jobid:
                orted_procs = self._find_orted_procs_with_jobid(orted_jobid)
                if orted_procs:
                    return orted_procs
            elif slurm_step_id:
                stepsd_step_ids = self._find_slurmstepd_with_step_id(slurm_step_id)
                return stepsd_step_ids
            elif process.get('slurm_step_id'):
                stepsd_step_ids = self._find_slurmstepd_with_step_id(process.get('slurm_step_id'))
                return stepsd_step_ids

        return None

    def _iterate_childs(self, process, level=0, slurm_step_id=None):
        """Generator recursive function which looks for child processes.

        Args:
            process (dict) - process data to start iteratate
            level (int) - the level of nesting in tree

        Returns:
            dict, int: a pair with process data and level of nesting in tree
        """
        yield process, level

        level=level+1

        childs = process.get('childs', [])
        childs_node = process.get('node')

        other_childs = self.childs_on_other_nodes(process, slurm_step_id)
        if other_childs:
            childs.extend(other_childs)
            childs_node = None

        for child_pid in childs:
            child_process = self.get_process(child_pid, childs_node)
            if child_process:
                yield from self._iterate_childs(child_process, level, slurm_step_id)
            else:
                if not self.ignore_errors:
                    raise ValueError(f'child process {child_pid} not found')

                _logger.warning('child process {child_pid} not found')

    def process_iterator(self, pid, node_name=None):
        """Generator which iterates on process and it's childs.

        Args:
            pid (str,int) - process identifier from which start iteration
            node_name (str) - optional node name where to look for process

        Returns:
            dict, int: a pair with process data and level of nesting in tree
        """
        process = self.get_process(pid, node_name)

        yield from self._iterate_childs(process, slurm_step_id=process.get('slurm_step_id'))
