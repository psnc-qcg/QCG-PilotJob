import logging

from qcg.pilotjob.common.config import Var
from qcg.pilotjob.executor.localres import parse_local_resources
from qcg.pilotjob.executor.slurmres import parse_slurm_resources, in_slurm_allocation


def _detect_resources(config):
    """Return resource parse function according to actual environment.

    This function checks if user specified resource configuration in QCG-PilotJob parameters - in such case, the
    available resources will be taken from user configuration. If no there is a check if QCG-PilotJob has been launched
    in Slurm allocation, and in such case the Slurm allocation will be available resources. Finally, the local available
    cores are returned as available resources.

    Args:
        config (dict): QCG-PilotJob configuration

    Returns:
        function: function for parsing available resources
    """
    logging.info('determining source of information about available resources ...')

    if config.get(Var.EXECUTION_NODES):
        logging.info('selected local resources information')
        return parse_local_resources(config)

    if in_slurm_allocation():
        logging.info('selected SLURM resources information')
        return parse_slurm_resources(config)

    logging.info('selected local resources information')
    return parse_local_resources(config)


__RESOURCES_HANDLING__ = {
    'auto': _detect_resources,
    'local': parse_local_resources,
    'slurm': parse_slurm_resources,
}


def get_resources(config):
    """Return available resources according to environment.

    Function returns available resources according to specified information source. The user may specify in
    configuration (``resources`` configuration parameter) the information source. This parameter can take values:
        * auto (default one) - automatically detect the source of information
        * local - detect information about number of nodes and cores from configuration parameters, and if they are not
            defined, detect local system number of cores (no remote nodes will be available)
        * slurm - detect Slurm allocation resources

    Automatic detection of source of information checks (in following order):
        * QCG-PilotJob parameters for resource definition
        * Slurm allocation environment
        * information about local system cores

    Args:
        config (dict): QCG-PilotJob configuration

    Returns:
        Resources: available resources information

    Raises:
        ValueError: for unknown value of ``resources`` configuration parameter value
    """
    res_source = config.get(Var.RESOURCES)

    if res_source not in __RESOURCES_HANDLING__:
        raise ValueError('Invalid resources configuration setting: {}'.format(str(res_source)))

    logging.info('source of information about available resources: %s', str(res_source))
    return __RESOURCES_HANDLING__[res_source](config)
