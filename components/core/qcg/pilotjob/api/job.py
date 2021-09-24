import json
import uuid



# top level job description attributes
from qcg.pilotjob.api.errors import InvalidJobDescriptionError, JobNotDefinedError, QCGPJMAError, FileError

JOB_TOP_ATTRS = {
    "name": {'req': True, 'types': [str]},
    "exec": {'req': False, 'types': [str]},
    "script": {'req': False, 'types': [str]},
    "args": {'req': False, 'types': [list, str]},
    "stdin": {'req': False, 'types': [str]},
    "stdout": {'req': False, 'types': [str]},
    "stderr": {'req': False, 'types': [str]},
    "wd": {'req': False, 'types': [str]},
    "modules": {'req': False, 'types': [list, str]},
    "venv": {'req': False, 'types': [str]},
    "model": {'req': False, 'types': [str]},
    "model_opts": {'req': False, 'types': [dict]},
    "numNodes": {'req': False, 'types': [int, dict]},
    "numCores": {'req': False, 'types': [int, dict]},
    "wt": {'req': False, 'types': [str]},
    "iteration": {'req': False, 'types': [int, list, dict]},
    "after": {'req': False, 'types': [list, str]}
}

# resources (nodes, cores) attributes of job description
JOB_RES_ATTRS = {
    "min": {'req': False, 'types': [int]},
    "max": {'req': False, 'types': [int]},
    "exact": {'req': False, 'types': [int]},
    "scheduler": {'req': False, 'types': [str]}
}

# iterations attributes of job description
ITERATE_ATTRS = {
    "start": {'req': False, 'types': [int]},
    "stop": {'req': False, 'types': [int]},
    "values": {'req': False, 'types': [list]}
}

# maximum number of iterations
MAX_ITERATIONS = 64 * 1024


class Jobs:
    """Group of job descriptions to submit

    Attributes:
        _list (dict(str,dict)): map with added job descriptions
        _job_idx (int): counter which is used to return ordered lists
    """

    def __init__(self):
        """Initialize instance."""
        self._list = {}
        self._job_idx = 0

    @staticmethod
    def _generate_default_name():
        """Generate job's default name if non specified.

        The UUID4 function is used to generate unique job name.

        Returns:
            str: unique job name
        """
        return str(uuid.uuid4())

    def _validate_smpl_job(self, attrs):
        """Validate job description attributes.

        It's is not a full validation, only the attributes names and types are checked.

        Args:
            attrs (dict): job description attributes

        Raises:
            InvalidJobDescriptionError: in case of invalid job description
        """
        if 'name' not in attrs:
            attrs['name'] = Jobs._generate_default_name()

        if attrs['name'] in self._list:
            raise InvalidJobDescriptionError("Job {} already in list".format(attrs['name']))

        for attr in attrs:
            if attr not in JOB_TOP_ATTRS:
                raise InvalidJobDescriptionError("Unknown attribute {}".format(attr))

            type_valid = False
            for attr_type in JOB_TOP_ATTRS[attr]['types']:
                if isinstance(attrs[attr], attr_type):
                    type_valid = True
                    break

            if not type_valid:
                raise InvalidJobDescriptionError("Invalid attribute {} type {}".format(attr, type(attrs[attr])))

        if 'after' in attrs and isinstance(attrs['after'], str):
            # convert after string to single element list
            attrs['after'] = [attrs['after']]

        for req_attr in JOB_TOP_ATTRS:
            if JOB_TOP_ATTRS[req_attr]['req'] and req_attr not in attrs:
                raise InvalidJobDescriptionError("Required attribute '{}' not defined".format(req_attr))

        if 'exec' not in attrs and 'script' not in attrs:
            raise InvalidJobDescriptionError("No 'exec' nor 'script' defined")

        if 'exec' in attrs and 'script' in attrs:
            raise InvalidJobDescriptionError("Both 'exec' and 'script' defined")

        if 'numCores' in attrs and isinstance(attrs['numCores'], int):
            attrs['numCores'] = {'exact': attrs['numCores']}

        if 'numNodes' in attrs and isinstance(attrs['numNodes'], int):
            attrs['numNodes'] = {'exact': attrs['numNodes']}

        Jobs._validate_smpl_element_definition(['numNodes', 'numCores'], attrs, JOB_RES_ATTRS)

        if 'iteration' in attrs:
            if isinstance(attrs['iteration'], int):
                attrs['iteration'] = {'start': 0, 'stop': attrs['iteration']}
            elif isinstance(attrs['iteration'], list):
                attrs['iteration'] = {'values': attrs['iteration']}

            Jobs._validate_smpl_element_definition(['iteration'], attrs, ITERATE_ATTRS)

            if 'stop' not in attrs['iteration'] and 'values' not in attrs['iteration']:
                raise InvalidJobDescriptionError("Required attribute stop of element iteration not defined")

            if 'stop' in attrs['iteration'] and 'values' in attrs['iteration']:
                raise InvalidJobDescriptionError("Stop and values iteration attribute are excluding")

            if 'values' in attrs['iteration']:
                niters = len(attrs['iteration']['values'])
            else:
                iter_start = attrs['iteration']['start'] if 'start' in attrs['iteration'] else 0
                niters = attrs['iteration']['stop'] - iter_start

            if niters < 0 or niters > MAX_ITERATIONS:
                raise InvalidJobDescriptionError("Wrong number of iterations - outside range (0, {})".format(
                    MAX_ITERATIONS))

    @staticmethod
    def _validate_smpl_element_definition(element_names, element_attributes, element_definition):
        """Validate job description elements according to specification.

        Args:
            element_names (list(str)): names of attributes of job description to validate
            element_attributes (dict): job description
            element_definition (dict): elements specification with allowed types
        """
        for element_name in element_names:
            if element_name in element_attributes:
                for element_attr in element_attributes[element_name]:
                    if element_attr not in element_definition:
                        raise InvalidJobDescriptionError("Unknown {} attribute {}".format(element_name, element_attr))

                    type_valid = False
                    for elem_type in element_definition[element_attr]['types']:
                        if isinstance(element_attributes[element_name][element_attr], elem_type):
                            type_valid = True
                            break

                    if not type_valid:
                        raise InvalidJobDescriptionError("Invalid {} attribute {} type {}".format(
                            element_name, element_attr, type(element_attributes[element_name][element_attr])))

                for req_attr in element_definition:
                    if element_definition[req_attr]['req'] and req_attr not in element_attributes[element_name]:
                        raise InvalidJobDescriptionError("Required attribute {} of element {} not defined".format(
                            req_attr, element_name))

    def _validate_std_job(self, std_job):
        """Perform simple validation of a job in format acceptable (std_job) by the QCG-PJM

        The default name is assigned if none defined. The existence of all required elements is checked.

        Args:
            std_job (dict): job description

        Raises:
            InvalidJobDescriptionError - in case of invalid job description
        """
        if 'name' not in std_job:
            std_job['name'] = self._generate_default_name()

        if 'execution' not in std_job or ('exec' not in std_job['execution'] and 'script' not in std_job['execution']):
            raise InvalidJobDescriptionError('Missing "execution/exec" key')

        if std_job['name'] in self._list:
            raise InvalidJobDescriptionError("Job {} already in list".format(std_job['name']))

    @staticmethod
    def _convert_smpl_to_std(smpl_job):
        """Convert simple job description to a standard format.

        Args:
            smpl_job (dict): simple job description

        Returns:
            dict: standard job description
        """
        std_job = {}

        std_job['name'] = smpl_job['name']
        if 'exec' in smpl_job:
            std_job.setdefault('execution', {})['exec'] = smpl_job['exec']
        else:
            std_job.setdefault('execution', {})['script'] = smpl_job['script']

        for key in ['args', 'stdin', 'stdout', 'stderr', 'wd', 'modules', 'venv', 'model', 'model_opts']:
            if key in smpl_job:
                std_job['execution'][key] = smpl_job[key]

        resources = {}
        for r_key in ['numCores', 'numNodes', 'wt']:
            if r_key in smpl_job:
                resources[r_key] = smpl_job[r_key]

        if len(resources) > 0:
            std_job['resources'] = resources

        for key in ['iteration']:
            if key in smpl_job:
                std_job['iteration'] = smpl_job[key]

        if 'after' in smpl_job:
            std_job['dependencies'] = {'after': smpl_job['after']}

        return std_job

    def add(self, job_attrs=None, **kw_attrs):
        """Add a new, simple job description to the group.

        If both arguments are present, they are merged and processed as a single dictionary.
        The following job attributes are currenlty supported:

        - ``name`` (str, optional): the job name
        - ``exec`` (str, optional): path to the executable program
        - ``script`` (str, optional): bash script content
        - ``args`` (str or list(str), optional): executable program arguments
        - ``stdin`` (str, optional): path to file which content should be passed to the standard input stream
        - ``stdout`` (str, optional): path to the file where standard output stream should be saved
        - ``stderr`` (str, optional): path to the file where standard error stream should be saved
        - ``wd`` (str, optional): path to the working directory where job should be started
        - ``modules`` (str or list(str), optional): list of modules that should be loaded before job start
        - ``venv`` (str, optional): path to the virtual environment that should be initialized before job start
        - ``model`` (str, optional): model of execution
        - ``model_opts`` (dict, optional): model options
        - ``numCores`` (int or dict, optional): number of required cores specification
        - ``numNodes`` (int or dict, optional): number of required nodes specification
        - ``wt`` (str, optional): job's maximum wall time
        - ``iteration`` (int, dict or list, optional): iterations definition
        - ``after`` (str or list(str), optional): name of the job's that must finish successfully before current one start

        The attributes ``exec`` (with optional ``args``) are mutually exclusive with ``script``.

        The ``numCores`` and ``numNodes`` atrributes may contain dictionary with following keys:

        - ``min`` (int, optional): minimum number of resources
        - ``max`` (int, optional): maximum number of resources
        - ``exact`` (int, optional): exact number of resources
        - ``scheduler`` (str, optional): name of iteration resource scheduler

        The ``min``, ``max`` attributes are mutually exclusive with ``exact``. The description of iteration resource
        schedulers can be found in documentation.

        The ``iteration`` argument may contain either:

        - dictionary with following keys:

          - ``start`` (int, optional): iterations start index
          - ``stop`` (int, optional): iterations stop index

        - ``values`` list with following iteration names

        The total number of iterations will be:

        - ``stop - start`` (the last iteration index will be ``stop - 1``) for boundary definition
        - length of ``values`` list

        Args:
            job_attrs (dict): job description attributes in a simple format
            kw_attrs (dict): job description attributes as a named arguments in a simple format

        Raises:
            InvalidJobDescriptionError: in case of non-unique job name or invalid job description
        """
        data = kw_attrs

        if job_attrs is not None:
            if data is not None:
                data = {**job_attrs, **data}
            else:
                data = job_attrs

        self._validate_smpl_job(data)
        self._append_job(data['name'], Jobs._convert_smpl_to_std(data))

        return self

    def _append_job(self, job_name, job_attrs):
        """Append a new job to internal list.

        All jobs should be appended by this function, which also put each of the job the index,
        which will be used to return ordered list of jobs.

        Args:
            job_name (str): job name
            job_attrs (dict): job attributes in standard format
        """
        self._list[job_name] = {'idx': self._job_idx, 'data': job_attrs}
        self._job_idx += 1

    def add_std(self, job_attrs=None, **kw_attrs):
        """Add a new, standard job description (acceptable by the QCG PJM) to the group.

        If both arguments are present, they are merged and processed as a single dictionary.

        Args:
            job_attrs (dict): job description attributes in a standard format
            kw_attrs (dict): job description attributes as a named arguments in a standard format

        Raises:
            InvalidJobDescriptionError: in case of non-unique job name or invalid job description
        """
        data = kw_attrs

        if job_attrs is not None:
            if data is not None:
                data = {**job_attrs, **data}
            else:
                data = job_attrs

        self._validate_std_job(data)
        self._append_job(data['name'], data)

        return self

    def remove(self, name):
        """Remote a job from the group.

        Args:
            name (str): name of the job to remove

        Raises:
            JobNotDefinedError: in case of missing job in a group with given name
        """
        if name not in self._list:
            raise JobNotDefinedError(name)

        del self._list[name]

    def clear(self):
        """Remove all jobs from the group.

        Returns:
            int: number of removed elements
        """
        njobs = len(self._list)
        self._list.clear()
        return njobs

    def job_names(self):
        """Return a list with job names in group.

        Returns:
            list(str): job names in group
        """
        return list(self._list)

    def ordered_job_names(self):
        """Return a list with job names in group in order they were appended.

        Returns:
            list(str): ordered job names
        """
        return [j[0] for j in sorted(list(self._list.items()), key=lambda j: j[1]['idx'])]

    def jobs(self):
        """Return job descriptions in format acceptable by the QCG-PJM

        Returns:
            list(dict): a list of jobs in the format acceptable by the QCG PJM (standard format)
        """
        return [j['data'] for j in list(self._list.values())]

    def ordered_jobs(self):
        """Return job descriptions in format acceptable by the QCG-PJM in order they were appended.

        Returns:
            list(dict): a list of jobs in the format acceptable by the QCG PJM (standard format)
        """
        return [j['data'] for j in sorted(list(self._list.values()), key=lambda j: j['idx'])]

    def load_from_file(self, file_path):
        """Read job's descriptions from JSON file in format acceptable (StdJob) by the QCG-PJM

        Args:
            file_path (str): path to the file with jobs descriptions in a standard format

        Raises:
            InvalidJobDescriptionError: in case of invalid job description
        """
        try:
            with open(file_path, 'r') as file_h:
                for job in json.load(file_h):
                    self._validate_std_job(job)

                    name = job['name']
                    if name in self._list:
                        raise InvalidJobDescriptionError('Job "{}" already defined"'.format(name))

                    self._append_job(name, job)
        except QCGPJMAError as exc:
            raise
        except Exception as exc:
            raise FileError('File to open/write file "{}": {}'.format(file_path, exc.args[0]))

    def save_to_file(self, file_path):
        """Save job list to JSON file in a standard format.

        Args:
            file_path (str): path to the destination file

        Raises:
            FileError: in case of problems with opening / writing output file.
        """
        try:
            with open(file_path, 'w') as file_h:
                file_h.write(json.dumps(self.ordered_jobs(), indent=2))
        except Exception as exc:
            raise FileError('File to open/write file "{}": {}'.format(file_path, exc.args[0]))
