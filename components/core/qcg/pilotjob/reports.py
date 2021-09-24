import json
import logging
import io


_logger = logging.getLogger(__name__)


class JobReport:
    """Base class for report generating classes.

    The ``report_job_entry`` must be overloaded by the child classes.

    Attributes:
        report_file (str): path to the report file
        buffer (io.StringIO): buffer for caching reports
        buffered_entries (int): number of buffered entries
        buffer_size (int): maximum number of entries in cache before they will be flushed to the report file
    """

    def __init__(self, report_file, buffer_size=100):
        """Initialize report class.

        Args:
            report_file (str): path to the report file
            buffer_size (int): maximum number of entries to be buffered
        """
        self.report_file = report_file
        self.buffer = io.StringIO()
        self.buffered_entries = 0
        self.buffer_size = buffer_size

    def report_job(self, job, iteration):
        """Report job statistics.

        Args:
            job (Job): job to report
            iteration (int): job's iteration index
        """
        self.report_job_entry(job, iteration, self.buffer)
        self.buffered_entries += 1

        if self.buffered_entries > self.buffer_size:
            self.flush()

    def flush(self):
        """Output buffered entries to the file."""
        with open(self.report_file, 'a') as out_f:
            out_f.write(self.buffer.getvalue())

        self.buffer.close()
        self.buffer = io.StringIO()
        self.buffered_entries = 0

    def report_job_entry(self, job, iteration, ostream):
        """The method for generating report content for given job's iteration.

        Args:
            job (Job): job to report
            iteration (int): job's iteration index
            ostream (io.StringIO): buffer to output job's report
        """
        raise NotImplementedError()


class TextFileReport(JobReport):
    """Generate human readable job report file."""

    NAME = 'text'

    def __init__(self, report_file):
        """Initialize text file reporter.

        Args:
            report_file (str): path to the report file
        """
        super(TextFileReport, self).__init__(report_file)
        _logger.info('initializing TEXT job report')

    def report_job_entry(self, job, iteration, ostream):
        """Generate human readable entry for job's iteration.

        Args:
            job (Job): job to report
            iteration (int): job's iteration index
            ostream (io.StringIO): buffer to output job's report
        """
        jname = job.get_name(iteration)
        jstate = job.str_state(iteration)
        jmessages = job.messages(iteration)
        jhistory = job.history(iteration)
        jruntime = job.runtime(iteration)

        ostream.write("{} ({}) {}\n\t{}\n\t{}\n".format(
            jname, jstate, jmessages or '',
            "\n\t".join(["{}: {}".format(str(en[1]), en[0].name) for en in jhistory]),
            "\n\t".join(["{}: {}".format(k, v) for k, v in jruntime.items()])))


class JsonFileReport(JobReport):
    """Generate easy parsable JSON job report file."""

    NAME = 'json'

    def __init__(self, report_file):
        """Initialize text file reporter.

        Args:
            report_file (str): path to the report file
        """
        super(JsonFileReport, self).__init__(report_file)
        _logger.info('initializing JSON job report')

    def report_job_entry(self, job, iteration, ostream):
        """Generate human readable entry for job's iteration.

        Args:
            job (Job): job to report
            iteration (int): job's iteration index
            ostream (io.StringIO): buffer to output job's report
        """
        jname = job.get_name(iteration)
        jstate = job.str_state(iteration)
        jmessages = job.messages(iteration)
        jhistory = job.history(iteration)
        jruntime = job.runtime(iteration)

        data = {
            'name': jname,
            'state': jstate,
            'history': [{'state': e[0].name, 'date': e[1].isoformat()} for e in jhistory],
            'runtime': jruntime,
        }

        if iteration is None:
            data['execution'] = job.execution.to_dict()
            data['resources'] = job.resources.to_dict()

            if job.dependencies:
                data['dependencies'] = job.dependencies.to_dict()

        if jmessages:
            data['messages'] = jmessages

        ostream.write(json.dumps(data, separators=(',', ': ')) + '\n')


def none_reporter():
    """Dummy class for no reporting job's."""
    return None


_available_formats = {
    'none': none_reporter,
    TextFileReport.NAME: TextFileReport,
    JsonFileReport.NAME: JsonFileReport
}


def get_reporter(format_name, report_file):
    """Return reporter class based on the name.

    The currently available reporter classes are:
        'text' - human readable job reports
        'json' - easy parsable JSON job reports
        'none' - none reporting

    Args:
        format_name (str): report format name
        report_file (str): path to the output file

    Returns:
        JobReport: instance of report class

    Raises:
        ValueError: when reporter with given name is not known
    """
    if format_name not in _available_formats:
        raise ValueError('reporter {} not available'.format(format_name))

    return _available_formats[format_name](report_file)
