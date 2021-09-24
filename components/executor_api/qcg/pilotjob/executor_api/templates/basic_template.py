from typing import Tuple, Any, Dict

from qcg.pilotjob.executor_api.templates.qcgpj_template import QCGPJTemplate


class BasicTemplate(QCGPJTemplate):
    @staticmethod
    def template() -> Tuple[str, Dict[str, Any]]:
        template = """
        {
            'name': '${name}',
            'execution': {
                'exec': '${exec}',
                'args': ${args},
                'stdout': '${stdout}',
                'stderr': '${stderr}'
            }
        }
         """

        defaults = {
            'args': [],
            'stdout': 'stdout',
            'stderr': 'stderr'
        }

        return template, defaults
