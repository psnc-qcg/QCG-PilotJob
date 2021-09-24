from builtins import list

import pytest

from qcg.pilotjob.executor_api.qcgpj_executor import QCGPJExecutor
from qcg.pilotjob.executor_api.templates.basic_template import BasicTemplate


def test_qcgpj_executor_create(tmpdir):
    cores = 2

    with QCGPJExecutor(resources=str(cores)) as e:
        qcgpjm = e.qcgpj_manager
        assert qcgpjm is not None
        res = qcgpjm.resources()
        assert all(('total_nodes' in res, 'total_cores' in res, res['total_nodes'] == 1, res['total_cores'] == cores))


def test_qcgpj_template(tmpdir):
    template, defaults = BasicTemplate.template()
    assert template is not None
    assert defaults is not None


def test_qcgpj_executor_submit_basic_template(tmpdir):
    with QCGPJExecutor() as e:
        e.submit(BasicTemplate.template, name='tj', exec='date')

    with pytest.raises(KeyError, match=r".*exec.*"):
        with QCGPJExecutor() as e:
            e.submit(BasicTemplate.template, name='tj')

    with QCGPJExecutor() as e:
        e.submit(BasicTemplate.template, name='tj', exec='cat', args='["-v", "/etc/hostname"]')

    e = QCGPJExecutor()
    e.submit(BasicTemplate.template, name='tj', exec='date')
    e.shutdown()


def test_qcgpj_executor_submit_custom_template_with_defaults(tmpdir):
    l = lambda: ("""
            {
                'name': '${name}',
                'execution': {
                    'exec': '${exec}',
                    'args': ${args},
                    'stdout': '${stdout}',
                    'stderr': '${stderr}'
                }
            }
             """,
                 {
                     'args': [],
                     'stdout': 'stdout',
                     'stderr': 'stderr'
                 }
                 )

    with QCGPJExecutor() as e:
        e.submit(l, name='tj', exec='date')


def test_qcgpj_executor_submit_custom_template_without_defaults(tmpdir):
    l = lambda: ("""
            {
                'name': 'tj',
                'execution': {
                    'exec': 'date',
                    'stdout': 'stdout',
                    'stderr': 'stderr'
                }
            }
            """
                 )

    with QCGPJExecutor() as e:
        e.submit(l)


def test_qcgpj_executor_run_basic_template(tmpdir):
    with QCGPJExecutor() as e:
        f = e.submit(BasicTemplate.template, name='tj', exec='date')
        result = f.result()
        assert result == {'tj': 'SUCCEED'}


def test_qcgpj_executor_done(tmpdir):
    with QCGPJExecutor() as e:
        f = e.submit(BasicTemplate.template, name='tj', exec='date')
        result = f.result()
        assert result == {'tj': 'SUCCEED'}
        assert f.done()

    with QCGPJExecutor() as e:
        f = e.submit(BasicTemplate.template, name='tj', exec='sleep', args=['10'])
        assert not f.done()


def test_qcgpj_executor_running(tmpdir):
    with QCGPJExecutor() as e:
        f = e.submit(BasicTemplate.template, name='tj', exec='date')
        result = f.result()
        assert result == {'tj': 'SUCCEED'}
        assert not f.running()

    with QCGPJExecutor() as e:
        f = e.submit(BasicTemplate.template, name='tj', exec='sleep', args=['10'])
        assert f.running()


def test_qcgpj_executor_cancel(tmpdir):
    with QCGPJExecutor() as e:
        f = e.submit(BasicTemplate.template, name='tj', exec='date')
        result = f.result()
        assert result == {'tj': 'SUCCEED'}
        assert not f.cancelled()

    with QCGPJExecutor() as e:
        f = e.submit(BasicTemplate.template, name='tj', exec='sleep', args=['10'])
        f.cancel()
        assert f.cancelled()
        

def test_qcgpj_executor_args(tmpdir):
    with QCGPJExecutor() as e:
        a = {
            'exec': 'date',
            'name': 'tj'
        }
        
        f = e.submit(BasicTemplate.template, a)
        result = f.result()
        assert result == {'tj': 'SUCCEED'}
        
    with QCGPJExecutor() as e:
        a = {
            'exec': 'date'
        }
        
        b = {
            'name': 'tj'
        }

        f = e.submit(BasicTemplate.template, a, b)
        result = f.result()
        assert result == {'tj': 'SUCCEED'}
        
        
def test_qcgpj_executor_args_kwargs(tmpdir):
    with QCGPJExecutor() as e:
        a = {
            'exec': 'date',
        }

        f = e.submit(BasicTemplate.template, a, name='tj')
        result = f.result()
        assert result == {'tj': 'SUCCEED'}
