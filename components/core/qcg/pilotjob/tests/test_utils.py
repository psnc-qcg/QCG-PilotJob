import pytest
import os
import shutil
import time


from qcg.pilotjob.utils.auxdir import find_aux_dirs, find_single_aux_dir, find_latest_aux_dir


def test_aux_dir_find(tmpdir):
    # in fresh directory there should be no aux dirs
    assert len(find_aux_dirs(tmpdir)) == 0

    # directory that not start with .qcgpjm
    dname = 'somepath.qcgpjm'
    os.makedirs(tmpdir.join(dname))
    assert len(find_aux_dirs(tmpdir)) == 0
    shutil.rmtree(tmpdir.join(dname))

    # valid directory without postfix
    dname = '.qcgpjm-service-'
    os.makedirs(tmpdir.join(dname))
    assert len(find_aux_dirs(tmpdir)) == 1 and find_aux_dirs(tmpdir)[0] == os.path.abspath(tmpdir.join(dname))
    shutil.rmtree(tmpdir.join(dname))

    # file, not directory
    dname = '.qcgpjm-service-'
    open(str(tmpdir.join(dname)), 'w+').close()
    assert os.path.isfile(str(tmpdir.join(dname)))
    assert len(find_aux_dirs(tmpdir)) == 0
    os.remove(tmpdir.join(dname))

    # not top level directory
    subdir = 'subdirectory'
    dname = '{}/.qcgpjm-service'.format(subdir)
    os.makedirs(tmpdir.join(dname))
    assert len(find_aux_dirs(tmpdir)) == 0
    shutil.rmtree(tmpdir.join(subdir))

    # many directories with postfixes
    dnames = [ '.qcgpjm-service-governor-0', '.qcgpjm-service-manager-0', '.qcgpjm-service-manager-1' ]
    for dname in dnames:
        os.makedirs(tmpdir.join(dname))
    aux_dirs = find_aux_dirs(tmpdir)
    assert len(aux_dirs) == len(dnames)
    assert all((os.path.abspath(str(tmpdir.join(dname))) in aux_dirs for dname in dnames))
    for dname in dnames:
        shutil.rmtree(str(tmpdir.join(dname)))

    # no auxiliary directories
    with pytest.raises(Exception):
        find_single_aux_dir(str(tmpdir))

    # too many directories
    dnames = [ '.qcgpjm-service-governor-0', '.qcgpjm-service-manager-0', '.qcgpjm-service-manager-1' ]
    for dname in dnames:
        os.makedirs(tmpdir.join(dname))
    with pytest.raises(Exception):
        find_single_aux_dir(str(tmpdir))
    for dname in dnames:
        shutil.rmtree(str(tmpdir.join(dname)))

    # just one auxiliary dir
    dname = '.qcgpjm-service-0'
    os.makedirs(tmpdir.join(dname))
    assert find_single_aux_dir(str(tmpdir)) == os.path.abspath(str(tmpdir.join(dname)))
    shutil.rmtree(tmpdir.join(dname))

    # last modified (last created) auxiliary dir
    dnames = ['.qcgpjm-service-manager-1', '.qcgpjm-service-manager-3', '.qcgpjm-service-manager-2']
    for dname in dnames:
        os.makedirs(tmpdir.join(dname))
        time.sleep(1)
    with pytest.raises(Exception):
        find_single_aux_dir(str(tmpdir))
    assert find_latest_aux_dir(tmpdir) == os.path.abspath(str(tmpdir.join(dnames[-1])))
    for dname in dnames:
        shutil.rmtree(str(tmpdir.join(dname)))

