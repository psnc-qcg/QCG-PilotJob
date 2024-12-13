# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
import glob
import shutil
from distutils.dir_util import copy_tree

sys.path.insert(0, os.path.abspath('..'))
sys.path.insert(0, os.path.abspath('../components/core'))
sys.path.insert(0, os.path.abspath('../components/cmds'))
sys.path.insert(0, os.path.abspath('../components/executor_api'))

# -- Project information -----------------------------------------------------

project = 'QCG-PilotJob'
copyright = '2021 - now, Piotr Kopta, Tomasz Piontek & Bartosz Bosak'
author = 'Piotr Kopta, Tomasz Piontek & Bartosz Bosak'
html_logo = 'images/qcg-pj-logo.png'
master_doc = 'index'

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx_rtd_theme',
    'sphinx.ext.autosectionlabel',
    'sphinx.ext.napoleon',
    'sphinx.ext.autodoc',
    'sphinxcontrib.apidoc'
]


apidoc_tmpdir = './.apidoc-tmpdir'

apidoc_module_dir = f'{apidoc_tmpdir}/qcg'
apidoc_separate_modules = True
apidoc_module_first = True
apidoc_extra_args = ['--implicit-namespaces']

excluded_paths = []
componenents_dir = '../components'

#create temporary dir for apidoc
shutil.rmtree(apidoc_tmpdir, ignore_errors=True)
os.mkdir(apidoc_tmpdir)

globbed = []
#copy all components to apidoc_tmpdir
for c in ['executor_api', 'core', 'cmds']:
    copy_tree(f'{componenents_dir}/{c}/qcg', f'{apidoc_tmpdir}/qcg')

excluded_paths.extend(glob.glob(f'{apidoc_tmpdir}/qcg/pilotjob/*.py'))
excluded_paths.append(f'{apidoc_tmpdir}/qcg/pilotjob/launcher')
excluded_paths.append(f'{apidoc_tmpdir}/qcg/pilotjob/tests')
excluded_paths.append(f'{apidoc_tmpdir}/qcg/pilotjob/utils')

apidoc_excluded_paths = excluded_paths
autodoc_member_order = 'bysource'
autoclass_content = 'both'

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
import sphinx_rtd_theme
html_theme = "sphinx_rtd_theme"

html_theme_options = {
    'style_nav_header_background': 'white',
    'collapse_navigation': 'true'
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
