"""Configuration file for Sphinx."""

import sphinx_rtd_theme  # noqa: F401

import otter

project = 'Otter'
copyright = '2024, Open Targets Team'  # noqa: A001
author = 'Open Targets Team'
version = otter.__version__


# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinxcontrib.autodoc_pydantic',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.todo',
    'sphinx.ext.coverage',
    'sphinx.ext.viewcode',
    'sphinx_issues',
]

templates_path = ['_templates']
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
html_css_files = ['style.css']

# -- Options for coverage ---------------------------------------------------
coverage_show_missing_items = True

# -- Options for autodoc pydantic -------------------------------------------
autodoc_pydantic_model_show_json = True
autodoc_pydantic_settings_show_json = False
autodoc_member_order = 'bysource'

# -- Options for issues -----------------------------------------------------
issues_user_uri = 'https://github.com/{user}'
issues_github_path = 'opentargets/otter'
extlinks_detect_hardcoded_links = True
