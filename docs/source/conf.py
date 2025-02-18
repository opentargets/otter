"""Configuration file for Sphinx."""

import sphinx_rtd_theme  # type: ignore[import] # noqa: F401

project = 'Otter'
copyright = '2024, Open Targets Team'  # noqa: A001
author = 'Open Targets Team'

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
# html_static_path = ['_static']

coverage_show_missing_items = True

# Uncomment the following line if you have defined custom style in ./_static/style.css
# html_css_files = ['style.css']


# -- Options for autodoc pydantic -------------------------------------------
autodoc_pydantic_model_show_json = True
autodoc_pydantic_settings_show_json = False
autodoc_member_order = 'bysource'
