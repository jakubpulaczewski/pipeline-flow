# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "pipeline-orchestrator"
copyright = "2025, Jakub Pulaczewski"
author = "Jakub Pulaczewski"
release = "0.0.0"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",  # Automatically document Python modules
    "sphinx.ext.napoleon",  # Support for Google/NumPy docstrings
    "sphinx.ext.doctest",  # Enable execution of Python examples
    "sphinx.ext.viewcode",  # Adds links to highlighted source code
    "myst_parser",  # Markdown support
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output


import sphinx_rtd_theme

html_theme = "sphinx_rtd_theme"

html_static_path = ["_static"]
html_sidebars = {}
